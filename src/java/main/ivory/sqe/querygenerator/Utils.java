package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.PairOfFloatMap;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfInts;
import tl.lin.data.map.HMapIV;
import tl.lin.data.map.HMapKF;
import tl.lin.data.map.HMapKI;
import tl.lin.data.map.HMapSFW;
import tl.lin.data.map.HMapSIW;
import tl.lin.data.map.MapKF;
import tl.lin.data.pair.PairOfStringFloat;
import tl.lin.data.pair.PairOfStrings;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);

  /**
   * @param tokens
   *    tokens of query
   * @param windowSize
   *    window size of each "phrase" to be extracted
   * @return
   *    all consecutive token sequences of <i>windowSize</i> length
   */
  public static String[] extractPhrases(String[] tokens, int windowSize) {
    int numWindows = tokens.length - windowSize;
    String[] phrases = new String[numWindows];
    for (int start = 0; start < numWindows; start++) {
      String phrase = "";
      for (int k = 0; k <= windowSize; k++) {
        int cur = start + k;
        phrase = phrase + tokens[cur]+" ";
      }
      phrase = phrase.trim();
      phrases[start] = phrase;
    }
    return phrases;
  }

  /**
   * Helper function for <i>generateTranslationTable</i>
   * 
   * @param fPhrase
   * @param transPhrase
   * @param prob
   * @param phrase2score
   * @param phrase2count
   */
  private static void addToPhraseTable(String fPhrase, String transPhrase, float prob, Map<String, HMapSFW> phrase2score, Map<String, HMapKI<String>> phrase2count){
    fPhrase = fPhrase.trim();
    transPhrase = transPhrase.trim();

    //LOG.info("Found translation phrase " + transPhrase);

    if (!phrase2score.containsKey(fPhrase)) {
      phrase2score.put(fPhrase, new HMapSFW());
    }
    // if same phrase extracted from multiple rules, average prob.s

    HMapKF<String> scoreTable = phrase2score.get(fPhrase);

    if (!phrase2count.containsKey(fPhrase)) {
      phrase2count.put(fPhrase, new HMapKI<String>());
    }
    HMapKI<String> countTable = phrase2count.get(fPhrase);

    // case1 : first time we've seen phrase (fPhrase, transPhrase)
    if (!scoreTable.containsKey(transPhrase)) {
      scoreTable.put(transPhrase, prob);    // update score in table
      countTable.increment(transPhrase, 1);     // update count in table
    }else {               // case2 : we've seen phrase (fPhrase, transPhrase) before. update the average prob.
      int count = countTable.get(transPhrase);    // get current count
      float scoreUpdated = (scoreTable.get(transPhrase)*count + prob) / (count+1);    // compute updated average
      scoreTable.put(transPhrase, scoreUpdated);    // update score in table
      countTable.increment(transPhrase, 1);     // update count in table
    }

  }

  /**
   * Remove stop words from text that has been tokenized. Useful when postprocessing output of MT system, which is tokenized but not stopword'ed.
   *  
   * @param tokenizedText
   *    input text, assumed to be tokenized.
   * @return
   *    same text without the stop words.
   */
  public static String removeBorderStopWords(Tokenizer tokenizer, String tokenizedText) {
    String[] tokens = tokenizedText.split(" ");
    int start = 0, end = tokens.length-1;

    if (start == end) {
      return tokenizedText;
    }

    for (int i = 0; i < tokens.length; i++) {
      if (!tokenizer.isStopWord(tokens[i])) {
        start = i;
        break;
      }
    }
    for (int i = tokens.length-1; i >= 0; i--) {
      if (!tokenizer.isStopWord(tokens[i])) {
        end = i;
        break;
      }
    }

    String output = "";
    for (int i = start; i <= end; i++) {
      output += ( tokens[i] + " " );
    }
    return output.trim();
  }

  /**
   * For a 1-to-many alignment, check if the source token is aligned to a consecutive sequence of target tokens 
   * @param lst
   */
  private static boolean isConsecutive(ArrayListOfInts lst) {
    if (lst.size() == 0)    return true;

    int prev = -1;
    for(int i : lst){
      if(prev != -1 && i > prev+1){
        return false;
      }
      prev = i;
    }
    return true;
  }

  /**
   * For a 1-to-many alignment, check if the source token is aligned to a consecutive sequence of target tokens (allowing stopwords to interrupt consecutiveness) 
   * @param lst
   * @param tokenizer 
   * @param rhs 
   */
  private static String isConsecutiveWithStopwords(ArrayListOfInts lst, String[] rhs, Tokenizer tokenizer) {
    if (lst.size() == 0)    return null;
    String target = "";

    int id = 0, prev = -1;
    while(id < lst.size()){
      int trg = lst.get(id);
      if(prev != -1 && trg > prev+1){
        if (!tokenizer.isStopWord(rhs[trg])) {
          return null;
        }
      }
      target += rhs[trg] + " ";
      prev = trg;
      id++;
    }
    return target.trim();
  }

  /**
   * Read SCFG (synchronous context-free grammar) and convert into a set of probability distributions, one per source token that appear on LHS of any rule in the grammar
   * @param conf
   *    read grammar file from Configuration object
   * @param docLangTokenizer
   *    to check for stopwords on RHS
   */
  public static Map<String, HMapSFW> generateTranslationTable(FileSystem fs, Configuration conf, String grammarFile, Tokenizer queryLangTokenizer, Tokenizer docLangTokenizer) {
    if (conf.getBoolean(Constants.Quiet, false)) {
      LOG.setLevel(Level.OFF);
    }
    LOG.info("Generating translation table from " + grammarFile);

    boolean isPhrase = conf.getInt(Constants.MaxWindow, 0) > 0;
    int one2many = conf.getInt(Constants.One2Many, 2);

    // scfgDist table is a set of (source_token --> X) maps, where X is a set of (token_trans --> score) maps
    Map<String,HMapSFW> scfgDist = new HashMap<String,HMapSFW>();

    // phrase2count table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> count) maps
    HMapSFW phraseDist = new HMapSFW();

    HMapSIW srcTokenCnt = new HMapSIW();

    Set<String> bagOfTargetTokens = new HashSet<String>();

    try {
      FSDataInputStream fis = fs.open(new Path(grammarFile));
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader r = new BufferedReader(isr);

      String rule = null;
      while ((rule = r.readLine())!=null) {
        processRule(one2many, isPhrase, -1, rule, bagOfTargetTokens, scfgDist, phraseDist, srcTokenCnt, queryLangTokenizer, docLangTokenizer, null, null);
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return null;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }

    return scfgDist;
  }

  public static void processRule(int isOne2Many, boolean isMany2Many, float score, String rule, Set<String> bagOfTargetTokens, Map<String, HMapSFW> probDist, 
      HMapSFW phraseDist, HMapSIW srcTokenCnt, Tokenizer queryLangTokenizer, Tokenizer docLangTokenizer, Map<String,String> stemmed2Stemmed, Set<String> unknownWords) {
    //    LOG.info("Processing rule " + rule);

    String[] parts = rule.split("\\|\\|\\|");
    String[] lhs = parts[0].trim().split(" ");
    String[] rhs = parts[1].trim().split(" ");
    String[] probs = parts[2].trim().split(" ");
    float ruleProb = 0;

    boolean isPassThrough = false;
    if (probs[0].equals("PassThrough")) {
      isPassThrough = true;
    }else {
      // get direct phrase translation probability Pr(e|f)
      ruleProb =  (float) Math.pow(Math.E, -Float.parseFloat(probs[0]));     // cdec uses -ln(prob)
    }
    // if non-negative weight is given, use that instead
    float weight = (score == -1) ? ruleProb : score;

    HMapIV<ArrayListOfInts> one2manyAlign = null;
    if (!isPassThrough){
      String[] alignments = parts[3].trim().split(" ");
      one2manyAlign = readAlignments(alignments);
    }

    // in SCFG rule such as a b X1 X2 c --> X1 d e X2 f, we want to find out the src/trg tokens that are aligned to some trg/src token, ignoring the X variable
    // we can then decide if we want to include it as a multi-token phrase in our query representation based on various heuristics (e.g., only include if no X in between of tokens)
    String fPhrase = "";
    ArrayListOfInts sourceTokenIds = new ArrayListOfInts();      
    ArrayListOfInts targetTokenIds = new ArrayListOfInts();
    int f=0;
    for (; f < lhs.length; f++) {
      String fTerm = lhs[f];
      if (queryLangTokenizer.isStopWord(fTerm) || fTerm.matches("\\[X,\\d+\\]") || fTerm.matches("<s>") || fTerm.matches("</s>")) {
        continue;
      }

      srcTokenCnt.increment(fTerm);
      sourceTokenIds.add(f);

      ArrayListOfInts ids;
      if (isPassThrough){
        ids = new ArrayListOfInts();
        ids.add(0);
      }else {
        ids = one2manyAlign.get(f);
      }

      if (ids == null || (isOne2Many == 0 && ids.size() > 1)) {
        continue;
      }

      // find phrase in LHS and match to phrase in RHS
      if (isMany2Many) {
        fPhrase += fTerm + " ";
        targetTokenIds = targetTokenIds.mergeNoDuplicates(ids);        
      }

      String eTerm = null;
      for (int e : ids) {
        eTerm = rhs[e];

        // assumption: if this is pass-through rule, re-stem token in doc-language
        if (isPassThrough || (unknownWords != null && unknownWords.contains(fTerm))) {
          eTerm = stemmed2Stemmed.get(eTerm);
        }

        if (eTerm == null || docLangTokenizer.isStopWord(eTerm)) {
          //          LOG.info("Skipped trg token " + eTerm);
          eTerm = null;
          continue;      
        }
        bagOfTargetTokens.add(eTerm);
        if (isOne2Many <= 1) {
          if (probDist.containsKey(fTerm)) {
            HMapSFW eToken2Prob = probDist.get(fTerm);
            eToken2Prob.increment(eTerm, weight);
          }else {
            HMapSFW eToken2Prob = new HMapSFW();
            eToken2Prob.put(eTerm, weight);
            probDist.put(fTerm, eToken2Prob);
          }
        }
      }

      if (isOne2Many == 2) {
        // if ids.size() > 1 eTerm is a multi-token expression
        // even if eTerm is overwritten here, we need to do above loop to update bagOfTargetTokens
        if (ids.size() > 1) {
          eTerm = isConsecutiveWithStopwords(ids, rhs, docLangTokenizer);     // <---- heuristic
        }

        // no proper translation on target-side (e.g., stopword OR non-consecutive multi-word translation), let's skip
        if (eTerm == null) {
          continue;
        }

        eTerm = Utils.removeBorderStopWords(docLangTokenizer, eTerm);
        
        // this is difference between one-to-many and one-to-one heuristics for 1-best MT case
        // we add multi-token expressions in addition to single target tokens,
        bagOfTargetTokens.add(eTerm);

        // update prob. distr.
        if (probDist.containsKey(fTerm)) {
          HMapSFW eToken2Prob = probDist.get(fTerm);
          eToken2Prob.increment(eTerm, weight);
        }else {
          HMapSFW eToken2Prob = new HMapSFW();
          eToken2Prob.put(eTerm, weight);
          probDist.put(fTerm, eToken2Prob);
        }
      }
    }

    if (!isMany2Many) {
      return;
    }

    // if there are unaligned source tokens (other than nonterminal symbols and sentence boundary markers)
    // skip this rule
    if (f < lhs.length) {
      LOG.debug("Unaligned source token");
      return;
    }

    // possible heuristic: only accept multi-token phrases in which both source and target tokens are not split by a variable. 
    // e.g.,  a b X1 ---> X1 d e ||| 0-1 1-2    here "d e" is a valid translation of "a b" wrt this heuristic 
    if (!isConsecutive(targetTokenIds) || !isConsecutive(sourceTokenIds)) {
      LOG.debug("Non-consecutive target");
      return; 
    }

    // construct target phrase based on aligned target tokens
    String transPhrase = "";
    int tokensInPhrase = 0;
    for (int e : targetTokenIds) {
      String eTerm = rhs[e];
      if (eTerm.matches("\\[X,\\d+\\]") || eTerm.equals("<s>") || eTerm.equals("</s>")) {
        continue;
      }
      transPhrase += eTerm + " ";
      tokensInPhrase++;
    }

    // trim white space at end of the string
    transPhrase = transPhrase.trim();

    // if we have a pair of non-empty phrases
    if (!fPhrase.equals("") && !transPhrase.equals("") && tokensInPhrase > 1) {
      phraseDist.increment(transPhrase, weight);
      //      LOG.info("Adding phrase pair("+fPhrase+","+transPhrase+","+prob+") from "+rule);
//      addToPhraseTable(fPhrase, transPhrase, ruleProb, probDist, phrase2count);
    }

  }

  /**
   * Read alignments of one SCFG rule and convert into a 1-to-many mapping.
   * 
   * @param alignments
   *    a list of alignments, each one in the form f-e where f denotes position of source token in LHS of rule, and e denotes position of target token in RHS of rule
   * @return
   *    a mapping, where each entry is from a single source token position to a list of aligned target tokens (since 1-to-many alignments are allowed)
   */
  private static HMapIV<ArrayListOfInts> readAlignments(String[] alignments) {
    HMapIV<ArrayListOfInts> one2manyAlign = new HMapIV<ArrayListOfInts>();
    for(String alignment : alignments){

      String[] alPair = alignment.split("-");
      int f = Integer.parseInt(alPair[0]);
      int e = Integer.parseInt(alPair[1]);

      if(!one2manyAlign.containsKey(f)){
        one2manyAlign.put(f, new ArrayListOfInts());  
      }
      one2manyAlign.get(f).add(e);
    }

    // for each source token id, sort ids of its translations in ascending order
    for(Integer f : one2manyAlign.keySet()) {
      ArrayListOfInts lst = one2manyAlign.get(f);
      lst.sort();
      one2manyAlign.put(f, lst);
    }

    return one2manyAlign;
  }

  /**
   * Scale a probability distribution (multiply each entry with <i>scale</i>), then filter out entries below <i>threshold</i>
   * 
   * @param threshold
   * @param scale
   * @param probMap
   */
  public static HMapSFW scaleProbMap(float threshold, float scale, HMapSFW probMap) {
    HMapSFW scaledProbMap = new HMapSFW();

    for (MapKF.Entry<String> entry : probMap.entrySet()) {
      float pr = entry.getValue() * scale;
      if (pr > threshold) {
        scaledProbMap.put(entry.getKey(), pr);
      }
    }

    return scaledProbMap;
  }

  /**
   * Take a weighted average of a given list of prob. distributions. 
   * @param threshold
   *    we can put a lowerbound on final probability of entries
   * @param scale
   *    value between 0 and 1 that determines total probability in final distribution (e.g., 0.2 scale will scale [0.8 0.1 0.1] into [0.16 0.02 0.02])
   * @param probMaps
   *    list of probability distributions
   */
  public static HMapSFW combineProbMaps(float threshold, float scale, List<PairOfFloatMap> probMaps) {
    HMapSFW combinedProbMap = new HMapSFW();

    int numDistributions = probMaps.size();

    // get a combined set of all translation alternatives
    // compute normalization factor when sum of weights is not 1.0
    Set<String> translationAlternatives = new HashSet<String>();
    float sumWeights = 0;
    for (int i=0; i < numDistributions; i++) {
      HMapSFW dist = probMaps.get(i).getMap();
      float weight = probMaps.get(i).getWeight();

      // don't add vocabulary from a distribution that has 0 weight
      if (weight > 0) {
        translationAlternatives.addAll(dist.keySet());
        sumWeights += weight;
      }
    }

    // normalize by sumWeights
    for (String e : translationAlternatives) {
      float combinedProb = 0f;
      for (int i=0; i < numDistributions; i++) {
        HMapSFW dist = probMaps.get(i).getMap();
        float weight = probMaps.get(i).getWeight();
        combinedProb += (weight/sumWeights) * dist.get(e);    // Prob(e|f) = weighted average of all distributions
      }
      combinedProb *= scale;
      if (combinedProb > threshold) {
        combinedProbMap.put(e, combinedProb);
      }
    }

    return combinedProbMap;
  }


  /**
   * Given a distribution of probabilities, normalize so that sum of prob.s is exactly 1.0 or <i>cumProbThreshold</i> (if lower than 1.0). 
   * If we want to discard entries with prob. below <i>lexProbThreshold</i>, we do that after initial normalization, then re-normalize before cumulative thresholding.
   * If we want to keep at most <i>maxNumTrans</i> translations in final distribution, it can be specified.
   * @param probMap
   * @param lexProbThreshold
   * @param cumProbThreshold
   * @param maxNumTrans
   */
  public static void normalize(Map<String, HMapSFW> probMap, float lexProbThreshold, float cumProbThreshold, int maxNumTrans) {
    for (String sourceTerm : probMap.keySet()) {
      HMapSFW probDist = probMap.get(sourceTerm);
      TreeSet<PairOfStringFloat> sortedFilteredProbDist = new TreeSet<PairOfStringFloat>();
      HMapSFW normProbDist = new HMapSFW();

      // compute normalization factor
      float sumProb = 0;
      for (MapKF.Entry<String> entry : probDist.entrySet()) {
        sumProb += entry.getValue(); 
      }

      // normalize values and remove low-prob entries based on normalized values
      float sumProb2 = 0;
      for (MapKF.Entry<String> entry : probDist.entrySet()) {
        float pr = entry.getValue() / sumProb;
        if (pr > lexProbThreshold) {
          sumProb2 += pr;
          sortedFilteredProbDist.add(new PairOfStringFloat(entry.getKey(), pr));
        }
      }

      // re-normalize values after removal of low-prob entries
      float cumProb = 0;
      int cnt = 0;
      while (cnt < maxNumTrans && cumProb < cumProbThreshold && !sortedFilteredProbDist.isEmpty()) {
        PairOfStringFloat entry = sortedFilteredProbDist.pollLast();
        float pr = entry.getValue() / sumProb2;
        cumProb += pr;
        normProbDist.put(entry.getKey(), pr);
        cnt++;
      }

      probMap.put(sourceTerm, normProbDist);
    }
  }

  /**
   * L1-normalization
   * 
   * @param probMap
   */
  public static void normalize(HMapSFW probMap) {
    float normalization = 0;
    for (MapKF.Entry<String> e : probMap.entrySet()) {
      float weight = e.getValue();
      normalization += weight;
    }
    for (MapKF.Entry<String> e : probMap.entrySet()) {
      probMap.put(e.getKey(), e.getValue()/normalization);
    }              
  }

  public static void filter(HMapSFW probMap, float lexProbThreshold) {
    for (MapKF.Entry<String> e : probMap.entrySet()) {
      if (e.getValue() > lexProbThreshold) {
        probMap.put(e.getKey(), e.getValue());
      }
    }              
  }

  /**
   * Create a mapping between query-language stemming and document-language stemming (if both are turned on). If there is a query token for which we do 
   * not have any translation, it is helpful to search for that token in documents. However, since we perform stemming on documents 
   * with doc-language stemmer, we might miss some. 
   * 
   * Example: In query 'emmy award', if we dont know how to translate emmy, we should search for 'emmy' in French documents, instead of 'emmi', which is how it's stemmed in English.
   * 
   * @param origQuery
   * @param queryLangTokenizer
   *    no stemming or stopword removal
   * @param docLangTokenizer
   *    no stopword removal, stemming enabled
   */
  public static Map<String, String> getStemMapping(String origQuery, Tokenizer queryLangTokenizer, Tokenizer docLangTokenizer) {
    Map<String, String> map = new HashMap<String, String>();
    Map<String, String> stem2NonStemMapping = queryLangTokenizer.getStem2NonStemMapping(origQuery);

    for (String stem : stem2NonStemMapping.keySet()) {
      String token = stem2NonStemMapping.get(stem);
      String docStem = docLangTokenizer.processContent(token.trim())[0];
      map.put(stem, docStem);
    }
    return map;
  }

  public static String getSetting(Configuration conf) {
    return conf.get(Constants.RunName) + "_" + conf.getInt(Constants.KBest, 0) + 
    "-" + (int)(100*conf.getFloat(Constants.MTWeight, 0)) + 
    "-" + (int) (100*conf.getFloat(Constants.BitextWeight, 0)) +
    "-" + (int) (100*conf.getFloat(Constants.TokenWeight, 0)) +
    "_" + conf.getInt(Constants.One2Many, 2);
  }

  public static JsonArray createJsonArray(String[] elements) {
    JsonArray arr = new JsonArray();
    for (String s: elements) {
      arr.add(new JsonPrimitive(s));
    }
    return arr;
  }

  /**
   * Convert prob. distribution to JSONArray in which float at position 2k corresponds to probabilities of term at position 2k+1, k=0...(n/2-1)
   * @param probMap
   */
  public static JsonArray createJsonArrayFromProbabilities(HMapSFW probMap) {
    if (probMap == null) {
      return null;
    }

    JsonArray arr = new JsonArray();
    for(MapKF.Entry<String> entry : probMap.entrySet()) {
      arr.add(new JsonPrimitive(entry.getValue()));
      arr.add(new JsonPrimitive(entry.getKey()));
    }
    return arr;
  }

  public static Set<String> readUnknowns(FileSystem fs, String unkFile) {
    if(unkFile == null) {
      return null;
    }
    Set<String> unkWords = new HashSet<String>();
    try {
      Path p = new Path(unkFile);
      FSDataInputStream fis = fs.open(p);
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader r = new BufferedReader(isr);

      String line = null;
      while ((line = r.readLine())!=null) {
        String[] tokens = line.split("\\s+");
        for (String token : tokens) {
          if (!token.trim().equals("")) {
            unkWords.add(token.trim());
          }
        }
      }
    }catch(Exception e) {
      LOG.info("Problem reading unknown words file: " + unkFile);
      return null;
    }
    return unkWords;
  }

  public static List<String> readOriginalQueries(FileSystem fs, String originalQueriesFile) {
    List<String> originalQueries = new ArrayList<String>();
    try{
      FSDataInputStream fis = fs.open(new Path(originalQueriesFile));

      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader r = new BufferedReader(isr);

      String line = null;
      while ((line = r.readLine())!=null) {
        originalQueries.add(line.trim());
      }
    }catch(Exception e) {
      LOG.info("Problem reading original queries file: " + originalQueriesFile);
      return null;
    }
    return originalQueries;
  }

  public static Set<PairOfStrings> getPairsInSCFG(FileSystem fs, String grammarFile) {
    Set<PairOfStrings> pairsInSCFG = new HashSet<PairOfStrings>();
    try {
      FSDataInputStream fis = fs.open(new Path(grammarFile));

      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader reader = new BufferedReader(isr);

      String rule = null;
      while ((rule = reader.readLine())!=null) {
        String[] parts = rule.split("\\|\\|\\|");
        String[] lhs = parts[0].trim().split(" ");
        String[] rhs = parts[1].trim().split(" ");;
        for (String l : lhs) {
          for (String r : rhs) {
            pairsInSCFG.add(new PairOfStrings(l, r));
          }
        }
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return null;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return pairsInSCFG;
  }

  public static String ivory2Indri(String structuredQuery) {
    return structuredQuery.replaceAll("\"#combine\":","#combine").replaceAll("\"#weight\":","#weight").replaceAll("\\{","").replaceAll("\\}","").replaceAll("\\[","(").replaceAll("\\]",")").replaceAll("\","," ").replaceAll(",\""," ").replaceAll("\"\\),",") ").replaceAll("\"\\)\\)","))");
  }
}
