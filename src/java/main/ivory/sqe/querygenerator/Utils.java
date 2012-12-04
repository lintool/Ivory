package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.PairOfFloatMap;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfStringFloat;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKF.Entry;

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

  private static String readConf(Configuration conf) {
    String grammarFile = conf.get(Constants.SCFGPath);
    return grammarFile;
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
   * For a 1-to-many alignment, check if the source token is aligned to a consecutive sequence of target tokens 
   * @param lst
   */
  private static boolean isConsecutive(ArrayListOfInts lst) {
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
   * Read SCFG (synchronous context-free grammar) and convert into a set of probability distributions, one per source token that appear on LHS of any rule in the grammar
   * @param conf
   *    read grammar file from Configuration object
   * @param docLangTokenizer
   *    to check for stopwords on RHS
   */
  public static Map<String, HMapSFW> generateTranslationTable(FileSystem fs, Configuration conf, Tokenizer docLangTokenizer) {
    String grammarFile = readConf(conf);

//    LOG.info("Generating translation table from " + grammarFile);

    boolean isPhrase = conf.getInt(Constants.MaxWindow, 0) > 0;

    // phrase2score table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
    Map<String,HMapSFW> scfgDist = new HashMap<String,HMapSFW>();

    // phrase2count table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> count) maps
    Map<String,HMapKI<String>> phrase2count = new HashMap<String,HMapKI<String>>();

    try {
      FSDataInputStream fis = fs.open(new Path(grammarFile));
      InputStreamReader isr = new InputStreamReader(fis, "UTF8");
      BufferedReader r = new BufferedReader(isr);
      
      String rule = null;
      while ((rule = r.readLine())!=null) {
        String[] parts = rule.split("\\|\\|\\|");
        String[] lhs = parts[1].trim().split(" ");
        String[] rhs = parts[2].trim().split(" ");
        String[] probs = parts[3].trim().split(" ");
        float prob = (float) Math.pow(Math.E, -Float.parseFloat(probs[0]));     // cdec uses -ln(prob)

        String[] alignments = parts[4].trim().split(" ");
        HMapIV<ArrayListOfInts> one2manyAlign = readAlignments(alignments);

        // append all target tokens that are aligned to some source token (other than nonterminals and sentence boundary markers)
        String fPhrase = "";
        ArrayListOfInts sourceTokenIds = new ArrayListOfInts();
        ArrayListOfInts targetTokenIds = new ArrayListOfInts();
        int f = 0;
        for (; f < lhs.length; f++) {
          String fTerm = lhs[f];
          if (fTerm.matches("\\[X,\\d+\\]") || fTerm.matches("<s>") || fTerm.matches("</s>")) {
            continue;
          }
          sourceTokenIds.add(f);
          ArrayListOfInts ids = one2manyAlign.get(f);

          // word-to-word translations
          if (!isPhrase) {
            // source token should be aligned to single target token
            if (ids == null || ids.size() != 1) continue;
            for (int e : ids) {
              String eTerm = rhs[e];
              if (docLangTokenizer.isStemmedStopWord(eTerm))  continue;
              if (scfgDist.containsKey(fTerm)) {
                HMapSFW eToken2Prob = scfgDist.get(fTerm);
                if(eToken2Prob.containsKey(eTerm)) {
                  eToken2Prob.increment(eTerm, prob);
                }else {
                  eToken2Prob.put(eTerm, prob);
                }
              }else {
                HMapSFW eToken2Prob = new HMapSFW();
                eToken2Prob.put(eTerm, prob);
                scfgDist.put(fTerm, eToken2Prob);
              }
            }
            // keep track of alignments to identify source and target phrases
          }else {
            fPhrase += fTerm + " ";
            targetTokenIds = targetTokenIds.mergeNoDuplicates(ids);
          }

        }       

        if (!isPhrase) {
          continue;
        }

        LOG.debug(rule);

        // if there are unaligned source tokens (other than nonterminal symbols and sentence boundary markers)
        // skip this rule
        if (f < lhs.length) {
          LOG.debug("Unaligned source token");
          continue;
        }
        // if you want a consecutive sequence of tokens, check here...
        if (!isConsecutive(targetTokenIds) || !isConsecutive(sourceTokenIds)) {
          LOG.debug("Non-consecutive target");
          continue; 
        }

        // construct target phrase based on aligned target tokens
        String transPhrase = "";
        for (int e : targetTokenIds) {
          String eTerm = rhs[e];
          if (eTerm.matches("\\[X,\\d+\\]") || eTerm.equals("<s>") || eTerm.equals("</s>")) {
            continue;
          }
          transPhrase += eTerm + " ";
        }

        // trim white space at end of the string
        fPhrase = fPhrase.trim();
        transPhrase = transPhrase.trim();

        // if we have a pair of non-empty phrases
        if (!fPhrase.equals("") && !transPhrase.equals("")) {
//          LOG.info("Adding phrase pair("+fPhrase+","+transPhrase+","+prob+") from "+rule);
          addToPhraseTable(fPhrase, transPhrase, prob, scfgDist, phrase2count);
        }
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return scfgDist;
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
    
    for (Entry<String> entry : probMap.entrySet()) {
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
      for (Entry<String> entry : probDist.entrySet()) {
        sumProb += entry.getValue(); 
      }

      // normalize values and remove low-prob entries based on normalized values
      float sumProb2 = 0;
      for (Entry<String> entry : probDist.entrySet()) {
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
   * Create a mapping between query-language stemming and document-language stemming. If there is a query token for which we do 
   * not have any translation, it is helpful to search for that token in documents. However, since we perform stemming on documents 
   * with doc-language stemmer, we might miss some. 
   * 
   * Example: In query 'emmy award', if we dont know how to translate emmy, we should search for 'emmi' in French documents, instead of 'emmy'.
   * 
   * @param origQuery
   * @param queryLangTokenizer
   *    no stemming or stopword removal
   * @param queryLangTokenizerWithStemming
   *    no stopword removal, stemming enabled  
   * @param docLangTokenizer
   *    no stopword removal, stemming enabled
   */
  public static Map<String, String> getStemMapping(String origQuery, Tokenizer queryLangTokenizer, Tokenizer queryLangTokenizerWithStemming, Tokenizer docLangTokenizer) {
    Map<String, String> map = new HashMap<String, String>();

    // strip out punctuation to prevent problems (FIX THIS)
    // ===> this aims to remove end of sentence period but accidentally removes last dot from u.s.a. as well
    //    origQuery = origQuery.replaceAll("\\?\\s*$", "").replaceAll("\"", "").replaceAll("\\(", "").replaceAll("\\)", "");  
    
    String[] tokens = queryLangTokenizer.processContent(origQuery);

    for (int i = 0; i < tokens.length; i++) {
      String stem1 = queryLangTokenizerWithStemming.processContent(tokens[i].trim())[0];
      String stem2 = docLangTokenizer.processContent(tokens[i].trim())[0];
      map.put(stem1, stem2);
    }
    return map;
  }
  
  public static String getSetting(Configuration conf) {
    return conf.get(Constants.RunName) + "_" + conf.getInt(Constants.KBest, 0) + 
          "-" + (int)(100*conf.getFloat(Constants.MTWeight, 0)) + 
          "-" + (int) (100*conf.getFloat(Constants.BitextWeight, 0))+ 
          "-" + (int) (100*conf.getFloat(Constants.TokenWeight, 0));
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
    for(Entry<String> entry : probMap.entrySet()) {
      arr.add(new JsonPrimitive(entry.getValue()));
      arr.add(new JsonPrimitive(entry.getKey()));
    }
    return arr;
  }

}
