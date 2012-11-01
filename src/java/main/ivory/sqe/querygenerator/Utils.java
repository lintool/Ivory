package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.PairOfFloatMap;
import java.io.BufferedReader;
import java.io.FileInputStream;
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
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
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

  public static Map<String, HMapSFW> generateTranslationTable(Configuration conf, Tokenizer docLangTokenizer) {
    String grammarFile = readConf(conf);

//    LOG.info("Generating translation table from " + grammarFile);

    boolean isPhrase = conf.getInt(Constants.MaxWindow, 0) > 0;

    // phrase2score table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
    Map<String,HMapSFW> scfgDist = new HashMap<String,HMapSFW>();

    // phrase2count table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> count) maps
    Map<String,HMapKI<String>> phrase2count = new HashMap<String,HMapKI<String>>();

    try {
      BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(grammarFile), "UTF-8"));
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

  public static JSONArray probMap2JSON(HMapSFW probMap) {
    if (probMap == null) {
      return null;
    }

    JSONArray arr = new JSONArray();
    try {
      for(Entry<String> entry : probMap.entrySet()) {
        arr.put(entry.getValue());
        arr.put(entry.getKey());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return arr;
  }

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
  
  public static Map<String, String> getStemMapping(String origQuery, Tokenizer queryLangTokenizer, Tokenizer queryLangTokenizerWithStemming, Tokenizer docLangTokenizer) {
    Map<String, String> map = new HashMap<String, String>();

    // strip out punctuation to prevent problems (FIX THIS)
    // ===> this aims to remove end of sentence period but accidentally removes last dot from u.s.a. as well
//    origQuery = origQuery.replaceAll("\\?\\s*$", "").replaceAll("\"", "").replaceAll("\\(", "").replaceAll("\\)", "");  
    
//    LOG.info(origQuery);
    // split by space so we can 
    String[] tokens = queryLangTokenizer.processContent(origQuery);
//    LOG.info(tokens.length);

    for (int i = 0; i < tokens.length; i++) {
//      LOG.info(tokens[i]);
      String stem1 = queryLangTokenizerWithStemming.processContent(tokens[i].trim())[0];
      String stem2 = docLangTokenizer.processContent(tokens[i].trim())[0];
//      LOG.info("<"+stem1+"->"+stem2+"<");
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
}
