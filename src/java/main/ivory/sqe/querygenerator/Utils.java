package ivory.sqe.querygenerator;

import ivory.sqe.retrieval.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.HMapKI;

public class Utils {
  private static final int AVG = 0, MAX = 1, SUM = 2;
  private static final int OFF = 0, ON = 1;
  private static int H1, H2, H3;
  
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
    String h1 = conf.get(Constants.Heuristic1); 
    if (h1.equals("off")) {
      H1 = OFF;
    }else {
      H1 = ON;
    }
    String h3 = conf.get(Constants.Heuristic3); 
    if (h3.equals("avg")) {
      H3 = AVG;
    }else if (h3.equals("sum")) {
      H3 = SUM;
    }else if (h3.equals("max")) {
      H3 = MAX;
    }else {
      H3 = AVG;   // default
    }
    return grammarFile;
  }
  
  /**
   * @param grammarFile
   *    grammar file that contains a SCFG grammar that has been extracted from GIZA++ alignments using Hiero w.r.t set of queries
   * @return
   *    set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
   */
  public static Map<String, HMapKF<String>> generatePhraseTable(Configuration conf) {
    String grammarFile = readConf(conf);
    
    // phrase2score table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
    Map<String,HMapKF<String>> phrase2score = new HashMap<String,HMapKF<String>>();

    // phrase2count table is a set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> count) maps
    Map<String,HMapKI<String>> phrase2count = new HashMap<String,HMapKI<String>>();

    try {
      BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(grammarFile), "UTF-8"));
      String rule = null;
      while ((rule = r.readLine())!=null) {
//        LOG.info("SCFG rule = " + rule);
        String[] parts = rule.split("\\|\\|\\|");
        String[] lhs = parts[1].trim().split(" ");
        String[] rhs = parts[2].trim().split(" ");;
        String[] probs = parts[3].trim().split(" ");
        String[] alignments = parts[4].trim().split(" ");;

        // early termination: need more than 1 alignment pair to get a phrase translation
        if (alignments.length < 2)
          continue;

        HMapIV<ArrayListOfInts> one2manyAlign = readAlignments(alignments);

        // we have parsed all components of this grammar rule. now, update phrase table accordingly
        updatePhraseTable(phrase2score, phrase2count, lhs, rhs, probs, one2manyAlign);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return phrase2score;
  }

  /**
   * @param phrase2score
   *    set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
   * @param phrase2count
   *    set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> count) maps
   * @param lhs
   *    LHS of grammar rule
   * @param rhs
   *    RHS of grammar rule
   * @param probs
   *    Phrase translation probabilities from grammar rule
   * @param one2manyAlign
   *    map of alignments ( LHS token id x --> List of RHS token ids aligned to x )
   */
  private static void updatePhraseTable(Map<String, HMapKF<String>> phrase2score, Map<String, HMapKI<String>> phrase2count, String[] lhs, String[] rhs, String[] probs, HMapIV<ArrayListOfInts> one2manyAlign) {
    // if H1, minwindow is 1
    int MaxWindow = 1;  
    int MinWindow = MaxWindow;  
    if (H1 == ON) {
      MinWindow = 0;
    }
    float prob = (float) Math.pow(Math.E, -Float.parseFloat(probs[0]));

    for (int w = MinWindow; w <= MaxWindow; w++){
      // f is the beginning point of the phrase
      // w is the size of the window, starting after f

      int numWindows = lhs.length - w;
      for (int start = 0; start < numWindows; start++) {
        //LOG.info("w="+w);
        //LOG.info("start="+start);
        // phrase window = [f,f+w]

        ArrayListOfInts phraseTranslationIds = new ArrayListOfInts();

        String fPhrase = "";
        int cnt = 0;
        for (int cur = start; cur <= start + w; cur++) {
          ArrayListOfInts translationIds = one2manyAlign.get(cur);
          if (translationIds == null) {
            // if there are any unaligned source terms in this window, move to next f
            start = cur; 
            break;
          }
          String fTerm = lhs[cur];

          // if H2, don't break
          if (fTerm.matches("\\[X,\\d+\\]") || fTerm.equals("<s>") || fTerm.equals("</s>"))  break;
          phraseTranslationIds = phraseTranslationIds.mergeNoDuplicates(translationIds);
          fPhrase += fTerm+" ";
          cnt++;
        }   

        //        String fPhrase = getPhraseTranslation(f, w, lhs, one2manyAlign, phraseTranslationIds);

        // if there was no source well-defined phrase at [f, f+w] (i.e., previous loop hit a 'break' case), move to next value of f
        if (cnt < w+1) {
          continue;
        }

        //LOG.info("Found source phrase " + fPhrase + "\n -->" + phraseTranslationIds);

        // check if the translation of [f, f+w-1] is a well-defined phrase as well
        // allow 1-to-many and many-to-1 phrase pairs if H1 is set
        if ((phraseTranslationIds.size() > 1 || (H1 == ON && w > 0)) && isConsecutive(phraseTranslationIds)) {
          String transPhrase = "";
          boolean ignore = false;
          for (int e : phraseTranslationIds) {
            String eTerm = rhs[e];
            if (eTerm.matches("\\[X,\\d+\\]") || eTerm.equals("<s>") || eTerm.equals("</s>")) {
              ignore = true;
              break;
            }
            transPhrase += eTerm + " ";
          }

          // add phrase pair to table
          if (!ignore) {
            fPhrase = fPhrase.trim();
            transPhrase = transPhrase.trim();

            //LOG.info("Found translation phrase " + transPhrase);

            if (!phrase2score.containsKey(fPhrase)) {
              phrase2score.put(fPhrase, new HMapKF<String>());
            }
            // H3 = if same phrase extracted from multiple rules, add, average or take max of prob.s

            HMapKF<String> scoreTable = phrase2score.get(fPhrase);
            if (H3 == SUM) {
              // H3 = sum
              scoreTable.increment(transPhrase, prob);    // sum
            }else if (H3 == AVG) {
              // H3 = average
              if (!phrase2count.containsKey(fPhrase)) {
                phrase2count.put(fPhrase, new HMapKI<String>());
              }
              HMapKI<String> countTable = phrase2count.get(fPhrase);

              // case1 : first time we've seen phrase (fPhrase, transPhrase)
              if (!scoreTable.containsKey(transPhrase)) {
//                LOG.debug("Phrase = "+fPhrase+" -> " +transPhrase);
                scoreTable.put(transPhrase, prob);    // update score in table
                countTable.increment(transPhrase, 1);     // update count in table
              }else {               // case2 : we've seen phrase (fPhrase, transPhrase) before. update the average prob.
                int count = countTable.get(transPhrase);    // get current count
                float scoreUpdated = (scoreTable.get(transPhrase)*count + prob) / (count+1);    // compute updated average
                scoreTable.put(transPhrase, scoreUpdated);    // update score in table
                countTable.increment(transPhrase, 1);     // update count in table
              }
            } else {
              // H3 = take max

              // if first occurrence, OR if current prob is greater than max, set score to prob
              if (!scoreTable.containsKey(transPhrase) || prob > scoreTable.get(transPhrase)) {       
                scoreTable.put(transPhrase, prob);
              }
            }

          }
        }
      }
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
}
