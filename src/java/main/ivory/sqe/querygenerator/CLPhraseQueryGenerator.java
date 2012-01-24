package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.log.Log;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.HMapKI;


public class CLPhraseQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(CLPhraseQueryGenerator.class);
  private Tokenizer tokenizer;
  private Map<String, HMapKF<String>> phraseTable;
  private int length;
  private static final int AVG = 0, MAX = 1, SUM = 2;
  private static final int OFF = 0, ON = 1;
  private static final int COMBINE = 0, PERTOKEN = 1, COVER = 2;
  private int H1, H2, H3, H4;
  
  public CLPhraseQueryGenerator() throws IOException {
    super();
  }

  public void init(FileSystem fs, Configuration conf) throws IOException {
    LOG.info(conf.get(Constants.Language));
    LOG.info(conf.get(Constants.TokenizerData));
    LOG.info(conf.get(Constants.Heuristic1));
    LOG.info(conf.getBoolean(Constants.Heuristic2, false));
    LOG.info(conf.get(Constants.Heuristic3));
    LOG.info(conf.get(Constants.SCFGPath));

    String h4 = conf.get(Constants.Heuristic4); 
    if (h4.equals("combine")) {
      H4 = COMBINE;
    }else if (h4.equals("token")) {
      H4 = PERTOKEN;
    }else if (h4.equals("cover")) {
      H4 = COVER;
    }else {
      LOG.info("Using default value for heuristic H4 = NONE");
      H4 = COMBINE;    // default
    }
    LOG.info("H4 = " + H4);

    String h1 = conf.get(Constants.Heuristic1); 
    if (h1.equals("off")) {
      H1 = OFF;
    }else {
      H1 = ON;
    }
    LOG.info("H1 = " + H1);

//    H2 = conf.getBoolean(Constants.Heuristic2, false);

    String h3 = conf.get(Constants.Heuristic3); 
    if (h3.equals("avg")) {
      H3 = AVG;
    }else if (h3.equals("sum")) {
      H3 = SUM;
    }else if (h3.equals("max")) {
      H3 = MAX;
    }else {
      LOG.info("Using default value for heuristic H3 = AVG");
      H3 = AVG;		// default
    }
    LOG.info("H3 = " + H3);


    phraseTable = generatePhraseTable(conf.get(Constants.SCFGPath));
    tokenizer = TokenizerFactory.createTokenizer(fs, conf.get(Constants.Language), conf.get(Constants.TokenizerData), null);
  }

  public JSONObject parseQuery(String query) {
    JSONObject queryJson = new JSONObject();
    try {
      JSONArray phraseTranslations = new JSONArray();

      String[] tokens = tokenizer.processContent(query.trim());
      length = tokens.length;
      //			boolean[] isCovered = new boolean[length];

      int MaxWindow = 1;	
      int MinWindow = MaxWindow;	
      if (H1 == ON) {
        MinWindow = 0;
      }

      // for query "t0 t1 t2 ...", identify multi-token windows or phrases (e.g. "t1 t2"), then search for translations in SCFG.
      // H1 = COVER => do not attempt translating a word, if w can be translated within some phrase
      // i.e., prefer translating phrases over words
      // H1 = COMBINE => add all phrase translations (regardless of window size) into a single #combine structure
      // H1 = WEIGHT => for each token ti, find all phrase translations that contain ti and put them in a #weight structure
      int numWindowSizes = MaxWindow-MinWindow+1;
      JSONObject[][] pweightObjects = new JSONObject[numWindowSizes][];
      boolean isCovered[] = new boolean[length];
      for (int wIndex = numWindowSizes-1; wIndex >= 0; wIndex--){
        int windowSize = MinWindow + wIndex;
        int numWindows = length - windowSize;
        pweightObjects[wIndex] = new JSONObject[numWindows];
        for (int start = 0; start < numWindows; start++) {
          if (H4 == COVER && windowSize == 0 && isCovered[start])   continue;
          String phrase = "";
          for (int k = 0; k <= windowSize; k++) {
            int cur = start + k;
            phrase = phrase + tokens[cur]+" ";
          }
          //LOG.info("w="+wIndex);
          //LOG.info("start="+start);
          JSONArray weights = getPhraseTranslations(phrase.trim());
          if (weights != null) {		
            //LOG.info("Added "+phrase+" --> "+weights);
            JSONObject phraseTrans = new JSONObject();
            phraseTrans.put("#pweight", weights);
            if (H4 == PERTOKEN) {
              pweightObjects[wIndex][start] = phraseTrans;
            }else if (H4 == COVER){
              // if we translate a phrase that contains token t, we mark t as covered and don't add any more phrase representations
              for (int k = 0; k <= windowSize; k++) {
                int cur = start + k;
                isCovered[cur] = true;
              }
              phraseTranslations.put(phraseTrans);
            }else {
              phraseTranslations.put(phraseTrans);
            }
          }else {
            // ????
            //LOG.info("Skipped "+phrase);
          }
        }
      }
      if (H4 == PERTOKEN) {
        // represent each token with an array of #pweight objects = tokenArr
        // represent query by #combine of token representations = queryArr

        JSONArray queryArr = new JSONArray();
        for (int i = 0; i < length; i++) {
          //LOG.info("Token # "+ i);
          JSONArray tokenArr = new JSONArray();		
          //				   i
          //				   0	   1		2		3 ...
          //				  ---------------------------------
          //			  w	0 | j=0    j=1		j=2		j=3
          //				1 | j=0	   j=0,1 	j=1,2	j=2,3	
          for (int wIndex = 0; wIndex < numWindowSizes; wIndex++){
            int windowSize = MinWindow + wIndex;
            //LOG.info("Window size="+windowSize);
            // j are the indices of pweightObjects[w] that (partially) represent token i
            for (int j = i-windowSize; j >= 0 && j <= i && j < pweightObjects[wIndex].length; j++){
              //LOG.info("Index="+j);
              if (pweightObjects[wIndex][j] != null) {
                tokenArr.put(pweightObjects[wIndex][j]);
              }
            }
          }

          // how many pweight objects are in the representation of token i
          int numObjects = tokenArr.length();

          if (numObjects  > 0) {
            // representation of each token is an array of corr. pweight and weight JSonObjects
            JSONObject wordAndPhraseTrans = new JSONObject();

            for (int j = numObjects-1; j >= 0; j--) {						
              tokenArr.put(2*j+1, tokenArr.get(j));
              tokenArr.put(2*j, 1.0f/numObjects);
            }
            wordAndPhraseTrans.put("#weight", tokenArr);
            queryArr.put(wordAndPhraseTrans);

            //LOG.info("combined = " + wordAndPhraseTrans.toString(1));
          }
        }

        queryJson.put("#combine", queryArr);
      } else {
        // represent query by #combine of all possible multi-token representations
        queryJson.put("#combine", phraseTranslations);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return queryJson;
  }

  public int getQueryLength(){
    return length;  
  }

  protected JSONArray getPhraseTranslations(String phrase) {
    // get a set of (phrase_trans --> prob) maps
    HMapKF<String> translation2prob = phraseTable.get(phrase);
    if(translation2prob==null){
      return null;
    }

    JSONArray phraseTranslationsArr = new JSONArray();
    float sumProbEF = 0;
    for (String translation : translation2prob.keySet()) {
      try {
        float prob = translation2prob.get(translation);
        sumProbEF += prob;
        phraseTranslationsArr.put(prob);
        phraseTranslationsArr.put(translation);
      } catch (JSONException e) {
        throw new RuntimeException("Error adding translation and prob values");
      }
    }
    
    for (int i = 0; i < phraseTranslationsArr.length(); i=i+2){
      try {
        float pr = (float) phraseTranslationsArr.getDouble(i);
        phraseTranslationsArr.put(i, pr/sumProbEF);
      } catch (JSONException e1) {
        throw new RuntimeException("Error normalizing");
      }
    }

    return phraseTranslationsArr;
  }

  /**
   * @param grammarFile
   * 		grammar file that contains a SCFG grammar that has been extracted from GIZA++ alignments using Hiero w.r.t set of queries
   * @return
   * 		set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
   */
  private Map<String, HMapKF<String>> generatePhraseTable(String grammarFile) {
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
   * 		set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> score) maps
   * @param phrase2count
   * 		set of (source_phrase --> X) maps, where X is a set of (phrase_trans --> count) maps
   * @param lhs
   * 		LHS of grammar rule
   * @param rhs
   * 		RHS of grammar rule
   * @param probs
   * 		Phrase translation probabilities from grammar rule
   * @param one2manyAlign
   * 		map of alignments ( LHS token id x --> List of RHS token ids aligned to x )
   */
  private void updatePhraseTable(Map<String, HMapKF<String>> phrase2score, Map<String, HMapKI<String>> phrase2count, String[] lhs, String[] rhs, String[] probs, HMapIV<ArrayListOfInts> one2manyAlign) {
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
          if (fTerm.matches("\\[X,\\d+\\]") || fTerm.equals("<s>") || fTerm.equals("</s>"))	 break;
          phraseTranslationIds = phraseTranslationIds.mergeNoDuplicates(translationIds);
          fPhrase += fTerm+" ";
          cnt++;
        }		

        //				String fPhrase = getPhraseTranslation(f, w, lhs, one2manyAlign, phraseTranslationIds);

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
              scoreTable.increment(transPhrase, prob);		// sum
            }else if (H3 == AVG) {
              // H3 = average
              if (!phrase2count.containsKey(fPhrase)) {
                phrase2count.put(fPhrase, new HMapKI<String>());
              }
              HMapKI<String> countTable = phrase2count.get(fPhrase);

              // case1 : first time we've seen phrase (fPhrase, transPhrase)
              if (!scoreTable.containsKey(transPhrase)) {
//                LOG.debug("Phrase = "+fPhrase+" -> " +transPhrase);
                scoreTable.put(transPhrase, prob);		// update score in table
                countTable.increment(transPhrase, 1);			// update count in table
              }else {               // case2 : we've seen phrase (fPhrase, transPhrase) before. update the average prob.
                int count = countTable.get(transPhrase);		// get current count
                float scoreUpdated = (scoreTable.get(transPhrase)*count + prob) / (count+1);		// compute updated average
                scoreTable.put(transPhrase, scoreUpdated);		// update score in table
                countTable.increment(transPhrase, 1);			// update count in table
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

  //	private String getPhraseTranslation(int start, int size, String[] lhs, HMapIV<ArrayListOfInts> one2manyAlign, ArrayListOfInts phraseTranslationIds) {
  //
  //	}

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
