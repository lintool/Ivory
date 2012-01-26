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


    phraseTable = Utils.generatePhraseTable(conf);
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
    float sumProb = 0;
    for (String translation : translation2prob.keySet()) {
      try {
        float prob = translation2prob.get(translation);
        sumProb += prob;
        phraseTranslationsArr.put(prob);
        phraseTranslationsArr.put(translation);
      } catch (JSONException e) {
        throw new RuntimeException("Error adding translation and prob values");
      }
    }
    
    // normalize weights
    for (int i = 0; i < phraseTranslationsArr.length(); i=i+2){
      try {
        float pr = (float) phraseTranslationsArr.getDouble(i);
        phraseTranslationsArr.put(i, pr/sumProb);
      } catch (JSONException e1) {
        throw new RuntimeException("Error normalizing");
      }
    }

    return phraseTranslationsArr;
  }
}
