package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class CLWordAndPhraseQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(CLWordAndPhraseQueryGenerator.class);
  private Tokenizer tokenizer;
  private int length;
  CLWordQueryGenerator clGenerator;
  CLPhraseQueryGenerator phraseGenerator;
  //  private static final int PERTOKEN = 1, COMBINE = 0;
  //  private int H4;
  private static final int OFF = 0, ON = 1;
  private static final int COMBINE = 0, PERTOKEN = 1, COVER = 2;
  private int H1, H4, H6;

  public CLWordAndPhraseQueryGenerator() throws IOException {
    super();
  }

  public void init(FileSystem fs, Configuration conf) throws IOException {
    LOG.info(conf.get(Constants.Language));
    LOG.info(conf.get(Constants.TokenizerData));
    LOG.info(conf.get(Constants.Heuristic1));
    LOG.info(conf.get(Constants.Heuristic3));
    LOG.info(conf.get(Constants.Heuristic4));
    LOG.info(conf.get(Constants.Heuristic6));

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

    tokenizer = TokenizerFactory.createTokenizer(fs, conf.get(Constants.Language), conf.get(Constants.TokenizerData), null);
    clGenerator = new CLWordQueryGenerator();
    clGenerator.init(fs, conf);
    phraseGenerator = new CLPhraseQueryGenerator();
    phraseGenerator.init(fs, conf);
  }

  public JSONObject parseQuery(String query) {
    JSONObject queryJson = new JSONObject();
    try {
      String[] tokens = tokenizer.processContent(query.trim());
      length = tokens.length;

      int maxWindow = 1;  
      int minWindow = maxWindow;  
      if (H1 == ON) {
        minWindow = 0;
      }

      // iterate over tokens and phrases, and create weighted representation for each. 
      // save representations into resp. arrays

      // phrase
      int numWindowSizes = maxWindow-minWindow+1;
      boolean isCovered[][] = new boolean[numWindowSizes][length];
      JSONObject[][] pweightObjects = new JSONObject[numWindowSizes][];
      for (int windowSize = maxWindow; windowSize >= minWindow; windowSize--){
        int wIndex = windowSize - minWindow;

        // extract phrases
        String[] phrases = Utils.extractPhrases(tokens, windowSize);
        pweightObjects[wIndex] = new JSONObject[phrases.length];

        // find translations for each phrase.
        for (int start = 0; start < phrases.length; start++) {
          // retrieve phrase translation, if exists
          String phrase = phrases[start];
          JSONArray pweights = phraseGenerator.getPhraseTranslations(phrase);
          if (pweights == null)   { continue; }    // there is no translation   
                 
          // H4 = cover: if we translate a longer phrase that contains token t, we mark t as covered and don't add shorter phrase representations
          boolean ignore = false;
          if (H4 == COVER){
            // go over each token in phrase and check if it has been covered before
            for (int cur = start; cur <= start+windowSize; cur++) {  
              // if already covered, do not add this phrase to list AND delegate coverage information down one level
              // if window size is max, we are at top-level so no filtering
              if (windowSize < maxWindow && isCovered[wIndex+1][cur]) {
                ignore = true;
//                LOG.info("already covered "+wIndex+","+cur); 
              }
              if (windowSize > 0) {     // if phrase has single term, no need to mark as covered
                LOG.info("covered "+wIndex+","+cur); 
                isCovered[wIndex][cur] = true;
              }
            }
          }
          if (!ignore) {
            JSONObject phraseTrans = new JSONObject();
            phraseTrans.put("#pweight", pweights);
            pweightObjects[wIndex][start] = phraseTrans;
          }
        }
      }
      
      // word
      JSONObject[] weightObjects = new JSONObject[length];
      for (int start = 0; start < length; start++) {
//        // we want to check coverage from phrases with length > 0 (not single term)
//        if(minWindow == 0){
//          if (isCovered[1][start])  continue;          
//        }else {
//          if (isCovered[0][start])  continue;          
//        }
        // create a #weight JSonObject
        String token = tokens[start];
//        LOG.info("Token "+token+" at "+start+" not covered by pweight");
        JSONArray weights = clGenerator.getTranslations(token);
        if (weights != null) {        
          JSONObject wordTrans = new JSONObject();
          wordTrans.put("#weight", weights);
          weightObjects[start] = wordTrans;
        }else {
          // token doesn't appear in vocab
          //LOG.info("Skipped "+token);
        }
      } 

      // represent each token with an array of #pweight and #weight objects = tokenArr
      // represent query by #combine of token representations = queryArr
      JSONArray queryArr = new JSONArray();
      for (int i = 0; i < length; i++) {
        if (H4 == COMBINE || H4 == COVER) {
          for (int windowSize = maxWindow; windowSize >= minWindow; windowSize--){
            int wIndex = windowSize - minWindow;
            if(i < pweightObjects[wIndex].length && pweightObjects[wIndex][i] != null){
              queryArr.put(pweightObjects[wIndex][i]);
            }
          }   
          if(weightObjects[i] != null){
            queryArr.put(weightObjects[i]);
          }
        }else if (H4 == PERTOKEN) {
          //LOG.info("Token # "+ i);
          JSONArray tokenArr = new JSONArray();
          // add phrases of all windowSizes
          for (int windowSize = maxWindow; windowSize >= minWindow; windowSize--){
            int wIndex = windowSize - minWindow;
            // add phrases starting at token i, i-1, ..., i-windowSize b/c all of those phrases contain token i
            for (int k = 0; k <= windowSize; k++){
              if((i - k) >= 0 && (i-k) < pweightObjects[wIndex].length && pweightObjects[wIndex][i-k] != null){
                tokenArr.put(pweightObjects[wIndex][i-k]);
              }
            }   
          }

          if(weightObjects[i] != null){
            tokenArr.put(weightObjects[i]);
          }     

          int numObjects = tokenArr.length();
          if (numObjects  > 0) {
            // representation of each token is an array of corr. pweight and weight JSonObjects
            JSONObject wordAndPhraseTrans = new JSONObject();

            for(int j = numObjects-1; j >= 0; j--){						
              tokenArr.put(2*j+1, tokenArr.get(j));
              tokenArr.put(2*j, 1.0f/numObjects);				// H4 = uniform weighting
            }
            wordAndPhraseTrans.put("#weight", tokenArr);
            queryArr.put(wordAndPhraseTrans);

            //LOG.info("combined = " + wordAndPhraseTrans.toString(0));
          }
        }
      }

      queryJson.put("#combine", queryArr);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return queryJson;
  }

  public int getQueryLength(){
    return length;  
  }

}
