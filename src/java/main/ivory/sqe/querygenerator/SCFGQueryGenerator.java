package ivory.sqe.querygenerator;

import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.core.tokenize.BigramChineseTokenizer;
import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umd.cloud9.io.map.HMapSFW;

/**
 * In addition to PSQ implemented in CLWordQueryGenerator, phrase translations are learned from a provided Synchronous context-free grammar (SCFG) (Hiero, 2006) and added to the representation.
 * 
 * @author ferhanture
 *
 */
public class SCFGQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(SCFGQueryGenerator.class);
  private Tokenizer queryLangTokenizer, docLangTokenizer, bigramTokenizer;
  private Map<String, HMapSFW > probMap;
  private int length, numTransPerToken;
  private boolean bigramSegment;
  private RetrievalEnvironment env;
  private String queryLang, docLang;

  public SCFGQueryGenerator() throws IOException {
    super();
  }

  @Override
  public void init(Configuration conf) throws IOException {

  }

  public void init(FileSystem fs, Configuration conf) throws IOException {
    LOG.info(conf.get(Constants.DocLanguage));
    LOG.info(conf.get(Constants.DocTokenizerData));
    LOG.info(conf.get(Constants.MinWindow));
    LOG.info(conf.get(Constants.MaxWindow));
    LOG.info(conf.get(Constants.SCFGWeight));

    numTransPerToken = conf.getInt(Constants.NumTransPerToken, Integer.MAX_VALUE);

    queryLang = conf.get(Constants.QueryLanguage);
    docLang = conf.get(Constants.DocLanguage);

    float lexProbThreshold = conf.getFloat(Constants.LexicalProbThreshold, 0f);
    float cumProbThreshold = conf.getFloat(Constants.CumulativeProbThreshold, 1f);

    String bigram = conf.get(Constants.BigramSegment);
    bigramSegment = (bigram != null && bigram.equals("on")) ? true : false;
    if (bigramSegment) {
      bigramTokenizer = new BigramChineseTokenizer();
    }
    LOG.info("Bigram segmentation = " + bigramSegment);

    // initialize environment to access index
    try {
      env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);
      env.initialize(true);
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }

    queryLangTokenizer = TokenizerFactory.createTokenizer(fs, conf.get(Constants.QueryLanguage), conf.get(Constants.QueryTokenizerData), false, false, null);
    conf.set("stopword", "/fs/clip-qa/ferhan/clir-experiments/stopword."+docLang);
    docLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, docLang, conf.get(Constants.DocTokenizerData), false, false, null);

    // construct prob. dist. from translation grammar (SCFG)
    probMap = Utils.generateTranslationTable(conf, docLangTokenizer);

    //remove low-prob entries, cut off after certain cumulative prob., and normalize
    Utils.normalize(probMap, lexProbThreshold, cumProbThreshold, 30);      
  }

  public JSONObject parseQuery(String query) {
    JSONObject queryJson = new JSONObject();
    try {
      String[] tokens = query.trim().split("\\s");

      length = tokens.length;
      JSONArray tokenTranslations = new JSONArray();
      for (String token : tokens) {

        //        LOG.info("Processing "+token);
        // if we do bigram segmentation on translation alternatives, we have to a have a weight structure, even if k=1
        // unless the token has less than 3 characters, in which bigram segmentation will only return the token itself
        if (numTransPerToken == 1 && !bigramSegment){
          String trans = getBestTranslation(token);
          if (trans != null) {
            tokenTranslations.put(trans);
          }
        }else {
          JSONObject tokenTrans = new JSONObject();
          JSONArray weights = Utils.probMap2JSON(getTranslations(token, null));
          if (weights != null) {        
            tokenTrans.put("#weight", weights);
            tokenTranslations.put(tokenTrans);
          }
        }
      }
      queryJson.put("#combine", tokenTranslations);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return queryJson;
  }

  private String getBestTranslation(String token) {
    HMapSFW probDist = probMap.get(token);

    if(probDist == null){
      //      LOG.info("OOV: "+token);

      // heuristic: if no translation found, include itself as only translation
      return token;
    }

    float maxProb = 0f;
    String maxProbTrans = null;
    for (edu.umd.cloud9.util.map.MapKF.Entry<String> entry : probDist.entrySet()) {
      if (entry.getValue() > maxProb) {
        maxProb = entry.getValue();
        maxProbTrans = entry.getKey();
      }
    }
    return maxProbTrans;
  }

  public int getQueryLength(){
    return length;  
  }

  protected HMapSFW getTranslations(String token, Map<String, String> stemmed2Stemmed) {
    HMapSFW probDist = probMap.get(token);
    //    LOG.info("Translations of "+token+"="+probDist);
    if(probDist == null){
      //      LOG.info("OOV: "+token);

      // heuristic: if no translation found, include itself as only translation
      probDist = new HMapSFW();
      String targetStem = stemmed2Stemmed.get(token);
      String target = (stemmed2Stemmed == null || targetStem == null) ? token : stemmed2Stemmed.get(token);
      probDist.put(target, 1);      
      return probDist;
    }

    // // support for bigram segmentation
    //    if (bigramSegment) {
    //      HMapSFW probDistBigram = new HMapSFW();
    //      for (String translation : probDist.keySet()) {
    //        float prob = probDist.get(translation);
    //
    //        translation = translation.replaceAll(" ", "");
    //        String[] eTokens = bigramTokenizer.processContent(translation);
    //        float splitProb = prob / eTokens.length;
    //        
    //        for (String eToken : eTokens) {
    //          if (env.getPostingsList(eToken) != null) {
    //            //              phraseTranslationsArr.put(splitProb);
    //            //              phraseTranslationsArr.put(eToken);
    //            probDistBigram.put(eToken, splitProb);
    //          }
    //        }
    //      }
    //      
    //      // replace distribution with modified one
    //      probDist = probDistBigram;
    //    }

    return probDist;

  }
}
