package ivory.sqe.querygenerator;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.tokenize.BigramChineseTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.StructuredQuery;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import edu.umd.cloud9.io.map.HMapSFW;

/**
 * In addition to PSQ implemented in CLWordQueryGenerator, phrase translations are learned from a provided Synchronous context-free grammar (SCFG) (Hiero, 2006) and added to the representation.
 * 
 * @author ferhanture
 *
 */
public class SCFGQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(SCFGQueryGenerator.class);
  private Tokenizer queryLangTokenizerWithStemming, docLangTokenizer, queryLangTokenizer, bigramTokenizer;
  private Map<String, Map<String, HMapSFW>> query2probMap;
  private int length, numTransPerToken;
  private boolean bigramSegment;
  private RetrievalEnvironment env;
  private String queryLang, docLang;
//  private List<String> originalQueries; 
  private float lexProbThreshold, cumProbThreshold;
  public SCFGQueryGenerator() throws IOException {
    super();
  }

  @Override
  public void init(FileSystem fs, Configuration conf) throws IOException {
    LOG.info(conf.get(Constants.DocLanguage));
    LOG.info(conf.get(Constants.DocTokenizerData));
    LOG.info(conf.get(Constants.MinWindow));
    LOG.info(conf.get(Constants.MaxWindow));
    LOG.info(conf.get(Constants.SCFGWeight));

    LOG.info("Stemmed stopword list file in query-language:" + conf.get(Constants.StemmedStopwordListQ));
    LOG.info("Stemmed stopword list file in doc-language:" + conf.get(Constants.StemmedStopwordListD));

    numTransPerToken = conf.getInt(Constants.NumTransPerToken, Integer.MAX_VALUE);

    queryLang = conf.get(Constants.QueryLanguage);
    docLang = conf.get(Constants.DocLanguage);

    lexProbThreshold = conf.getFloat(Constants.LexicalProbThreshold, 0f);
    cumProbThreshold = conf.getFloat(Constants.CumulativeProbThreshold, 1f);

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

    queryLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, queryLang, conf.get(Constants.QueryTokenizerData), false, null, null, null);
    queryLangTokenizerWithStemming = TokenizerFactory.createTokenizer(fs, conf, queryLang, conf.get(Constants.QueryTokenizerData), true, null, conf.get(Constants.StemmedStopwordListQ), null);
    docLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, docLang, conf.get(Constants.DocTokenizerData), true, null, conf.get(Constants.StemmedStopwordListD), null);

    // read original queries from file
//    originalQueries = Utils.readOriginalQueries(fs, conf.get(Constants.OriginalQueriesPath));
  }

  @Override
  public StructuredQuery parseQuery(String query, FileSystem fs, Configuration conf) {   
    JsonObject queryJson = new JsonObject();
   
    String origQuery = query.trim().split("\\|\\|\\|\\|")[0].trim();
    String grammarFile = conf.get(Constants.GrammarPath);
    Map<String, HMapSFW> probMap = processGrammar(fs, conf, grammarFile);
    
    Map<String, String> stemmed2Stemmed = Utils.getStemMapping(origQuery, queryLangTokenizer,
        queryLangTokenizerWithStemming, docLangTokenizer);

    JsonArray tokenTranslations = new JsonArray();
    String[] tokens = queryLangTokenizerWithStemming.processContent(origQuery);
    length = tokens.length;
    for (String token : tokens) {
      if (queryLangTokenizerWithStemming.isStopWord(token)){
        continue;
      }
      // if we do bigram segmentation on translation alternatives, we have to a have a weight
      // structure, even if k=1
      // unless the token has less than 3 characters, in which bigram segmentation will only return
      // the token itself
      if (numTransPerToken == 1 && !bigramSegment) {
        String trans = getBestTranslation(origQuery, token);
        if (trans != null) {
          tokenTranslations.add(new JsonPrimitive(trans));
        }
      } else {
        JsonObject tokenTrans = new JsonObject();
        JsonArray weights = Utils.createJsonArrayFromProbabilities(getTranslations(origQuery, token, probMap, stemmed2Stemmed));
        if (weights != null) {
          tokenTrans.add("#weight", weights);
          tokenTranslations.add(tokenTrans);
        }
      }
    }
    queryJson.add("#combine", tokenTranslations);

    return new StructuredQuery(queryJson, length);
  }

  public Map<String, HMapSFW> processGrammar(FileSystem fs, Configuration conf, String grammarFile) {
    Map<String, HMapSFW> probMap = Utils.generateTranslationTable(fs, conf, grammarFile, 
        queryLangTokenizerWithStemming, docLangTokenizer);
    if (probMap == null) {
      LOG.info("No probabilities extracted from " + grammarFile);
    }else {
      //remove low-prob entries, cut off after certain cumulative prob., and normalize
      Utils.normalize(probMap, lexProbThreshold, cumProbThreshold, 30);      
    }   
    return probMap;
  }

  private String getBestTranslation(String query, String token) {
    HMapSFW probDist = query2probMap.get(query).get(token);

    if(probDist == null){
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

  protected HMapSFW getTranslations(String query, String token, Map<String, HMapSFW> probMap, Map<String, String> stemmed2Stemmed) {
    HMapSFW probDist = null;
    try {
      probDist = probMap.get(token);
    } catch (NullPointerException e) {
      LOG.info("Prob map not found for " + query);
      e.printStackTrace();
    }
    
    if(probDist == null){
      // borrow OOV word heuristic from MT: if no translation found, include itself as translation
      probDist = new HMapSFW();
      String targetStem = stemmed2Stemmed.get(token);
      String target = (stemmed2Stemmed == null || targetStem == null) ? token : stemmed2Stemmed.get(token);
      probDist.put(target, 1);      
      return probDist;
    }

    return probDist;
  }
}
