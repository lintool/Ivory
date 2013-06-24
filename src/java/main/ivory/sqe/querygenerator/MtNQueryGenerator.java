package ivory.sqe.querygenerator;

import ivory.core.tokenize.SnowballStemmer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.PairOfFloatMap;
import ivory.sqe.retrieval.StructuredQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfStrings;


/**
 * A different way of formulating the N-best translation output from cdec.
 * It is in format {#combine { #weight ... } ... } where each #weight 
 * corresponds to a single source token representation.  
 * 
 * Retrieval engine computes a TF and DF value for each source token, 
 * based on the alternative translations used in the N-best list
 * 
 * @author ferhanture
 *
 */
public class MtNQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(MtNQueryGenerator.class);
  private Tokenizer docLangTokenizer, queryLangTokenizerWithStemming, queryLangTokenizer;
  private int length;
  private int kBest;
  private boolean bigramSegment = false;
  private ProbabilisticStructuredQueryGenerator clGenerator;
  private SCFGQueryGenerator scfgGenerator;
  private float mtWeight, bitextWeight,scfgWeight, tokenWeight, phraseWeight, alpha, lexProbThreshold;
  private String queryLang, docLang;
  private boolean scaling;
  private Set<String> unknownWords;

  public MtNQueryGenerator() {
    super();
  }

  @Override
  public void init(FileSystem fs, Configuration conf) throws IOException {
    if (conf.getBoolean(Constants.Quiet, false)) {
      LOG.setLevel(Level.OFF);
    }
    queryLang = conf.get(Constants.QueryLanguage);
    docLang = conf.get(Constants.DocLanguage);

    LOG.info("Stemmed stopword list file in query-language:" + conf.get(Constants.StemmedStopwordListQ));
    LOG.info("Stemmed stopword list file in doc-language:" + conf.get(Constants.StemmedStopwordListD));

    tokenWeight = conf.getFloat(Constants.TokenWeight, 1f);
    phraseWeight = conf.getFloat(Constants.PhraseWeight, 0f);
    alpha = conf.getFloat(Constants.Alpha, 1);
    scaling = conf.getBoolean(Constants.Scaling, false);
    lexProbThreshold = conf.getFloat(Constants.LexicalProbThreshold, 0f);

    String queryTokenizerPath = conf.get(Constants.QueryTokenizerData);
    String docTokenizerPath = conf.get(Constants.DocTokenizerData);
    kBest = conf.getInt(Constants.KBest, 1); 
    LOG.info("K = " + kBest);

    mtWeight = conf.getFloat(Constants.MTWeight, 1f);
    bitextWeight = conf.getFloat(Constants.BitextWeight, 0f);
    scfgWeight = conf.getFloat(Constants.GrammarWeight, 0f);
    LOG.info(conf.get(Constants.MTWeight));
    LOG.info(conf.get(Constants.BitextWeight));
    LOG.info(conf.get(Constants.GrammarWeight));

    queryLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, queryLang, queryTokenizerPath, false, null, null, null);
    queryLangTokenizerWithStemming = TokenizerFactory.createTokenizer(fs, conf, queryLang, queryTokenizerPath, true, null, conf.get(Constants.StemmedStopwordListQ), null);
    docLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, docLang, docTokenizerPath, true, null, conf.get(Constants.StemmedStopwordListD), null);

    unknownWords = Utils.readUnknowns(fs, conf.get(Constants.UNKFile));
    LOG.info("Unknown words = " + unknownWords);

    LOG.info("one2many= " + conf.getInt(Constants.One2Many, 2));

    if (clGenerator == null) {
      clGenerator = new ProbabilisticStructuredQueryGenerator();
      clGenerator.init(fs, conf);
    }
    if (scfgGenerator == null) {
      scfgGenerator = new SCFGQueryGenerator();
      scfgGenerator.init(fs, conf);
    }
  }

  @Override
  public StructuredQuery parseQuery(String query, FileSystem fs, Configuration conf) {   
    JsonObject queryJson = new JsonObject();
    JsonObject queryTJson = new JsonObject();
    JsonObject queryPJson = new JsonObject();

    List<String> tokensBOW = new ArrayList<String>(), tokensBOP = new ArrayList<String>();

    Translation translation = TranslationFactory.readTranslationsFromNBest(query, alpha, 
        unknownWords, queryLangTokenizer, queryLangTokenizerWithStemming, docLangTokenizer, conf);

    String origQuery = translation.getOriginalQuery();
    String grammarFile = conf.get(Constants.GrammarPath);
    Map<String, HMapSFW> probMap = null;
    if (scfgWeight > 0) {
      probMap = scfgGenerator.processGrammar(fs, conf, grammarFile);
    }
    Set<PairOfStrings> pairsInGrammar = null;
    if (bitextWeight > 0) {
      pairsInGrammar = clGenerator.processGrammar(fs, conf, grammarFile);
    }
    // if no weighting, delegate to appropriate generator class
    if (mtWeight == 0 && scfgWeight == 0 && bitextWeight == 1) {
      return clGenerator.parseQuery(origQuery + "||||", fs, conf);
    }

    // create a mapping from {source token stemmed with query language tokenizer} to {source token stemmed with doc language tokenizer}
    // if decoder uses a pass-through rule and leave a token as it is, we use this mapping to re-stem the token wrt doc language vocab
    String[] stemmedSourceTokens = queryLangTokenizerWithStemming.processContent(origQuery);
    Map<String,String> stemmed2Stemmed = translation.getStemMapping();

    // if k is 1, we assume standard space-delimited query format
    // otherwise, query format consists of k "weight ||| translation" pairs, each separated by |||
    if (kBest == 1){
      if (phraseWeight > 0) {
        Set<String> targetPhrases = translation.getPhraseDist().keySet();
        for (String targetPhrase : targetPhrases) {
          // heuristic: remove stop words from the RHS except for the ones between content words 
          // (e.g., tremblement de terre ~ earthquake)
          targetPhrase = Utils.removeBorderStopWords(docLangTokenizer, targetPhrase);
          tokensBOP.add(targetPhrase);
        }
      }

      Set<String> targetTokens = translation.getTargetTokens();
      for (String target : targetTokens) {
        tokensBOW.add(target);
      }

      String[] bopArr = new String[tokensBOP.size()];
      JsonObject bopJson = new JsonObject();
      bopJson.add("#combine", Utils.createJsonArray(tokensBOP.toArray(bopArr)));

      String[] bowArr = new String[tokensBOW.size()];
      JsonObject bowJson = new JsonObject();
      bowJson.add("#combine", Utils.createJsonArray(tokensBOW.toArray(bowArr)));

      JsonArray weightedQuery = new JsonArray();
      weightedQuery.add(new JsonPrimitive(tokenWeight));
      weightedQuery.add(bowJson);
      weightedQuery.add(new JsonPrimitive(phraseWeight));
      weightedQuery.add(bopJson);
      queryJson.add("#weight", weightedQuery);
    }else {     // k > 1
      // add phrase translations into a #weight array structure
      if (phraseWeight > 0) {
        JsonArray pArr = Utils.createJsonArrayFromProbabilities(translation.getPhraseDist());
        queryPJson.add("#weight", pArr);
      }

      // add token translations into a #combine of #weight array structures
      JsonArray tokensArr = new JsonArray();
      if (tokenWeight > 0) {
        for (String srcToken : stemmedSourceTokens) {
          HMapSFW nbestDist = translation.getDistributionOf(srcToken);

          if (queryLangTokenizerWithStemming.isStopWord(srcToken)){
            continue;
          }
          LOG.info("Processing "+srcToken);

          // combine translations from N-best AND bilingual dictionary
          List<PairOfFloatMap> tokenRepresentationList = new ArrayList<PairOfFloatMap>();

          // Pr{bitext}
          if (bitextWeight > 0) {
            HMapSFW bitextDist = clGenerator.getTranslations(origQuery.trim(), srcToken, pairsInGrammar, stemmed2Stemmed);
            if(bitextDist != null && !bitextDist.isEmpty()){
              tokenRepresentationList.add(new PairOfFloatMap(bitextDist, bitextWeight));
            }
          }

          // Pr{scfg}
          if (scfgWeight > 0) {
            HMapSFW scfgDist = scfgGenerator.getTranslations(origQuery.trim(), srcToken, probMap, stemmed2Stemmed);
            if (scfgDist != null && !scfgDist.isEmpty() ){
              tokenRepresentationList.add(new PairOfFloatMap(scfgDist, scfgWeight));
            }
          }

          // Pr{n-best}
          if (mtWeight > 0 && nbestDist != null && !nbestDist.isEmpty()) {
            Utils.normalize(nbestDist);
            tokenRepresentationList.add(new PairOfFloatMap(nbestDist, mtWeight));
          }

          JsonArray combinedArr;
          float scale = 1;
          if (scaling) {
            scale = scale * translation.getSourceTokenCnt().get(srcToken) / ((float)translation.getCount());
          }
          if(tokenRepresentationList.size() == 0) {
            continue;       // if empty distr., do not represent this source token in query
          } else if(tokenRepresentationList.size() == 1) {
            combinedArr = Utils.createJsonArrayFromProbabilities(Utils.scaleProbMap(lexProbThreshold, scale, tokenRepresentationList.get(0).getMap()));
          } else {
            combinedArr = Utils.createJsonArrayFromProbabilities(Utils.combineProbMaps(lexProbThreshold, scale, tokenRepresentationList));
          }

          JsonObject tokenWeightedArr = new JsonObject();          
          tokenWeightedArr.add("#weight", combinedArr);

          // optional: if this source token has occurred more than once per query, reflect this in the representation
          //  for (int i = 0; i < Math.ceil(tokenCount.get(srcToken)/(float)kBest); i++) {
          //    tokensArr.put(tokenWeightedArr);
          //  }
          tokensArr.add(tokenWeightedArr);
        }
        queryTJson.add("#combine", tokensArr);
      }

      // combine the token-based and phrase-based representations into a #combweight structure
      JsonArray queryJsonArr = new JsonArray();

      HMapSFW scaledPhrase2Weight = null;
      if (phraseWeight > 0) {
        scaledPhrase2Weight = Utils.scaleProbMap(lexProbThreshold, phraseWeight, translation.getPhraseDist());      
        for (String phrase : scaledPhrase2Weight.keySet()) {
          queryJsonArr.add(new JsonPrimitive(scaledPhrase2Weight.get(phrase)));
          queryJsonArr.add(new JsonPrimitive(phrase));
        }
      }
      if (tokenWeight > 0) {
        queryJsonArr.add(new JsonPrimitive(tokenWeight));
        queryJsonArr.add(queryTJson);
      }
      queryJson.add("#combweight", queryJsonArr);
    }

    return new StructuredQuery(queryJson, length);
  }
}
