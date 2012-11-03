package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.PairOfFloatMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.tartarus.snowball.SnowballStemmer;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKF.Entry;

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
  Tokenizer docLangTokenizer, queryLangTokenizerWithStemming, queryLangTokenizer;
  SnowballStemmer stemmer;
  int length;
  private int kBest;
  boolean bigramSegment = false;
  private CLWordQueryGenerator clGenerator;
  private SCFGQueryGenerator scfgGenerator;
  private float mtWeight, bitextWeight,scfgWeight, tokenWeight, phraseWeight, alpha, lexProbThreshold;
  private String queryLang, docLang;
  private boolean scaling;

  public MtNQueryGenerator() {
    super();
  }

  @Override
  public void init(Configuration conf) throws IOException {
    mtWeight = conf.getFloat(Constants.MTWeight, 1f);
    bitextWeight = conf.getFloat(Constants.BitextWeight, 0f);
    scfgWeight = conf.getFloat(Constants.SCFGWeight, 0f);
    LOG.info(conf.get(Constants.MTWeight));
    LOG.info(conf.get(Constants.BitextWeight));
    LOG.info(conf.get(Constants.SCFGWeight));
  }

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

    init(conf);
    queryLangTokenizer = TokenizerFactory.createTokenizer(queryLang, queryTokenizerPath, false, null, null, null);
    queryLangTokenizerWithStemming = TokenizerFactory.createTokenizer(queryLang, queryTokenizerPath, true, null, conf.get(Constants.StemmedStopwordListQ), null);
    docLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, docLang, docTokenizerPath, true, null, conf.get(Constants.StemmedStopwordListD), null);

    clGenerator = new CLWordQueryGenerator();
    clGenerator.init(fs, conf);

    scfgGenerator = new SCFGQueryGenerator();
    scfgGenerator.init(fs, conf);
  }

  public JSONObject parseQuery(String query){

    JSONObject queryJson = new JSONObject();
    JSONObject queryTJson = new JSONObject();
    JSONObject queryPJson = new JSONObject();
    try {
      List<String> tokensBOW = new ArrayList<String>(), tokensBOP = new ArrayList<String>();
      Map<String, HMapSFW> src2trg2weight = new HashMap<String, HMapSFW>();
      Map<String,String> target2source = new HashMap<String,String>(); 
      HMapSFW phrase2weight = new HMapSFW();

      String[] kbestTranslations = query.trim().split("\\|\\|\\|\\|");
      String origQuery = kbestTranslations[0].split(";")[2].trim(); 

      // if no weighting, delegate to appropriate generator class
      if (mtWeight == 0 && scfgWeight == 0 && bitextWeight == 1) {
        return clGenerator.parseQuery(";"+origQuery);
      }

      String[] stemmedSourceTokens = queryLangTokenizerWithStemming.processContent(origQuery);
      Map<String,String> stemmed2Stemmed = Utils.getStemMapping(origQuery, queryLangTokenizer, queryLangTokenizerWithStemming, docLangTokenizer);

      // if k is 1, we assume standard space-delimited query format
      // otherwise, query format consists of k "weight ||| translation" pairs, each separated by |||
      if (kBest == 1){
        String[] line = kbestTranslations[0].trim().split(";");
        String[] rules = line[0].trim().split("\\|\\|\\|");

        for (String rule : rules) {
          rule = rule.trim();
          String[] ruleArr = rule.split("::");
          String phrasePair = ruleArr[2];
          String trgPhrase = phrasePair.split("\\|")[1]; 
          float ruleProb = Float.parseFloat(ruleArr[0]);

          // include entire RHS in the alternative phrase-based representation (even if it's not a multiword expr)

          // heuristic: remove stop words from the RHS except for the ones between content words 
          // (e.g., tremblement de terre, means earthquake)
          trgPhrase = docLangTokenizer.removeBorderStopWords(trgPhrase);
          tokensBOP.add(trgPhrase);

          // add target tokens
          String[] tokenPairs= ruleArr[1].split("\\|\\|");
          for (String tokenPair : tokenPairs) {
            // token consists of prob|source|target
            String[] arr = tokenPair.split("\\|");
            String source = arr[0];
            String target = arr[1];
            // Optional stem normalization
            // -1 is a special marker for Pass-through rules, meaning that source is OOV
            // in this case, we want to restem the source using the doc lang tokenizer
            // e.g. Emmy --> emmi with English stemmer. If we translate emmi with pass-through rule, it becomes emmi in French
            // however, Emmy --> emmy with French stemmer, so this will cause us to miss all relevant documents
            if (ruleProb == -1) {         
              target = stemmed2Stemmed.get(target);
            }

            if (target != null && !queryLangTokenizerWithStemming.isStemmedStopWord(source) && !source.equals("NULL") && !docLangTokenizer.isStemmedStopWord(target)) {
              tokensBOW.add(target);
            }
          }
        }

        String[] bopArr = new String[tokensBOP.size()];
        bopArr = tokensBOP.toArray(bopArr);
        JSONArray bop = new JSONArray(bopArr);
        JSONObject bopJson = new JSONObject();
        bopJson.put("#combine", bop);

        String[] bowArr = new String[tokensBOW.size()];
        bowArr = tokensBOW.toArray(bowArr);
        JSONArray bow = new JSONArray(bowArr);
        JSONObject bowJson = new JSONObject();
        bowJson.put("#combine", bow);

        JSONArray weightedQuery = new JSONArray();
        weightedQuery.put(tokenWeight);
        weightedQuery.put(bowJson);
        weightedQuery.put(phraseWeight);
        weightedQuery.put(bopJson);
        queryJson.put("#weight", weightedQuery);
      }else {     // k > 1

        // apply discount on logprobs to avoid floating point errors
        float discount = 0;
        String[] line = kbestTranslations[0].trim().split(";");
        discount = -Float.parseFloat(line[1]);

        float[] transProbs = new float[kbestTranslations.length];
        float sumOfProbs = 0;
        for (int k = 0; k < kbestTranslations.length; k++){
          line = kbestTranslations[k].trim().split(";");
          transProbs[k] = (float) Math.pow(Math.E, alpha * (Float.parseFloat(line[1]) + discount));
          sumOfProbs += transProbs[k];
        }

        // parse each of the k top translations
        float cumPhraseProbs = 0;
        HMapKI<String> tokenCount = new HMapKI<String>();
        for (int k = 0; k < kbestTranslations.length; k++){
          // init
          target2source.clear();

          // parse input from cdec
          line = kbestTranslations[k].trim().split(";");

          // normalize prob. of k-th translation
          float transProb = transProbs[k]/sumOfProbs;

          //          String text = line[2].replaceAll("\\s+", "");
          String[] rules = line[0].trim().split("\\|\\|\\|");

          for (String rule : rules) {
            String[] ruleArr = rule.split("::");
            String[] tokenPairs = ruleArr[1].split("\\|\\|");
            String[] phraseArr = ruleArr[2].split("\\|");
            //            String sourcePhrase = phraseArr[0];
            String targetPhrase = phraseArr[1].trim();
            float ruleProb = Float.parseFloat(ruleArr[0]);
            for (String tokenPair : tokenPairs) {
              // token consists of prob|source|target
              String[] arr2 = tokenPair.split("\\|");
              String source = arr2[0];
              String target = arr2[1];

              // Optional stem normalization
              if (ruleProb == -1) {
                target = stemmed2Stemmed.get(target);
              }

              //              LOG.info("assign:{"+source+"}->["+target+"]="+transProb);
              if (target == null || queryLangTokenizerWithStemming.isStemmedStopWord(source) || source.equals("NULL") || docLangTokenizer.isStemmedStopWord(target)) {
                continue;
              }

              // if a source token is aligned to multiple target tokens, 
              // treat each alignment as a separate possible translation w/ same probability
              tokenCount.increment(source);
              if (src2trg2weight.containsKey(source)) {
                if(src2trg2weight.get(source).containsKey(target)) {
                  src2trg2weight.get(source).increment(target, transProb);
                }else {
                  src2trg2weight.get(source).put(target, transProb);
                }
              }else {
                HMapSFW trg2weight = new HMapSFW();
                trg2weight.put(target, transProb);
                src2trg2weight.put(source, trg2weight);
              }
            }
            if (targetPhrase.split(" ").length > 1) {
              phrase2weight.increment(targetPhrase, transProb);
              cumPhraseProbs += transProb;
            }
          }
        }

        // add phrase translations into a #weight array structure
        if (phraseWeight > 0) {
          JSONArray pArr = Utils.probMap2JSON(Utils.scaleProbMap(lexProbThreshold, 1/cumPhraseProbs, phrase2weight));
          queryPJson.put("#weight", pArr);
        }

        // add token translations into a #combine of #weight array structures
        JSONArray tokensArr = new JSONArray();
        if (tokenWeight > 0) {
          for (String srcToken : stemmedSourceTokens) {
            HMapSFW nbestDist = src2trg2weight.get(srcToken);

            // skip stop words among source query words
            if (queryLangTokenizerWithStemming.isStemmedStopWord(srcToken)){
              LOG.info("Skipped stopword "+srcToken);
              continue;
            }
            JSONObject tokenWeightedArr = new JSONObject();
            LOG.info("Processing "+srcToken);

            // skip stop words among source query words
            if (nbestDist == null){
              LOG.info("Unaligned in MT: "+srcToken);
            }else {
              // normalize probabilities for this token
              float normalization = 0;
              for (Entry<String> e : nbestDist.entrySet()) {
                float weight = e.getValue();
                normalization += weight;
              }
              for (Entry<String> e : nbestDist.entrySet()) {
                nbestDist.put(e.getKey(), e.getValue()/normalization);
              }              
            }

            // combine translations from N-best AND bilingual dictionary
            List<PairOfFloatMap> tokenRepresentationList = new ArrayList<PairOfFloatMap>();

            // Pr{bitext}
            HMapSFW bitextDist = clGenerator.getTranslations(srcToken, stemmed2Stemmed);
            //          LOG.info("bitext: "+bitextDist+"\n"+bitextWeight);
            if (bitextDist != null && !bitextDist.isEmpty() && bitextWeight > 0) {
              tokenRepresentationList.add(new PairOfFloatMap(bitextDist, bitextWeight));
            }

            // Pr{scfg}
            HMapSFW scfgDist = scfgGenerator.getTranslations(srcToken, stemmed2Stemmed);
            //          LOG.info("scfg: "+scfgDist+"\n"+scfgWeight);
            if (scfgDist != null && !scfgDist.isEmpty() && scfgWeight > 0) {
              tokenRepresentationList.add(new PairOfFloatMap(scfgDist, scfgWeight));
            }

            // Pr{n-best}
            if (mtWeight > 0 && nbestDist != null && !nbestDist.isEmpty()) {
              tokenRepresentationList.add(new PairOfFloatMap(nbestDist, mtWeight));
            }

            JSONArray combinedArr;
            float scale = 1;
            if (scaling) {
              scale = scale * tokenCount.get(srcToken)/((float) kbestTranslations.length);
            }
            if(tokenRepresentationList.size() == 0) {
              continue;       // if empty distr., do not represent this source token in query
            } else if(tokenRepresentationList.size() == 1) {
              combinedArr = Utils.probMap2JSON(Utils.scaleProbMap(lexProbThreshold, scale, tokenRepresentationList.get(0).getMap()));
            } else {
              combinedArr = Utils.probMap2JSON(Utils.combineProbMaps(lexProbThreshold, scale, tokenRepresentationList));
            }

            tokenWeightedArr.put("#weight", combinedArr);

            // optional: if this source token has occurred more than once per query, reflect this in the representation
            //  for (int i = 0; i < Math.ceil(tokenCount.get(srcToken)/(float)kBest); i++) {
            //    tokensArr.put(tokenWeightedArr);
            //  }
            tokensArr.put(tokenWeightedArr);
          }
          queryTJson.put("#combine", tokensArr);
        }

        // combine the token-based and phrase-based representations into a #combweight structure
        JSONArray queryJsonArr = new JSONArray();

        HMapSFW normalizedPhrase2Weight = null;
        if (phraseWeight > 0) {
          normalizedPhrase2Weight = Utils.scaleProbMap(lexProbThreshold, phraseWeight/cumPhraseProbs, phrase2weight);      
          for (String phrase : normalizedPhrase2Weight.keySet()) {
            queryJsonArr.put(normalizedPhrase2Weight.get(phrase));
            queryJsonArr.put(phrase);
          }
        }
        if (tokenWeight > 0) {
          queryJsonArr.put(tokenWeight);
          queryJsonArr.put(queryTJson);
        }
        queryJson.put("#combweight", queryJsonArr);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return queryJson;
  }

  public int getQueryLength(){
    return length;  
  }

}
