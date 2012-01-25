package ivory.sqe.querygenerator;

import ivory.core.tokenize.BigramChineseTokenizer;
import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.OpenNLPTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class MTOutputQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(CLWordQueryGenerator.class);
  Tokenizer tokenizer, fTokenizer;
  SnowballStemmer stemmer;
  int length;
  private VocabularyWritable fVocab_f2e, eVocab_f2e;
  private TTable_monolithic_IFAs f2eProbs;

  private static final int NONE = 0, ADD = 1, REPLACE= 2;
  private int H5, kBest;

  public MTOutputQueryGenerator() {
    super();
  }

  public void init(FileSystem fs, Configuration conf) throws IOException {
    LOG.info(conf.get(Constants.Heuristic5));
    LOG.info(conf.get(Constants.KBest));
    LOG.info(conf.get(Constants.Language));
    LOG.info(conf.get(Constants.TokenizerData));

    String lang = conf.get(Constants.Language);
    String tokenizerPath = conf.get(Constants.TokenizerData);
    if (lang.equals(Constants.English)) {
      if (!fs.exists(new Path(tokenizerPath))) {
        LOG.info("Tokenizer path "+tokenizerPath+" doesn't exist -- using GalagoTokenizer");
        tokenizer = new GalagoTokenizer();    
      }else {
        tokenizer = TokenizerFactory.createTokenizer(lang, tokenizerPath, null);
      }
    }else if (lang.equals(Constants.German)) {
      tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.Language), conf.get(Constants.TokenizerData), null);
    }else if (lang.equals(Constants.Chinese)) {
      if (!fs.exists(new Path(tokenizerPath))) {
        tokenizer = new BigramChineseTokenizer();
      }else {
        tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.Language), conf.get(Constants.TokenizerData), null);
      }
    }else {
      throw new RuntimeException("Language code "+lang+ " not known");
    }

    String h5 = conf.get(Constants.Heuristic5); 
    if (h5.equals("none")) {
      H5 = NONE;
    }else if (h5.equals("add")) {
      H5 = ADD;
    }else if (h5.equals("replace")) {
      H5 = REPLACE;
    }else {
      LOG.info("Using default value for heuristic H5 = NONE");
      H5 = NONE;    // default
    }
    LOG.info("H5 = " + H5);

    kBest = conf.getInt(Constants.KBest, 1); 
    LOG.info("K = " + kBest);

    if (H5 > NONE) {      // H5 assumes that the source language is English. i.e., (English --> other-language) IR
      fVocab_f2e = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.fVocabPath)), fs);
      eVocab_f2e = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.eVocabPath)), fs);
      f2eProbs = new TTable_monolithic_IFAs(fs, new Path(conf.get(Constants.f2eProbsPath)), true);
      stemmer = new englishStemmer();
      fTokenizer = new OpenNLPTokenizer(); 
    }

  }

  public JSONObject parseQuery(String query){
    JSONObject queryJson = new JSONObject();
    try {
      // if k is 1, we assume standard space-delimited query format
      // otherwise, query format consists of k "weight ||| translation" pairs, each separated by |||
      if (kBest == 1){
        String[] tokens = tokenizer.processContent(query.trim());
        List<String> finalTokens = applyHeuristic(tokens);    // if H5 is off, there will be no change in tokens list
        
        String[] arr = new String[finalTokens.size()];
        queryJson.put("#combine", new JSONArray(finalTokens.toArray(arr)));
      }else {
        String[] weightedKbestTranslations = query.trim().split("\\|\\|\\|");
        JSONArray weightedArr = new JSONArray();
        float sumProb = 0;
        for (int k = 0; k < 2 * kBest; k += 2){
          float weight = (float) Math.pow(Math.E, Float.parseFloat(weightedKbestTranslations[k]));

          String trans = weightedKbestTranslations[k+1];
          String[] tokens = tokenizer.processContent(trans.trim());
          List<String> finalTokens = applyHeuristic(tokens);    
          
          String[] arr = new String[finalTokens.size()];
          JSONObject transJson = new JSONObject();
          transJson.put("#combine", new JSONArray(finalTokens.toArray(arr)));
          weightedArr.put(weight);
          weightedArr.put(transJson);
          
          sumProb += weight;
        }

        // normalize weights
        for (int i = 0; i < weightedArr.length(); i=i+2){
          try {
            float pr = (float) weightedArr.getDouble(i);
            weightedArr.put(i, pr/sumProb);
          } catch (JSONException e1) {
            throw new RuntimeException("Error normalizing");
          }
        }
        queryJson.put("#weight", weightedArr);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return queryJson;
  }

  // based on H5 value, replace OOV words with best word-by-word translation
  private List<String> applyHeuristic(String[] tokens) {
    List<String> finalTokens;

    // if H5 is off, there will be no change in tokens list
    if (H5 == NONE) {
      finalTokens = Arrays.asList(tokens);
      length = tokens.length;
      return finalTokens;
    }
    finalTokens = new ArrayList<String>();

    for (String token : tokens) {
      // discard <s> , </s>, punctuation etc.
      if (token.equals("<s>") || token.equals("</s>") || fTokenizer.isStopWord(token))  continue;

      // only H5 => process token so it is compatible with vocabulary
      stemmer.setCurrent(token);
      stemmer.stem();
      String stemmed = stemmer.getCurrent().toLowerCase();

      // only H5 => check if token is in English vocabulary
      int f = 0;   
      f = fVocab_f2e.get(stemmed);
      if (f > 0) {
        LOG.info("English token = "+stemmed);

        // H5: for each English term, add its best Chinese translation
        PriorityQueue<PairOfFloatInt> eS = f2eProbs.get(f).getTranslationsWithProbs(0.0f);
        if (!eS.isEmpty()) {
          PairOfFloatInt entry = eS.poll();
          int e = entry.getRightElement();
          String eTerm = eVocab_f2e.get(e);
          finalTokens.add(eTerm);
          length++;
        }
      }

      // if H5 is deactive OR H5 = add, then we include all tokens in translated query, no matter what.
      if (H5 != REPLACE) {
        LOG.info("Chinese token = "+token);
        finalTokens.add(token);
        length++;
      }
    }
    return finalTokens;
  }

  public int getQueryLength(){
    return length;  
  }
}
