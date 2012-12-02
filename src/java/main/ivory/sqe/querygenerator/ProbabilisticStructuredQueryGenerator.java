package ivory.sqe.querygenerator;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.StructuredQuery;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

/**
 * Given a query in language F, will generate a Indri-like query representation, following Darwish's probabilistic structured queries (PSQ) technique (Darwish and Oard, 2003)
 * 
 * @author ferhanture
 *
 */
public class ProbabilisticStructuredQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(ProbabilisticStructuredQueryGenerator.class);
  private Tokenizer queryLangTokenizer, queryLangTokenizerWithStem, docLangTokenizer;
  private VocabularyWritable fVocab_f2e, eVocab_f2e;
  private TTable_monolithic_IFAs f2eProbs;
  private int length, numTransPerToken;
  private float lexProbThreshold, cumProbThreshold;
  private Set<PairOfStrings> pairsInSCFG;
  private boolean H6, bigramSegment;
  private RetrievalEnvironment env;
  private String queryLang, docLang;

  public ProbabilisticStructuredQueryGenerator() throws IOException {
    super();
  }

  @Override
  public void init(FileSystem fs, Configuration conf) throws IOException {
    if (conf.getBoolean(Constants.Quiet, false)) {
      LOG.setLevel(Level.OFF);
    }

    queryLang = conf.get(Constants.QueryLanguage);
    docLang = conf.get(Constants.DocLanguage);
    fVocab_f2e = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.QueryVocab)), fs);
    eVocab_f2e = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.DocVocab)), fs);
    f2eProbs = new TTable_monolithic_IFAs(fs, new Path(conf.get(Constants.f2eProbsPath)), true);

    LOG.info("Stemmed stopword list file in query-language:" + conf.get(Constants.StemmedStopwordListQ));
    LOG.info("Stemmed stopword list file in doc-language:" + conf.get(Constants.StemmedStopwordListD));

    queryLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, queryLang, conf.get(Constants.QueryTokenizerData), false, null, null, null);
    queryLangTokenizerWithStem = TokenizerFactory.createTokenizer(fs, conf, queryLang, conf.get(Constants.QueryTokenizerData), true, null, conf.get(Constants.StemmedStopwordListQ), null);
    docLangTokenizer = TokenizerFactory.createTokenizer(fs, conf, docLang, conf.get(Constants.DocTokenizerData), true, null, conf.get(Constants.StemmedStopwordListD), null);

    lexProbThreshold = conf.getFloat(Constants.LexicalProbThreshold, 0f);
    cumProbThreshold = conf.getFloat(Constants.CumulativeProbThreshold, 1f);
    numTransPerToken = conf.getInt(Constants.NumTransPerToken, Integer.MAX_VALUE);

    String h6 = conf.get(Constants.Heuristic6);
    if (h6 == null || h6.equals("off")) {
      H6 = false;
    }else {
      H6 = true;
    }
    LOG.info("H6 = " + H6);

    if (H6 && conf.get(Constants.SCFGPath) != null) {
      pairsInSCFG = new HashSet<PairOfStrings>();
      getPairsInSCFG(conf.get(Constants.SCFGPath));
    }

    // initialize environment to access index
    try {
      env = new RetrievalEnvironment(conf.get(Constants.IndexPath), fs);
      env.initialize(true);
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
  }

  @Override
  public StructuredQuery parseQuery(String query) {
    JsonObject queryJson = new JsonObject();

    String origQuery = query.trim().split(";")[1].trim();
    Map<String, String> stemmed2Stemmed = Utils.getStemMapping(origQuery, queryLangTokenizer,
        queryLangTokenizerWithStem, docLangTokenizer);

    String[] tokens = queryLangTokenizerWithStem.processContent(origQuery);

    length = tokens.length;
    JsonArray tokenTranslations = new JsonArray();
    for (String token : tokens) {
      LOG.info("Processing token " + token);
      if (queryLangTokenizerWithStem.isStemmedStopWord(token))
        continue;
      LOG.info("not stopword");

      // output is not a weighted structure iff numTransPerToken=1
      // and we're not doing bigram segmentation (which requires a weighted structure since it
      // splits a single token into multiple ones)
      if (numTransPerToken == 1 && !bigramSegment) {
        String trans = getBestTranslation(token);
        if (trans != null) {
          tokenTranslations.add(new JsonPrimitive(trans));
        }
      } else {
        JsonObject tokenTrans = new JsonObject();
        HMapSFW distr = getTranslations(token, stemmed2Stemmed);
        if (distr == null) {
          continue;
        }
        JsonArray weights = Utils.createJsonArrayFromProbabilities(distr);
        if (weights != null) {
          tokenTrans.add("#weight", weights);
          tokenTranslations.add(tokenTrans);
        }
      }
    }
    queryJson.add("#combine", tokenTranslations);
    return new StructuredQuery(queryJson, length);
  }

  protected String getBestTranslation(String token) {
    int f = fVocab_f2e.get(token);
    if (f <= 0) {
      // heuristic: if no translation found, include itself as only translation
      return null;
    }
    PriorityQueue<PairOfFloatInt> eS = f2eProbs.get(f).getTranslationsWithProbs(lexProbThreshold);

    if (!eS.isEmpty()) {
      PairOfFloatInt entry = eS.poll();
      int e = entry.getRightElement();
      String eTerm = eVocab_f2e.get(e);
      return eTerm;
    }
    return token;
  }

  protected HMapSFW getTranslations(String token, Map<String, String> stemmed2Stemmed) {
    HMapSFW probDist = new HMapSFW();
    int f = fVocab_f2e.get(token);
    if (f <= 0) {
      // LOG.info("OOV: "+token);

      // heuristic: if no translation found, include itself as only translation
      String targetStem = stemmed2Stemmed.get(token);
      String target = (stemmed2Stemmed == null || targetStem == null) ? token : stemmed2Stemmed.get(token);
      probDist.put(target, 1);      
      return probDist;
    }
    PriorityQueue<PairOfFloatInt> eS = f2eProbs.get(f).getTranslationsWithProbs(lexProbThreshold);
    //    LOG.info("Adding "+ eS.size() +" translations for "+token+","+f);

    float sumProbEF = 0;
    int numTrans = 0;
    //tf(e) = sum_f{tf(f)*prob(e|f)}
    while (numTrans < numTransPerToken && !eS.isEmpty()) {
      PairOfFloatInt entry = eS.poll();
      float probEF = entry.getLeftElement();
      int e = entry.getRightElement();
      String eTerm = eVocab_f2e.get(e);

      //      LOG.info("Pr("+eTerm+"|"+token+")="+probEF);

      if (probEF > 0 && e > 0 && !docLangTokenizer.isStemmedStopWord(eTerm) && (pairsInSCFG == null || pairsInSCFG.contains(new PairOfStrings(token,eTerm)))) {      
        // assuming our bilingual dictionary is learned from normally segmented text, but we want to use bigram tokenizer for CLIR purposes
        // then we need to convert the translations of each source token into a sequence of bigrams
        // we can distribute the translation probability equally to the each bigram
        if (bigramSegment) {
          String[] eTokens = docLangTokenizer.processContent(eTerm);
          float splitProb = probEF / eTokens.length;
          for (String eToken : eTokens) {
            // filter tokens that are not in the index for efficiency
            if (env.getPostingsList(eToken) != null) {
              probDist.put(eToken, splitProb);
            }
          }
          // here we add probability for tokens that we ignored in above condition, 
          // but it works better (empirically) this way 
          // AND it is consistent with what we would get if we did not do the index-filtering above
          // only faster
          sumProbEF += probEF;      
        }else {
          if (env.getPostingsList(eTerm) != null) {
            probDist.increment(eTerm, probEF);
            sumProbEF += probEF;
          }
        }
        numTrans++;
      }else{
        LOG.info("Skipped target stopword/OOV " + eTerm);
      }

      // early terminate if cumulative prob. has reached specified threshold
      if (sumProbEF > cumProbThreshold || numTrans >= numTransPerToken) {
        break;
      }
    }

    // normalize weights
    for(String e : probDist.keySet()){
      probDist.put(e, probDist.get(e) / sumProbEF);
    }

    //    LOG.info("Translations of "+token+"="+probDist);

    return probDist;
  }

  private void getPairsInSCFG(String grammarFile) {
    BufferedReader reader;
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(grammarFile), "UTF-8"));
      String rule = null;
      while ((rule = reader.readLine())!=null) {
        String[] parts = rule.split("\\|\\|\\|");
        String[] lhs = parts[1].trim().split(" ");
        String[] rhs = parts[2].trim().split(" ");;
        for (String l : lhs) {
          for (String r : rhs) {
            pairsInSCFG.add(new PairOfStrings(l, r));
          }
        }
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
