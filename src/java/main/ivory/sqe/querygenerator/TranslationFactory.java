package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.sqe.retrieval.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HMapStIW;

public class TranslationFactory {
  private static final Logger LOG = Logger.getLogger(TranslationFactory.class);

  public static Translation readTranslationsFromNBest(String queryRepresentation, float alpha, Set<String> unknownWords, 
      Tokenizer queryLangTokenizer, Tokenizer docLangTokenizer, Configuration conf) {
    String[] arr = queryRepresentation.trim().split("\\|\\|\\|\\|");
    String origQuery = arr[0];

    Map<String,String> stemmed2Stemmed = Utils.getStemMapping(origQuery, queryLangTokenizer, docLangTokenizer);

    int n = arr.length - 1;
    if (n <= 0) {
      throw new RuntimeException("Bad query format!: " + queryRepresentation);
    }
        
    // apply discount on logprobs to avoid floating point errors
    float discount = 0;
    String[] line = arr[1].trim().split(";;;");
    discount = -Float.parseFloat(line[0]);
    
    float[] transProbs = new float[n];
    float sumOfProbs = 0;
    for (int k = 0; k < n; k++){
      line = arr[k+1].trim().split(";;;");
      transProbs[k] = (float) Math.pow(Math.E, alpha * (Float.parseFloat(line[0]) + discount));
      sumOfProbs += transProbs[k];
    }
        
    boolean isPhrase = conf.getInt(Constants.MaxWindow, 0) > 0;
    int one2many = conf.getInt(Constants.One2Many, 2);
    
    // src token --> (trg token --> prob(trg|src))
    Map<String,HMapStFW> token2tokenDist = new HashMap<String,HMapStFW>();
    
    // target phrase --> prob
    HMapStFW phraseDist = new HMapStFW();
    
    HMapStIW srcTokenCnt = new HMapStIW();

    Set<String> bagOfTargetTokens = new HashSet<String>();

    for (int k = 0; k < n; k++) {
      transProbs[k] = transProbs[k]/sumOfProbs;
      line = arr[k+1].trim().split(";;;");
      // first elt of line is the score of kth best translation
      for (int i = 1; i < line.length; i++) {
        try {
          Utils.processRule(one2many, isPhrase, transProbs[k], line[i], bagOfTargetTokens, token2tokenDist, phraseDist, srcTokenCnt, 
              queryLangTokenizer, docLangTokenizer, stemmed2Stemmed, unknownWords);
        } catch (Exception e) {
          e.printStackTrace();
          LOG.info("Error while processing rule: " + line[i]);
        }
      }
    }
    
    // normalize
    Utils.normalize(token2tokenDist, conf.getFloat(Constants.LexicalProbThreshold, 0), conf.getFloat(Constants.CumulativeProbThreshold, 1f), 30);
    Utils.filter(phraseDist, conf.getFloat(Constants.LexicalProbThreshold, 0));

    return new TranslationFromNBest(n, origQuery, stemmed2Stemmed, bagOfTargetTokens, token2tokenDist, phraseDist, srcTokenCnt);
    
  }
}
