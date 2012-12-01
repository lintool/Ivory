package ivory.sqe.querygenerator;

import ivory.core.tokenize.BigramChineseTokenizer;
import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class BagOfWordsQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(ProbabilisticStructuredQueryGenerator.class);
  Tokenizer tokenizer;
  int length;
  
  public BagOfWordsQueryGenerator() {
	  super();
  }

	public void init(FileSystem fs, Configuration conf) throws IOException {
    if (conf.getBoolean(Constants.Quiet, false)) {
      LOG.setLevel(Level.OFF);
    }
    
    String lang = conf.get(Constants.DocLanguage);
    String tokenizerPath = conf.get(Constants.DocTokenizerData);
    if (lang.equals(Constants.English)) {
      if (!fs.exists(new Path(tokenizerPath))) {
        LOG.info("Tokenizer path "+tokenizerPath+" doesn't exist -- using GalagoTokenizer");
        tokenizer = new GalagoTokenizer();    
      }else {
        tokenizer = TokenizerFactory.createTokenizer(lang, tokenizerPath, null);
      }
    }else if (lang.equals(Constants.German)) {
      tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.DocLanguage), conf.get(Constants.DocTokenizerData), null);
    }else if (lang.equals(Constants.Chinese)) {
      if (!fs.exists(new Path(tokenizerPath))) {
        LOG.info("Tokenizer path "+tokenizerPath+" doesn't exist -- using BigramChineseTokenizer");
        tokenizer = new BigramChineseTokenizer();
      }else {
        tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.DocLanguage), conf.get(Constants.DocTokenizerData), null);
      }
    }else {
      throw new RuntimeException("DocLanguage code "+lang+ " not known");
    }
	}
	
  @Override
  public void init(Configuration conf) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public JsonObject parseQuery(String query){
	  String[] tokens = tokenizer.processContent(query.trim());
	  length = tokens.length;

	  JsonObject queryJson = new JsonObject();
		queryJson.add("#combine", Utils.createJsonArray(tokens));
		  
	  return queryJson;
 }
  
  public int getQueryLength(){
	return length;  
  }

}
