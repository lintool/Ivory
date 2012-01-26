package ivory.sqe.querygenerator;

import java.io.IOException;

import ivory.core.tokenize.BigramChineseTokenizer;
import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DefaultBagOfWordQueryGenerator implements QueryGenerator {
  private static final Logger LOG = Logger.getLogger(CLWordQueryGenerator.class);
  Tokenizer tokenizer;
  int length;
  
  public DefaultBagOfWordQueryGenerator() {
	  super();
  }

	public void init(FileSystem fs, Configuration conf) throws IOException {
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
        LOG.info("Tokenizer path "+tokenizerPath+" doesn't exist -- using BigramChineseTokenizer");
        tokenizer = new BigramChineseTokenizer();
      }else {
        tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.Language), conf.get(Constants.TokenizerData), null);
      }
    }else {
      throw new RuntimeException("Language code "+lang+ " not known");
    }
	}
	
  public JSONObject parseQuery(String query){
	  String[] tokens = tokenizer.processContent(query.trim());
	  length = tokens.length;

	  JSONObject queryJson = new JSONObject();
	  try {
		  queryJson.put("#combine", new JSONArray(tokens));
	  } catch (JSONException e) {
		  e.printStackTrace();
	  }
	  return queryJson;
 }
  
  public int getQueryLength(){
	return length;  
  }
}
