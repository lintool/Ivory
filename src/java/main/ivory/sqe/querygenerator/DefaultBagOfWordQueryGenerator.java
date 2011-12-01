package ivory.sqe.querygenerator;

import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.Tokenizer;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DefaultBagOfWordQueryGenerator {
  Tokenizer tokenizer;
  int length;
  
  public DefaultBagOfWordQueryGenerator() {
	  super();
	  tokenizer = new GalagoTokenizer();
  }

  public JSONObject parseQuery(String query){
	  String[] tokens = tokenizer.processContent(query);
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
