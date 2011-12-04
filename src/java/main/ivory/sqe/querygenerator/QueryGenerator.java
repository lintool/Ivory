package ivory.sqe.querygenerator;

import org.json.JSONObject;

public interface QueryGenerator {
	  public JSONObject parseQuery(String query);
	  
	  public int getQueryLength();
}
