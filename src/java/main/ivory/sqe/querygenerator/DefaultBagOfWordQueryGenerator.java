package ivory.sqe.querygenerator;

import java.io.IOException;

import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DefaultBagOfWordQueryGenerator implements QueryGenerator {
  Tokenizer tokenizer;
  int length;

  public DefaultBagOfWordQueryGenerator() {
    super();
  }

  public void init(FileSystem fs, Configuration conf) throws IOException {
    if (conf.get(Constants.Language).equals(Constants.English)) {
      tokenizer = new GalagoTokenizer();
    } else if (conf.get(Constants.Language).equals(Constants.German)) {
      tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.Language), conf
          .get(Constants.TokenizerData), null);
    } else if (conf.get(Constants.Language).equals(Constants.Chinese)) {
      tokenizer = TokenizerFactory.createTokenizer(conf.get(Constants.Language), conf
          .get(Constants.TokenizerData), null);
    } else {
      throw new RuntimeException("Language code " + conf.get(Constants.Language) + " not known");
    }
  }

  public JSONObject parseQuery(String query) {
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

  public int getQueryLength() {
    return length;
  }
}
