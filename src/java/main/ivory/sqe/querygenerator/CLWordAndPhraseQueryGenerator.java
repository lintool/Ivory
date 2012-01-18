package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.log.Log;


public class CLWordAndPhraseQueryGenerator implements QueryGenerator {
	private static final Logger LOG = Logger.getLogger(CLWordAndPhraseQueryGenerator.class);
	private Tokenizer tokenizer;
	private int length;
	CLWordQueryGenerator clGenerator;
	CLPhraseQueryGenerator phraseGenerator;
	
	public CLWordAndPhraseQueryGenerator() throws IOException {
		super();
	}

	public void init(FileSystem fs, Configuration conf) throws IOException {
		LOG.info(conf.get(Constants.Language));
		LOG.info(conf.get(Constants.TokenizerData));
		LOG.info(conf.get(Constants.Heuristic1));
		LOG.info(conf.get(Constants.Heuristic3));

		tokenizer = TokenizerFactory.createTokenizer(fs, conf.get(Constants.Language), conf.get(Constants.TokenizerData), null);
		clGenerator = new CLWordQueryGenerator();
		phraseGenerator = new CLPhraseQueryGenerator();
		clGenerator.init(fs, conf);
		phraseGenerator.init(fs, conf);
	}

	public JSONObject parseQuery(String query) {
		JSONObject queryJson = new JSONObject();
		try {
			int window = 2;		//window size
			
			String[] tokens = tokenizer.processContent(query);
			length = tokens.length;
			
			// iterate over tokens and phrases, and create weighted representation for each. 
			// save representations into following arrays
			JSONObject[] pweightObjects = new JSONObject[length - 1];
			JSONObject[] weightObjects = new JSONObject[length];
			for (int start = 0; start < length; start++) {
				JSONArray pweights = null, weights = null;
				
				// create a #pweight JSonObject
				if(start < (length - window + 1)){
					String phrase = "";
					for (int k = 0; k < window; k++) {
						int end = start+k;
						phrase = phrase + tokens[end]+" ";
					}
					phrase = phrase.trim();
					pweights = phraseGenerator.getPhraseTranslations(phrase);
					if (pweights != null) {				
						JSONObject phraseTrans = new JSONObject();
						phraseTrans.put("#pweight", pweights);
						pweightObjects[start] = phraseTrans;
					}else {
						// phrase doesn't appear in grammar
						//LOG.info("Skipped "+phrase);
					}
				}
				
				// create a #weight JSonObject
				String token = tokens[start];
				weights = clGenerator.getTranslations(token);
				if (weights != null) {				
					JSONObject wordTrans = new JSONObject();
					wordTrans.put("#weight", weights);
					weightObjects[start] = wordTrans;
				}else {
					// token doesn't appear in vocab
					//LOG.info("Skipped "+token);
				}
			}	
			
			// represent each token with an array of #pweight and #weight objects = tokenArr
			// represent query by #combine of token representations = queryArr
			JSONArray queryArr = new JSONArray();
			for (int i = 0; i < length; i++) {
				//LOG.info("Token # "+ i);
				JSONArray tokenArr = new JSONArray();
				
				// pweight objects at position I-1 and I corr. to token at position I
				if (i-1 >= 0 && pweightObjects[i-1] != null) {
					//LOG.info("pweight1 = " + pweightObjects[i-1]);
					tokenArr.put(pweightObjects[i-1]);
				}
				if (i < length-window+1 && pweightObjects[i] != null) {
					//LOG.info("pweight2 = " + pweightObjects[i]);
					tokenArr.put(pweightObjects[i]);
				}			
				// weight object at position I corr. to token at position I
				if (weightObjects[i] != null) {
					//LOG.info("weight = " + weightObjects[i]);
					tokenArr.put(weightObjects[i]);
				}
				
				int numObjects = tokenArr.length();
				if (numObjects  > 0) {
					// representation of each token is an array of corr. pweight and weight JSonObjects
					JSONObject wordAndPhraseTrans = new JSONObject();
					
					for(int j = numObjects-1; j >= 0; j--){						
						tokenArr.put(2*j+1, tokenArr.get(j));
						tokenArr.put(2*j, 1.0f/numObjects);				// H4 = uniform weighting
					}
					wordAndPhraseTrans.put("#weight", tokenArr);
					queryArr.put(wordAndPhraseTrans);
					
					//LOG.info("combined = " + wordAndPhraseTrans.toString(0));
				}
			}
			
			queryJson.put("#combine", queryArr);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return queryJson;
	}

	public int getQueryLength(){
		return length;  
	}

}
