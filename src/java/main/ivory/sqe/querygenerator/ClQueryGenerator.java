package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.log.Log;
import edu.umd.hooka.VocabularyWritable;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class ClQueryGenerator implements QueryGenerator {
	private static final Logger LOG = Logger.getLogger(ClQueryGenerator.class);
	private Tokenizer tokenizer;
	private VocabularyWritable fVocab_f2e, eVocab_f2e;
	private TTable_monolithic_IFAs f2eProbs;
	private int length;
	private float probThreshold = Float.MAX_VALUE;
	
	public ClQueryGenerator() throws IOException {
		super();
	}

	public void init(FileSystem fs, Configuration conf) throws IOException {
		fVocab_f2e = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.fVocabPath)), fs);
		eVocab_f2e = (VocabularyWritable) HadoopAlign.loadVocab(new Path(conf.get(Constants.eVocabPath)), fs);
		
		f2eProbs = new TTable_monolithic_IFAs(fs, new Path(conf.get(Constants.f2eProbsPath)), true);
		tokenizer = TokenizerFactory.createTokenizer(fs, conf.get(Constants.SourceLanguageCode), conf.get(Constants.TokenizerModelPath), fVocab_f2e);
		probThreshold = conf.getFloat(Constants.ProbThreshold, 0.0f);
	}

	public JSONObject parseQuery(String query) {
		JSONObject queryJson = new JSONObject();
		try {

			String[] tokens = tokenizer.processContent(query);
			length = tokens.length;
			JSONArray tokenTranslations = new JSONArray();
			for (String token : tokens) {
				JSONObject tokenTrans = new JSONObject();
				JSONArray weights = addTranslations(token);
				if (weights != null) {				
					tokenTrans.put("#weight", weights);
					tokenTranslations.put(tokenTrans);
				}else {
					// ????
					Log.info("Skipped "+token);
				}
			}
			queryJson.put("#combine", tokenTranslations);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return queryJson;
	}

	private JSONArray addTranslations(String token) {
		int f = fVocab_f2e.get(token);
		if (f <= 0) {
			return null;
		}
		LOG.info("Adding translations for "+token);
		JSONArray arr = new JSONArray();
		int[] eS = f2eProbs.get(f).getTranslations(0.0f);

		float sumProbEF = 0;
		
		//tf(e) = sum_f{tf(f)*prob(e|f)}
		for (int e : eS) {
			float probEF;
			String eTerm = eVocab_f2e.get(e);

			probEF = f2eProbs.get(f, e);
			if(probEF > 0){
				try {
					arr.put(probEF);
					arr.put(eTerm);
					sumProbEF += probEF;
					LOG.info("adding "+eTerm+","+probEF+","+sumProbEF);
				} catch (JSONException e1) {
					throw new RuntimeException("Error adding translation and prob values");
				}
			}
			
			// early terminate if cumulative prob. has reached specified threshold
			if (sumProbEF > probThreshold) {
				break;
			}
		}
		for (int i = 0; i < arr.length(); i=i+2){
			try {
				float pr = (float) arr.getDouble(i);
				arr.put(i, pr/sumProbEF);
			} catch (JSONException e1) {
				throw new RuntimeException("Error normalizing");
			}
		}
		return arr;
	}

	public int getQueryLength(){
		return length;  
	}

}
