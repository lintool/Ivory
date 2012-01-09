package ivory.sqe.querygenerator;

import ivory.core.tokenize.Tokenizer;
import ivory.core.tokenize.TokenizerFactory;
import ivory.sqe.retrieval.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.log.Log;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.HMapKF;


public class CLPhraseQueryGenerator implements QueryGenerator {
	private static final Logger LOG = Logger.getLogger(CLPhraseQueryGenerator.class);
	private Tokenizer tokenizer;
	private Map<String, HMapKF<String>> phraseTable;
	private int length;

	public CLPhraseQueryGenerator() throws IOException {
		super();
	}

	public void init(FileSystem fs, Configuration conf) throws IOException {
		phraseTable = generatePhraseTable(conf.get(Constants.SCFGPath));
		tokenizer = TokenizerFactory.createTokenizer(fs, Constants.SourceLanguageCode, Constants.TokenizerModelPath, null);
	}

	public JSONObject parseQuery(String query) {
		JSONObject queryJson = new JSONObject();
		try {

			String[] tokens = tokenizer.processContent(query);
			length = tokens.length;
			JSONArray phraseTranslations = new JSONArray();
			boolean[] isCovered = new boolean[length];

			int window = 2;		//window size

			for(int start=0; start<tokens.length-(window-1);start++){
				String phrase = "";
				for(int k=0; k<window; k++){
					int end = start+k;
					phrase = phrase + tokens[end]+" ";
				}
				phrase = phrase.trim();
				JSONArray weights = addPhraseTranslations(phrase, phraseTable);
				if (weights != null) {				
					JSONObject phraseTrans = new JSONObject();
					phraseTrans.put("#pweight", weights);
					phraseTranslations.put(phraseTrans);
				}else {
					// ????
					Log.info("Skipped "+phrase);
				}
				for(int k=0; k<window; k++){
					int end = start+k;
					isCovered[end] = true;
				}
			}	
			queryJson.put("#combine", phraseTranslations);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return queryJson;
	}

	public int getQueryLength(){
		return length;  
	}

	private static JSONArray addPhraseTranslations(String phrase, Map<String, HMapKF<String>> phraseTable) {
		HMapKF<String> translation2prob = phraseTable.get(phrase);
		if(translation2prob==null){
			return null;
		}
		
		JSONArray phraseTranslationsArr = new JSONArray();
		for(String translation : translation2prob.keySet()){
			try {
				float prob = translation2prob.get(translation);
				phraseTranslationsArr.put(prob);
				phraseTranslationsArr.put(translation);
			} catch (JSONException e) {
				throw new RuntimeException("Error adding translation and prob values");
			}
		}

		return phraseTranslationsArr;
	}

	private static Map<String, HMapKF<String>> generatePhraseTable(String grammarFile) {
		Map<String,HMapKF<String>> phraseTable = new HashMap<String,HMapKF<String>>();
		try {
			BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(grammarFile), "UTF-8"));
			String rule = null;
			while((rule = r.readLine())!=null){
				String[] parts = rule.split("\\|\\|\\|");
				String[] lhs = parts[1].trim().split(" ");
				String[] rhs = parts[2].trim().split(" ");;
				String[] probs = parts[3].trim().split(" ");
				String[] alignments = parts[4].trim().split(" ");;

				// need more than 1 alignment and target term to get a phrase translation
				if(alignments.length<2 || rhs.length<2)
					continue;

				HMapIV<ArrayListOfInts> one2manyAlign = readAlignments(alignments);

				//phrase table
				updatePhraseTable(phraseTable, lhs, rhs, probs, one2manyAlign);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return phraseTable;
	}

	private static void updatePhraseTable(Map<String, HMapKF<String>> phraseTable, String[] lhs, String[] rhs, String[] probs, HMapIV<ArrayListOfInts> one2manyAlign) {
		int w=2;	
		float prob = (float) Math.pow(2, -Float.parseFloat(probs[0]));
		for(int f=0; f<lhs.length-(w-1);f++){
			String fPhrase = "";
			// phrase window = [f,f+w)
			int cur =f;
			ArrayListOfInts phraseTranslationIds = new ArrayListOfInts();
			for(;cur<f+w;cur++){
				ArrayListOfInts translationIds = one2manyAlign.get(cur);
				if(translationIds==null){
					// if there are any unaligned source terms in this window, move to next f
					f = cur; 
					break;
				}
				String fTerm = lhs[cur];
				if(fTerm.matches("\\[X,\\d+\\]") || fTerm.equals("<s>") || fTerm.equals("</s>"))	 break;
				phraseTranslationIds = phraseTranslationIds.merge(translationIds);
				fPhrase += fTerm+" ";
			}
			//if loop terminated early, move to next f
			if(cur < f+w){
				continue;
			}

			if(isConsecutive(phraseTranslationIds)){

				String transPhrase = "";
				boolean ignore = false;
				for(int e : phraseTranslationIds){
					String eTerm = rhs[e];
					if(eTerm.matches("\\[X,\\d+\\]") || eTerm.equals("<s>") || eTerm.equals("</s>")){
						ignore = true;
						break;
					}
					transPhrase += eTerm+" ";
				}

				//add phrase pair to table
				if(!ignore){
					if(!phraseTable.containsKey(fPhrase.trim())){
						phraseTable.put(fPhrase.trim(), new HMapKF<String>());
					}
					phraseTable.get(fPhrase.trim()).increment(transPhrase.trim(), prob);
				}
			}
		}		
	}

	private static boolean isConsecutive(ArrayListOfInts lst) {
		int prev = -1;
		for(int i : lst){
			if(prev != -1 && i!=prev+1){
				return false;
			}
			prev = i;
		}
		return true;
	}

	private static HMapIV<ArrayListOfInts> readAlignments(String[] alignments) {
		HMapIV<ArrayListOfInts> one2manyAlign = new HMapIV<ArrayListOfInts>();
		for(String alignment : alignments){
			String[] alPair = alignment.split("-");
			int f = Integer.parseInt(alPair[0]);
			int e = Integer.parseInt(alPair[1]);
			if(!one2manyAlign.containsKey(f)){
				one2manyAlign.put(f, new ArrayListOfInts());	
			}
			one2manyAlign.get(f).add(e);
		}
		return one2manyAlign;
	}

}
