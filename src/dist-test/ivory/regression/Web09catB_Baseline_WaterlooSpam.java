package ivory.regression;

import static org.junit.Assert.assertEquals;
import ivory.eval.Qrels;
import ivory.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.HashMap;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.umd.cloud9.collection.DocnoMapping;

public class Web09catB_Baseline_WaterlooSpam {

	private static final Logger sLogger = Logger.getLogger(Web09catB_Baseline_WaterlooSpam.class);

	private static String[] ql_spam_03_rawAP = new String[] { "1", "0.3370", "2", "0.5055", "3",
			"0.0007", "4", "0.0482", "5", "0.0310", "6", "0.0997", "7", "0.1021", "8", "0.0338",
			"9", "0.2049", "10", "0.0532", "11", "0.4499", "12", "0.1266", "13", "0.0161", "14",
			"0.0834", "15", "0.3814", "16", "0.3198", "17", "0.1791", "18", "0.2973", "19",
			"0.0000", "20", "0.0000", "21", "0.4001", "22", "0.4946", "23", "0.0414", "24",
			"0.1301", "25", "0.2721", "26", "0.1998", "27", "0.2679", "28", "0.3625", "29",
			"0.0580", "30", "0.2299", "31", "0.4840", "32", "0.0760", "33", "0.4936", "34",
			"0.0244", "35", "0.4137", "36", "0.0995", "37", "0.0631", "38", "0.1053", "39",
			"0.1564", "40", "0.2227", "41", "0.1451", "42", "0.0199", "43", "0.5388", "44",
			"0.0651", "45", "0.3230", "46", "0.7002", "47", "0.5457", "48", "0.1925", "49",
			"0.3336", "50", "0.0887" };

	private static String[] ql_spam_03_rawP10 = new String[] { "1", "0.4000", "2", "1.0000", "3",
			"0.0000", "4", "0.2000", "5", "0.1000", "6", "0.0000", "7", "0.5000", "8", "0.1000",
			"9", "0.5000", "10", "0.3000", "11", "0.7000", "12", "0.7000", "13", "0.1000", "14",
			"0.0000", "15", "0.9000", "16", "0.5000", "17", "0.3000", "18", "0.5000", "19",
			"0.0000", "20", "0.0000", "21", "0.6000", "22", "1.0000", "23", "0.2000", "24",
			"0.3000", "25", "0.1000", "26", "0.4000", "27", "0.3000", "28", "0.6000", "29",
			"0.0000", "30", "0.5000", "31", "0.8000", "32", "0.6000", "33", "0.7000", "34",
			"0.1000", "35", "0.7000", "36", "0.6000", "37", "0.1000", "38", "0.5000", "39",
			"0.6000", "40", "0.4000", "41", "0.6000", "42", "0.0000", "43", "0.8000", "44",
			"0.4000", "45", "0.9000", "46", "0.8000", "47", "0.8000", "48", "0.1000", "49",
			"0.6000", "50", "0.2000" };

	private static String[] ql_spam_05_rawAP = new String[] { "1", "0.3335", "2", "0.5232", "3",
			"0.0011", "4", "0.0320", "5", "0.0364", "6", "0.0962", "7", "0.0806", "8", "0.0403",
			"9", "0.1901", "10", "0.0482", "11", "0.4515", "12", "0.0983", "13", "0.0116", "14",
			"0.0894", "15", "0.3756", "16", "0.3158", "17", "0.1926", "18", "0.2957", "19",
			"0.0000", "20", "0.0000", "21", "0.3035", "22", "0.4872", "23", "0.0613", "24",
			"0.1097", "25", "0.2669", "26", "0.1995", "27", "0.2882", "28", "0.3738", "29",
			"0.0525", "30", "0.2234", "31", "0.4636", "32", "0.0779", "33", "0.4715", "34",
			"0.0171", "35", "0.4118", "36", "0.0934", "37", "0.0639", "38", "0.1002", "39",
			"0.1420", "40", "0.2286", "41", "0.1497", "42", "0.0228", "43", "0.5960", "44",
			"0.0642", "45", "0.3168", "46", "0.7009", "47", "0.5706", "48", "0.1975", "49",
			"0.3593", "50", "0.0891" };

	private static String[] ql_spam_05_rawP10 = new String[] { "1", "0.4000", "2", "1.0000", "3",
			"0.0000", "4", "0.1000", "5", "0.1000", "6", "0.0000", "7", "0.3000", "8", "0.2000",
			"9", "0.5000", "10", "0.4000", "11", "0.7000", "12", "0.6000", "13", "0.1000", "14",
			"0.1000", "15", "0.9000", "16", "0.6000", "17", "0.3000", "18", "0.5000", "19",
			"0.0000", "20", "0.0000", "21", "0.5000", "22", "1.0000", "23", "0.4000", "24",
			"0.4000", "25", "0.1000", "26", "0.5000", "27", "0.5000", "28", "0.9000", "29",
			"0.0000", "30", "0.6000", "31", "0.8000", "32", "0.6000", "33", "0.7000", "34",
			"0.1000", "35", "0.9000", "36", "0.6000", "37", "0.1000", "38", "0.7000", "39",
			"0.5000", "40", "0.4000", "41", "0.6000", "42", "0.0000", "43", "0.9000", "44",
			"0.4000", "45", "0.9000", "46", "0.8000", "47", "0.9000", "48", "0.2000", "49",
			"0.7000", "50", "0.2000" };

	private static String[] bm25_spam_002_rawAP = new String[] { "1", "0.4997", "2", "0.5216", "3",
			"0.0014", "4", "0.0299", "5", "0.1416", "6", "0.1191", "7", "0.0447", "8", "0.0512",
			"9", "0.1409", "10", "0.0139", "11", "0.4306", "12", "0.1039", "13", "0.0004", "14",
			"0.1027", "15", "0.3786", "16", "0.3486", "17", "0.2019", "18", "0.1677", "19",
			"0.0000", "20", "0.0000", "21", "0.3393", "22", "0.4852", "23", "0.0569", "24",
			"0.1272", "25", "0.2730", "26", "0.0442", "27", "0.2655", "28", "0.3728", "29",
			"0.0111", "30", "0.2315", "31", "0.4916", "32", "0.0767", "33", "0.4979", "34",
			"0.0061", "35", "0.4410", "36", "0.0945", "37", "0.0825", "38", "0.1121", "39",
			"0.1645", "40", "0.1947", "41", "0.2542", "42", "0.0270", "43", "0.6870", "44",
			"0.0594", "45", "0.3415", "46", "0.7193", "47", "0.5612", "48", "0.1956", "49",
			"0.3046", "50", "0.0828" };

	private static String[] bm25_spam_002_rawP10 = new String[] { "1", "0.8000", "2", "1.0000",
			"3", "0.0000", "4", "0.0000", "5", "0.1000", "6", "0.1000", "7", "0.1000", "8",
			"0.2000", "9", "0.6000", "10", "0.0000", "11", "0.7000", "12", "0.6000", "13",
			"0.0000", "14", "0.2000", "15", "0.9000", "16", "0.8000", "17", "0.3000", "18",
			"0.2000", "19", "0.0000", "20", "0.0000", "21", "0.7000", "22", "1.0000", "23",
			"0.3000", "24", "0.4000", "25", "0.1000", "26", "0.2000", "27", "0.5000", "28",
			"0.9000", "29", "0.0000", "30", "0.5000", "31", "0.9000", "32", "0.4000", "33",
			"0.7000", "34", "0.0000", "35", "0.7000", "36", "0.5000", "37", "0.1000", "38",
			"0.7000", "39", "0.5000", "40", "0.4000", "41", "0.6000", "42", "0.0000", "43",
			"1.0000", "44", "0.3000", "45", "0.9000", "46", "0.8000", "47", "0.9000", "48",
			"0.1000", "49", "0.4000", "50", "0.1000" };

	private static Qrels sQrels;
	private static DocnoMapping sMapping;

	@Test
	public void runRegression() throws Exception {

		Map<String, Map<String, Float>> AllModelsAPScores = new HashMap<String, Map<String, Float>>();

		AllModelsAPScores
				.put("web09catB-ql-waterloo-spam-0.3", loadScoresIntoMap(ql_spam_03_rawAP));
		AllModelsAPScores
				.put("web09catB-ql-waterloo-spam-0.5", loadScoresIntoMap(ql_spam_05_rawAP));
		AllModelsAPScores
				.put("web09catB-bm25-waterloo-spam-0.02", loadScoresIntoMap(bm25_spam_002_rawAP));

		Map<String, Map<String, Float>> AllModelsP10Scores = new HashMap<String, Map<String, Float>>();

		AllModelsP10Scores.put("web09catB-ql-waterloo-spam-0.3",
				loadScoresIntoMap(ql_spam_03_rawP10));
		AllModelsP10Scores.put("web09catB-ql-waterloo-spam-0.5",
				loadScoresIntoMap(ql_spam_05_rawP10));
		AllModelsP10Scores.put("web09catB-bm25-waterloo-spam-0.02",
				loadScoresIntoMap(bm25_spam_002_rawP10));

		sQrels = new Qrels("docs/data/clue/qrels.web09catB.txt");

		String[] params = new String[] { "docs/data/clue/run.web09catB.spam.xml",
				"docs/data/clue/queries.web09.xml" };

		FileSystem fs = FileSystem.getLocal(new Configuration());

		BatchQueryRunner qr = new BatchQueryRunner(params, fs);

		long start = System.currentTimeMillis();
		qr.runQueries();
		long end = System.currentTimeMillis();

		sLogger.info("Total query time: " + (end - start) + "ms");

		sMapping = qr.getDocnoMapping();

		for (String model : qr.getModels()) {
			sLogger.info("Verifying results of model \"" + model + "\"");

			Map<String, Accumulator[]> results = qr.getResults(model);

			verifyResults(model, results, AllModelsAPScores.get(model), AllModelsP10Scores
					.get(model));

			sLogger.info("Done!");
		}

	}

	private static Map<String, Float> loadScoresIntoMap(String[] arr) {
		Map<String, Float> scores = new HashMap<String, Float>();
		for (int i = 0; i < arr.length; i += 2) {
			scores.put(arr[i], Float.parseFloat(arr[i + 1]));
		}

		return scores;
	}

	private static void verifyResults(String model, Map<String, Accumulator[]> results,
			Map<String, Float> apScores, Map<String, Float> p10Scores) {
		float apSum = 0, p10Sum = 0;
		for (String qid : results.keySet()) {
			float ap = (float) RankedListEvaluator.computeAP(results.get(qid), sMapping, sQrels
					.getReldocsForQid(qid));

			float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), sMapping,
					sQrels.getReldocsForQid(qid));

			apSum += ap;
			p10Sum += p10;

			sLogger.info("verifying qid " + qid + " for model " + model);
			assertEquals(apScores.get(qid), ap, 10e-6);
			assertEquals(p10Scores.get(qid), p10, 10e-6);
		}

		float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 50.0f);
		float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / 50.0f);

		if (model.equals("web09catB-ql-waterloo-spam-0.3")) {
			assertEquals(0.2163, MAP, 10e-5);
			assertEquals(0.4220, P10Avg, 10e-5);
		} else if (model.equals("web09catB-ql-waterloo-spam-0.5")) {
			assertEquals(0.2143, MAP, 10e-5);
			assertEquals(0.4540, P10Avg, 10e-5);
		} else if (model.equals("web09catB-bm25-waterloo-spam-0.02")) {
			assertEquals(0.2180, MAP, 10e-5);
			assertEquals(0.4240, P10Avg, 10e-5);
		}

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Web09catB_Baseline_WaterlooSpam.class);
	}
}
