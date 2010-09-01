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

public class Web09catB_SIGIR2010 {

	private static final Logger sLogger = Logger.getLogger(Web09catB_SIGIR2010.class);

	private static String[] clue_ql_rawAP = new String[] { "26", "0.1891", "27", "0.1981", "28",
			"0.2793", "29", "0.0679", "30", "0.2060", "31", "0.4260", "32", "0.0696", "33",
			"0.4708", "34", "0.0245", "35", "0.4336", "36", "0.1028", "37", "0.0500", "38",
			"0.0874", "39", "0.1265", "40", "0.1879", "41", "0.1171", "42", "0.0096", "43",
			"0.3539", "44", "0.0431", "45", "0.2405", "46", "0.7038", "47", "0.4459", "48",
			"0.1267", "49", "0.2187", "50", "0.0656" };

	private static String[] clue_sd_rawAP = new String[] { "26", "0.2235", "27", "0.1981", "28",
			"0.2793", "29", "0.0771", "30", "0.2288", "31", "0.4260", "32", "0.0605", "33",
			"0.4704", "34", "0.0265", "35", "0.4336", "36", "0.1028", "37", "0.0526", "38",
			"0.0894", "39", "0.1540", "40", "0.1879", "41", "0.1853", "42", "0.0380", "43",
			"0.3872", "44", "0.0587", "45", "0.2533", "46", "0.6951", "47", "0.4580", "48",
			"0.1167", "49", "0.2513", "50", "0.0664" };

	private static String[] clue_wsd_sd_rawAP = new String[] { "26", "0.1760", "27", "0.1981",
			"28", "0.2793", "29", "0.0566", "30", "0.2366", "31", "0.4260", "32", "0.0510", "33",
			"0.4737", "34", "0.0263", "35", "0.4336", "36", "0.1028", "37", "0.0631", "38",
			"0.0886", "39", "0.1592", "40", "0.1879", "41", "0.2793", "42", "0.0721", "43",
			"0.3995", "44", "0.0848", "45", "0.2631", "46", "0.6812", "47", "0.3810", "48",
			"0.1172", "49", "0.2416", "50", "0.0516" };

	private static Qrels sQrels;
	private static DocnoMapping sMapping;

	@Test
	public void runRegression() throws Exception {

		Map<String, Map<String, Float>> AllModelsAPScores = new HashMap<String, Map<String, Float>>();

		AllModelsAPScores.put("clue-ql", loadScoresIntoMap(clue_ql_rawAP));
		AllModelsAPScores.put("clue-sd", loadScoresIntoMap(clue_sd_rawAP));
		AllModelsAPScores.put("clue-wsd-sd", loadScoresIntoMap(clue_wsd_sd_rawAP));

		sQrels = new Qrels("docs/data/clue/qrels.web09catB.txt");

		String[] params = new String[] { "docs/data/clue/run.web09catB.SIGIR2010.xml",
				"docs/data/clue/queries.web09.26-50.xml" };

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

			verifyResults(model, results, AllModelsAPScores.get(model));

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
			Map<String, Float> apScores) {
		float apSum = 0;
		for (String qid : results.keySet()) {
			float ap = (float) RankedListEvaluator.computeAP(results.get(qid), sMapping, sQrels
					.getReldocsForQid(qid));

			apSum += ap;

			sLogger.info("verifying qid " + qid + " for model " + model);
			assertEquals(apScores.get(qid), ap, 10e-6);
		}

		float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 25.0f);

		if (model.equals("clue_ql")) {
			assertEquals(0.2098, MAP, 10e-5);
		} else if (model.equals("clue_sd")) {
			assertEquals(0.2208, MAP, 10e-5);
		} else if (model.equals("clue_wsd_sd")) {
			assertEquals(0.2212, MAP, 10e-5);
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Web09catB_SIGIR2010.class);
	}
}
