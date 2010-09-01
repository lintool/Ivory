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

public class Gov2_SIGIR2010 {

	private static final Logger sLogger = Logger.getLogger(Gov2_SIGIR2010.class);

	private static String[] gov2_ql_rawAP = new String[] { 
		"776","0.2608","777","0.3142","778","0.1082","779","0.4978","780","0.3421",
		"781","0.3598","782","0.6516","783","0.2095","784","0.4571","785","0.4402",
		"786","0.4279","787","0.5753","788","0.5235","789","0.2194","790","0.5108",
		"791","0.4538","792","0.1647","793","0.2988","794","0.0645","795","0.0246",
		"796","0.1733","797","0.6315","798","0.1932","799","0.1891","800","0.1998",
		"801","0.4514","802","0.4094","803","0.0000","804","0.4933","805","0.0443",
		"806","0.0141","807","0.5632","808","0.7256","809","0.3118","810","0.3130",
		"811","0.2744","812","0.5479","813","0.3021","814","0.7217","815","0.1545",
		"816","0.7935","817","0.3378","818","0.2183","819","0.5572","820","0.7611",
		"821","0.2489","822","0.1110","823","0.3301","824","0.3342","825","0.0533",
		"826","0.2922","827","0.4429","828","0.2497","829","0.0922","830","0.0335",
		"831","0.6072","832","0.1760","833","0.5381","834","0.3873","835","0.0484",
		"836","0.2219","837","0.0788","838","0.2846","839","0.5712","840","0.1665",
		"841","0.3718","842","0.1087","843","0.4271","844","0.0504","845","0.3979",
		"846","0.1982","847","0.3106","848","0.1165","849","0.2089","850","0.2177"};

	private static String[] gov2_sd_rawAP = new String[] { 
		"776","0.2516","777","0.2833","778","0.1522","779","0.4696","780","0.3818",
		"781","0.3296","782","0.6560","783","0.2324","784","0.4571","785","0.5624",
		"786","0.4286","787","0.5695","788","0.5757","789","0.2228","790","0.4777",
		"791","0.4256","792","0.1689","793","0.3365","794","0.1415","795","0.0257",
		"796","0.1733","797","0.6027","798","0.1720","799","0.1351","800","0.1943",
		"801","0.4518","802","0.4829","803","0.0003","804","0.5113","805","0.0688",
		"806","0.0690","807","0.5949","808","0.6908","809","0.3288","810","0.3806",
		"811","0.3021","812","0.5988","813","0.3097","814","0.7128","815","0.1622",
		"816","0.7636","817","0.4217","818","0.2115","819","0.5763","820","0.7706",
		"821","0.3505","822","0.0938","823","0.4169","824","0.3294","825","0.1074",
		"826","0.3519","827","0.4385","828","0.3407","829","0.1291","830","0.0745",
		"831","0.6338","832","0.1715","833","0.4465","834","0.3916","835","0.1256",
		"836","0.2195","837","0.0680","838","0.2533","839","0.5949","840","0.1665",
		"841","0.3948","842","0.1744","843","0.4942","844","0.0837","845","0.3752",
		"846","0.1997","847","0.2352","848","0.1117","849","0.3618","850","0.2083"};

	private static String[] gov2_wsd_sd_rawAP = new String[] { 
		"776","0.2491","777","0.2743","778","0.1716","779","0.4568","780","0.4127",
		"781","0.3463","782","0.6412","783","0.2160","784","0.4507","785","0.6215",
		"786","0.4133","787","0.5624","788","0.5776","789","0.2258","790","0.4496",
		"791","0.4114","792","0.1613","793","0.3560","794","0.2095","795","0.0255",
		"796","0.1624","797","0.5643","798","0.1639","799","0.0688","800","0.1905",
		"801","0.4464","802","0.4472","803","0.0009","804","0.5242","805","0.0852",
		"806","0.2282","807","0.6044","808","0.6920","809","0.3448","810","0.4131",
		"811","0.3065","812","0.6094","813","0.3101","814","0.7038","815","0.1841",
		"816","0.7767","817","0.4384","818","0.2734","819","0.5650","820","0.7703",
		"821","0.3654","822","0.0904","823","0.4170","824","0.3259","825","0.1266",
		"826","0.3574","827","0.3984","828","0.3744","829","0.2135","830","0.0932",
		"831","0.5624","832","0.1636","833","0.3710","834","0.4282","835","0.1627",
		"836","0.2101","837","0.0578","838","0.3016","839","0.5967","840","0.1665",
		"841","0.4767","842","0.2080","843","0.5148","844","0.1092","845","0.4022",
		"846","0.1971","847","0.2503","848","0.1116","849","0.3912","850","0.2086"};

	private static Qrels sQrels;
	private static DocnoMapping sMapping;

	@Test
	public void runRegression() throws Exception {

		Map<String, Map<String, Float>> AllModelsAPScores = new HashMap<String, Map<String, Float>>();

		AllModelsAPScores.put("gov2-ql", loadScoresIntoMap(gov2_ql_rawAP));
		AllModelsAPScores.put("gov2-sd", loadScoresIntoMap(gov2_sd_rawAP));
		AllModelsAPScores.put("gov2-wsd-sd", loadScoresIntoMap(gov2_wsd_sd_rawAP));

		sQrels = new Qrels("docs/data/gov2/qrels.gov2.all");

		String[] params = new String[] { "docs/data/gov2/run.gov2.SIGIR2010.xml",
				"docs/data/gov2/gov2.title.776-850" };

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

			if (qid.trim().equals("815")){
				assertEquals(apScores.get(qid), ap, 10e-4);
			}
			else {
				assertEquals(apScores.get(qid), ap, 10e-6);
			}
		}

		float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 75.0f);

		if (model.equals("gov2-dir-base")) {
			assertEquals(0.3195, MAP, 10e-5);
		} else if (model.equals("gov2-dir-sd")) {
			assertEquals(0.3357, MAP, 10e-5);
		} else if (model.equals("gov2-wsd-sd")) {
			assertEquals(0.3435, MAP, 10e-5);
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Gov2_SIGIR2010.class);
	}
}
