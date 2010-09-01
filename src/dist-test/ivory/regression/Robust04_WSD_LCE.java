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

public class Robust04_WSD_LCE {

	private static final Logger sLogger = Logger.getLogger(Robust04_WSD_LCE.class);

	private static String[] sDir_WSD_LCE_F_RawAP = new String[] { 
		"601","0.6473","602","0.3065","603","0.2543","604","0.8510","605","0.0950",
		"606","0.7227","607","0.3552","608","0.1146","609","0.3738","610","0.0209",
		"611","0.3010","612","0.6203","613","0.2916","614","0.4448","615","0.0452",
		"616","0.8019","617","0.2778","618","0.1407","619","0.5779","620","0.0320",
		"621","0.4265","622","0.1674","623","0.3365","624","0.3225","625","0.0438",
		"626","0.5603","627","0.0286","628","0.3017","629","0.1824","630","0.7500",
		"631","0.1132","632","0.3888","633","0.6515","634","0.8104","635","0.7538",
		"636","0.2969","637","0.5961","638","0.0923","639","0.3584","640","0.4158",
		"641","0.4686","642","0.1796","643","0.5907","644","0.2280","645","0.6843",
		"646","0.3557","647","0.3594","648","0.6216","649","0.7682","650","0.1907",
		"651","0.0553","652","0.4753","653","0.5926","654","0.7655","655","0.0038",
		"656","0.5384","657","0.5179","658","0.2447","659","0.4141","660","0.7238",
		"661","0.6913","662","0.7024","663","0.6929","664","0.9241","665","0.1842",
		"666","0.0177","667","0.4887","668","0.3813","669","0.0891","670","0.3067",
		"671","0.3445","673","0.1924","674","0.0700","675","0.2792","676","0.3171",
		"677","0.9341","678","0.2440","679","0.9484","680","0.2835","681","0.6288",
		"682","0.3961","683","0.1559","684","0.1173","685","0.2626","686","0.3723",
		"687","0.3902","688","0.1490","689","0.0140","690","0.0089","691","0.3348",
		"692","0.5148","693","0.4030","694","0.4742","695","0.3732","696","0.2745",
		"697","0.1536","698","0.4575","699","0.5620","700","0.6816" };

	private static String[] sDir_WSD_LCE_F_RawP10 = new String[] {
		"601","0.3000","602","0.2000","603","0.3000","604","0.6000","605","0.2000",
		"606","0.5000","607","0.4000","608","0.0000","609","0.6000","610","0.0000",
		"611","0.5000","612","0.6000","613","0.6000","614","0.4000","615","0.0000",
		"616","0.9000","617","0.5000","618","0.1000","619","0.7000","620","0.1000",
		"621","0.8000","622","0.3000","623","0.8000","624","0.4000","625","0.1000",
		"626","0.5000","627","0.1000","628","0.4000","629","0.2000","630","0.3000",
		"631","0.0000","632","0.9000","633","1.0000","634","0.8000","635","0.8000",
		"636","0.5000","637","0.8000","638","0.2000","639","0.7000","640","0.7000",
		"641","0.5000","642","0.3000","643","0.4000","644","0.3000","645","0.9000",
		"646","0.4000","647","0.7000","648","1.0000","649","1.0000","650","0.3000",
		"651","0.0000","652","0.9000","653","0.6000","654","0.9000","655","0.0000",
		"656","0.7000","657","0.7000","658","0.4000","659","0.5000","660","0.9000",
		"661","1.0000","662","0.8000","663","0.6000","664","0.7000","665","0.4000",
		"666","0.0000","667","0.8000","668","0.7000","669","0.1000","670","0.4000",
		"671","0.0000","673","0.4000","674","0.0000","675","0.4000","676","0.2000",
		"677","0.8000","678","0.4000","679","0.6000","680","0.2000","681","0.7000",
		"682","0.8000","683","0.4000","684","0.1000","685","0.2000","686","0.6000",
		"687","0.8000","688","0.6000","689","0.0000","690","0.0000","691","0.5000",
		"692","0.8000","693","0.6000","694","0.4000","695","0.9000","696","0.5000",
		"697","0.4000","698","0.4000","699","0.7000","700","0.9000" };


	private static Qrels sQrels;
	private static DocnoMapping sMapping;

	@Test
	public void runRegression() throws Exception {

		Map<String, Map<String, Float>> AllModelsAPScores = new HashMap<String, Map<String, Float>>();

		AllModelsAPScores.put("robust04-dir-wsd-lce-f", loadScoresIntoMap(sDir_WSD_LCE_F_RawAP));

		Map<String, Map<String, Float>> AllModelsP10Scores = new HashMap<String, Map<String, Float>>();

		AllModelsP10Scores.put("robust04-dir-wsd-lce-f", loadScoresIntoMap(sDir_WSD_LCE_F_RawP10));

		sQrels = new Qrels("docs/data/trec/qrels.robust04.noCRFR.txt");

		String[] params = new String[] { "docs/data/trec/run.robust04.wsd.lce.xml",
				"docs/data/trec/queries.robust04.xml" };

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

			if ((apScores.get(qid))!=null && ap!=0){

				sLogger.info("verifying qid " + qid + " for model " + model);
				assertEquals(apScores.get(qid), ap, 10e-6);
				assertEquals(p10Scores.get(qid), p10, 10e-6);
			}
		}

		// one topic didn't contain qrels, so trec_eval only picked up 99 topics
		float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 99f);
		float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / 99f);

		if (model.equals("robust04-dir-wsd-lce-f")) {
			assertEquals(0.3885, MAP, 10e-5);
			assertEquals(0.4848, P10Avg, 10e-5);
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Robust04_WSD_LCE.class);
	}
}
