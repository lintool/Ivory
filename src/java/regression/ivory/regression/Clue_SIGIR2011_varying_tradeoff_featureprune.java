package ivory.regression;

import ivory.eval.Qrels;
import ivory.eval.Qrels_new;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;
import ivory.smrf.retrieval.BatchQueryRunner_cascade;

import java.util.HashMap;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.umd.cloud9.collection.DocnoMapping;

public class Clue_SIGIR2011_varying_tradeoff_featureprune {

	private static final Logger sLogger = Logger.getLogger(Clue_SIGIR2011_varying_tradeoff_featureprune.class);

	private static String[] p1 = new String[] {
	 "26", "0.3053",  "27", "0.1172",  "28", "0.3839",  "29", "0.1031",  "30", "0.6167", 
	 "31", "0.5023",  "32", "0.0896",  "33", "0.5895",  "34", "0.0396",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2702",  "39", "0.3421",  "40", "0.2137", 
	 "41", "0.5126",  "42", "0.0",  "43", "0.2562",  "44", "0.1308",  "45", "0.4424", 
	 "46", "0.7208",  "47", "0.3552",  "48", "0.1259",  "49", "0.2554",  "50", "0.0157"};

	private static String[] p3 = new String[] {
	 "26", "0.3053",  "27", "0.1172",  "28", "0.3839",  "29", "0.1031",  "30", "0.6167", 
	 "31", "0.5023",  "32", "0.0896",  "33", "0.5895",  "34", "0.0396",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2702",  "39", "0.3421",  "40", "0.2137", 
	 "41", "0.4383",  "42", "0.0",  "43", "0.2562",  "44", "0.1308",  "45", "0.4424", 
	 "46", "0.7208",  "47", "0.3552",  "48", "0.1259",  "49", "0.2554",  "50", "0.0157"};

	private static String [] p5 = new String [] {
	 "26", "0.3053",  "27", "0.1172",  "28", "0.3839",  "29", "0.1031",  "30", "0.6167", 
	 "31", "0.5023",  "32", "0.0896",  "33", "0.5895",  "34", "0.0396",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2702",  "39", "0.3421",  "40", "0.2137", 
	 "41", "0.4383",  "42", "0.0",  "43", "0.2562",  "44", "0.1308",  "45", "0.4424", 
	 "46", "0.7208",  "47", "0.3552",  "48", "0.1259",  "49", "0.2554",  "50", "0.0157"};

	private static String [] p7 = new String[] {
	 "26", "0.1145",  "27", "0.1172",  "28", "0.3839",  "29", "0.0",  "30", "0.6167", 
	 "31", "0.5023",  "32", "0.4107",  "33", "0.5895",  "34", "0.0396",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2702",  "39", "0.3421",  "40", "0.2137", 
	 "41", "0.4043",  "42", "0.0",  "43", "0.2562",  "44", "0.1308",  "45", "0.4424", 
	 "46", "0.7208",  "47", "0.3552",  "48", "0.1259",  "49", "0.2554",  "50", "0.0157"};

        private static String [] p9 = new String[] {
	 "26", "0.1285",  "27", "0.1172",  "28", "0.3839",  "29", "0.0",  "30", "0.5104", 
	 "31", "0.5023",  "32", "0.4107",  "33", "0.4958",  "34", "0.0396",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2765",  "39", "0.3387",  "40", "0.2137", 
	 "41", "0.4064",  "42", "0.0",  "43", "0.2556",  "44", "0.0848",  "45", "0.2953", 
	 "46", "0.7208",  "47", "0.323",  "48", "0.1238",  "49", "0.1847",  "50", "0.0559"};


	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

                g.put("clue-featureprune-0.1", new GroundTruth("clue-featureprune-0.1", Metric.NDCG20, 25, p1, 0.2966f));

                g.put("clue-featureprune-0.3", new GroundTruth("clue-featureprune-0.3", Metric.NDCG20, 25, p3, 0.2936f));

                g.put("clue-featureprune-0.5", new GroundTruth("clue-featureprune-0.5", Metric.NDCG20, 25, p5, 0.2936f));

                g.put("clue-featureprune-0.7", new GroundTruth("clue-featureprune-0.7", Metric.NDCG20, 25, p7, 0.2934f));

                g.put("clue-featureprune-0.9", new GroundTruth("clue-featureprune-0.9", Metric.NDCG20, 25, p9, 0.2758f));

		Qrels_new qrels = new Qrels_new("data/clue/qrels.web09catB.txt");

    String[] params = new String[] {
            "data/clue/run.clue.SIGIR2011.varying.tradeoff.featureprune.xml",
            "data/clue/queries.web09.26-50.xml" };

		FileSystem fs = FileSystem.getLocal(new Configuration());

		BatchQueryRunner_cascade qr = new BatchQueryRunner_cascade(params, fs);

		long start = System.currentTimeMillis();
		qr.runQueries();
		long end = System.currentTimeMillis();

		sLogger.info("Total query time: " + (end - start) + "ms");

		DocnoMapping mapping = qr.getDocnoMapping();

		for (String model : qr.getModels()) {
			sLogger.info("Verifying results of model \"" + model + "\"");

			Map<String, Accumulator[]> results = qr.getResults(model);
			g.get(model).verify(results, mapping, qrels);

			sLogger.info("Done!");
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Clue_SIGIR2011_varying_tradeoff_featureprune.class);
	}
}
