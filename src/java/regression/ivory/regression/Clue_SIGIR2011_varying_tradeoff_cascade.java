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

public class Clue_SIGIR2011_varying_tradeoff_cascade {

	private static final Logger sLogger = Logger.getLogger(Clue_SIGIR2011_varying_tradeoff_cascade.class);

	private static String[] p1 = new String[] {
	 "26", "0.2659",  "27", "0.1172",  "28", "0.3839",  "29", "0.1036",  "30", "0.6195", 
	 "31", "0.5023",  "32", "0.1599",  "33", "0.556",  "34", "0.1257",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.303",  "39", "0.3315",  "40", "0.2137", 
	 "41", "0.3811",  "42", "0.0",  "43", "0.2779",  "44", "0.1331",  "45", "0.455", 
	 "46", "0.7055",  "47", "0.541",  "48", "0.1292",  "49", "0.2464",  "50", "0.0745"};

	private static String[] p3 = new String[] {
	 "26", "0.2585",  "27", "0.1172",  "28", "0.3839",  "29", "0.1031",  "30", "0.6195", 
	 "31", "0.5023",  "32", "0.1599",  "33", "0.556",  "34", "0.1257",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.303",  "39", "0.3315",  "40", "0.2137", 
	 "41", "0.3698",  "42", "0.0",  "43", "0.2779",  "44", "0.1331",  "45", "0.455", 
	 "46", "0.7055",  "47", "0.541",  "48", "0.1292",  "49", "0.2464",  "50", "0.0745"};

	private static String [] p5 = new String [] {
	 "26", "0.2585",  "27", "0.1172",  "28", "0.3839",  "29", "0.1031",  "30", "0.6195", 
	 "31", "0.5023",  "32", "0.1599",  "33", "0.556",  "34", "0.1257",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.303",  "39", "0.3315",  "40", "0.2137", 
	 "41", "0.3698",  "42", "0.0",  "43", "0.2779",  "44", "0.1331",  "45", "0.455", 
	 "46", "0.7055",  "47", "0.541",  "48", "0.1292",  "49", "0.2464",  "50", "0.0745"};

	private static String [] p7 = new String[] {
	 "26", "0.2333",  "27", "0.1172",  "28", "0.3839",  "29", "0.1074",  "30", "0.5254", 
	 "31", "0.5023",  "32", "0.1933",  "33", "0.5571",  "34", "0.1187",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.303",  "39", "0.3331",  "40", "0.2137", 
	 "41", "0.3831",  "42", "0.0",  "43", "0.2779",  "44", "0.1331",  "45", "0.4159", 
	 "46", "0.7055",  "47", "0.5391",  "48", "0.1292",  "49", "0.2374",  "50", "0.0771"};


        private static String [] p9 = new String[] {
	 "26", "0.2819",  "27", "0.1172",  "28", "0.3839",  "29", "0.124",  "30", "0.2389", 
	 "31", "0.5023",  "32", "0.4082",  "33", "0.5267",  "34", "0.0734",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1755",  "38", "0.2457",  "39", "0.2996",  "40", "0.2137", 
	 "41", "0.3233",  "42", "0.0",  "43", "0.2442",  "44", "0.0848",  "45", "0.2665", 
	 "46", "0.7035",  "47", "0.3875",  "48", "0.1631",  "49", "0.1935",  "50", "0.1145"};


	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("clue-cascade-0.1", new GroundTruth("clue-cascade-0.1", Metric.NDCG20, 25, p1, 0.3061f));

		g.put("clue-cascade-0.3", new GroundTruth("clue-cascade-0.3", Metric.NDCG20, 25, p3, 0.3054f));

		g.put("clue-cascade-0.5", new GroundTruth("clue-cascade-0.5", Metric.NDCG20, 25, p5, 0.3054f));

		g.put("clue-cascade-0.7", new GroundTruth("clue-cascade-0.7", Metric.NDCG20, 25, p7, 0.3006f));

		g.put("clue-cascade-0.9", new GroundTruth("clue-cascade-0.9", Metric.NDCG20, 25, p9, 0.2760f));

		Qrels_new qrels = new Qrels_new("data/clue/qrels.web09catB.txt");

    String[] params = new String[] {
            "data/clue/run.clue.SIGIR2011.varying.tradeoff.cascade.xml",
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
		return new JUnit4TestAdapter(Clue_SIGIR2011_varying_tradeoff_cascade.class);
	}
}
