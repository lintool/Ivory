package ivory.regression;

import ivory.eval.Qrels;
import ivory.eval.Qrels_new;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;
import ivory.smrf.retrieval.CascadeBatchQueryRunner;

import java.util.HashMap;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import edu.umd.cloud9.collection.DocnoMapping;

public class Clue_SIGIR2011 {

	private static final Logger sLogger = Logger.getLogger(Clue_SIGIR2011.class);

	private static String[] QL = new String[] {
	 "26", "0.2819",  "27", "0.1172",  "28", "0.3839",  "29", "0.124",  "30", "0.2389", 
	 "31", "0.5023",  "32", "0.4082",  "33", "0.5267",  "34", "0.0734",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1755",  "38", "0.2457",  "39", "0.2996",  "40", "0.2137", 
	 "41", "0.3233",  "42", "0.0",  "43", "0.2206",  "44", "0.0848",  "45", "0.2665", 
	 "46", "0.7035",  "47", "0.3875",  "48", "0.1631",  "49", "0.1935",  "50", "0.1145"};

	private static String[] CASCADE = new String[] {
	 "26", "0.2659",  "27", "0.1172",  "28", "0.3839",  "29", "0.1036",  "30", "0.6195", 
	 "31", "0.5023",  "32", "0.1599",  "33", "0.556",  "34", "0.1257",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.303",  "39", "0.3315",  "40", "0.2137", 
	 "41", "0.3811",  "42", "0.0",  "43", "0.2779",  "44", "0.1331",  "45", "0.455", 
	 "46", "0.7055",  "47", "0.541",  "48", "0.1292",  "49", "0.2464",  "50", "0.0745"};

	private static String [] featurePrune = new String [] {
	 "26", "0.3053",  "27", "0.1172",  "28", "0.3839",  "29", "0.1031",  "30", "0.6167", 
	 "31", "0.5023",  "32", "0.0896",  "33", "0.5895",  "34", "0.0396",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2702",  "39", "0.3421",  "40", "0.2137", 
	 "41", "0.5126",  "42", "0.0",  "43", "0.2562",  "44", "0.1308",  "45", "0.4424", 
	 "46", "0.7208",  "47", "0.3552",  "48", "0.1259",  "49", "0.2554",  "50", "0.0157"};

	private static String [] adaRank = new String[] {
	 "26", "0.3231",  "27", "0.1172",  "28", "0.3839",  "29", "0.1076",  "30", "0.5403", 
	 "31", "0.5023",  "32", "0.1683",  "33", "0.5569",  "34", "0.1202",  "35", "0.5955", 
	 "36", "0.2334",  "37", "0.1982",  "38", "0.2932",  "39", "0.3672",  "40", "0.2137", 
	 "41", "0.4291",  "42", "0.0",  "43", "0.2997",  "44", "0.1292",  "45", "0.4095", 
	 "46", "0.7705",  "47", "0.5748",  "48", "0.1297",  "49", "0.2138",  "50", "0.0599"};


	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("clue-ql", new GroundTruth("clue-ql", Metric.NDCG20, 25, QL, 0.2751f));

		g.put("clue-cascade", new GroundTruth("clue-cascade", Metric.NDCG20, 25, CASCADE, 0.3061f));

		g.put("clue-adaRank", new GroundTruth("clue-adaRank", Metric.NDCG20, 25, adaRank, 0.3095f));

		g.put("clue-featureprune", new GroundTruth("clue-featureprune", Metric.NDCG20, 25, featurePrune, 0.2966f));

		Qrels_new qrels = new Qrels_new("data/clue/qrels.web09catB.txt");

    String[] params = new String[] {
            "data/clue/run.clue.SIGIR2011.xml",
            "data/clue/queries.web09.26-50.xml" };

		FileSystem fs = FileSystem.getLocal(new Configuration());

		CascadeBatchQueryRunner qr = new CascadeBatchQueryRunner(params, fs);

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
		return new JUnit4TestAdapter(Clue_SIGIR2011.class);
	}
}
