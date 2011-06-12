package ivory.regression;

import ivory.cascade.retrieval.CascadeBatchQueryRunner;
import ivory.eval.Qrels;
import ivory.eval.GradedQrels;
import ivory.regression.GroundTruth.Metric;
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

public class Wt10g_SIGIR2011 {

	private static final Logger sLogger = Logger.getLogger(Wt10g_SIGIR2011.class);

	private static String[] QL = new String[] {
	 "501", "0.3011",  "502", "0.2535",  "503", "0.1479",  "504", "0.7132",  "505", "0.3732", 
	 "506", "0.1186",  "507", "0.553",  "508", "0.3835",  "509", "0.5171",  "510", "0.8517", 
	 "511", "0.433",  "512", "0.3341",  "513", "0.0435",  "514", "0.2113",  "515", "0.4027", 
	 "516", "0.078",  "517", "0.1242",  "518", "0.295",  "519", "0.3927",  "520", "0.1879", 
	 "521", "0.2466",  "522", "0.4229",  "523", "0.3672",  "524", "0.1659",  "525", "0.1637", 
	 "526", "0.0762",  "527", "0.8291",  "528", "0.7814",  "529", "0.4496",  "530", "0.6789", 
	 "531", "0.0613",  "532", "0.511",  "533", "0.4432",  "534", "0.0417",  "535", "0.0974", 
	 "536", "0.2402",  "537", "0.2198",  "538", "0.511",  "539", "0.271",  "540", "0.1964", 
	 "541", "0.527",  "542", "0.0885",  "543", "0.0448",  "544", "0.5999",  "545", "0.2713", 
	 "546", "0.1851",  "547", "0.2474",  "548", "0.6053",  "549", "0.5831",  "550", "0.3949"};

	private static String[] CASCADE = new String[] {
	 "501", "0.3193",  "502", "0.3123",  "503", "0.1036",  "504", "0.7406",  "505", "0.3852", 
	 "506", "0.1186",  "507", "0.5553",  "508", "0.2812",  "509", "0.5533",  "510", "0.8579", 
	 "511", "0.5177",  "512", "0.3084",  "513", "0.0435",  "514", "0.2691",  "515", "0.2837", 
	 "516", "0.078",  "517", "0.0767",  "518", "0.3953",  "519", "0.3367",  "520", "0.1743", 
	 "521", "0.297",  "522", "0.4656",  "523", "0.3672",  "524", "0.1572",  "525", "0.1921", 
	 "526", "0.0762",  "527", "0.8431",  "528", "0.8213",  "529", "0.4364",  "530", "0.6278", 
	 "531", "0.1826",  "532", "0.511",  "533", "0.6656",  "534", "0.0387",  "535", "0.0981", 
	 "536", "0.2841",  "537", "0.2176",  "538", "0.511",  "539", "0.4368",  "540", "0.1964", 
	 "541", "0.4812",  "542", "0.0381",  "543", "0.142",  "544", "0.5432",  "545", "0.4074", 
	 "546", "0.2747",  "547", "0.2521",  "548", "0.5912",  "549", "0.5653",  "550", "0.3659"};


	private static String [] featurePrune = new String [] {
	 "501", "0.3436",  "502", "0.3493",  "503", "0.1164",  "504", "0.7549",  "505", "0.3805", 
	 "506", "0.1065",  "507", "0.5075",  "508", "0.4076",  "509", "0.5365",  "510", "0.8958", 
	 "511", "0.5016",  "512", "0.217",  "513", "0.0435",  "514", "0.2926",  "515", "0.2575", 
	 "516", "0.078",  "517", "0.1398",  "518", "0.3797",  "519", "0.2807",  "520", "0.1995", 
	 "521", "0.2978",  "522", "0.4562",  "523", "0.3672",  "524", "0.0819",  "525", "0.2267", 
	 "526", "0.0762",  "527", "0.7536",  "528", "0.8213",  "529", "0.4671",  "530", "0.5008", 
	 "531", "0.2276",  "532", "0.511",  "533", "0.8196",  "534", "0.0401",  "535", "0.0369", 
	 "536", "0.2756",  "537", "0.0837",  "538", "0.511",  "539", "0.4368",  "540", "0.1964", 
	 "541", "0.4612",  "542", "0.0279",  "543", "0.142",  "544", "0.5066",  "545", "0.4311", 
	 "546", "0.2232",  "547", "0.2189",  "548", "0.6053",  "549", "0.4743",  "550", "0.3614"};


	private static String [] adaRank = new String[] {
	 "501", "0.3237",  "502", "0.3125",  "503", "0.1061",  "504", "0.7395",  "505", "0.3852", 
	 "506", "0.1065",  "507", "0.558",  "508", "0.2991",  "509", "0.5184",  "510", "0.8573", 
	 "511", "0.5177",  "512", "0.309",  "513", "0.0435",  "514", "0.2691",  "515", "0.2813", 
	 "516", "0.078",  "517", "0.0767",  "518", "0.3883",  "519", "0.3182",  "520", "0.1736", 
	 "521", "0.2978",  "522", "0.4672",  "523", "0.3672",  "524", "0.1558",  "525", "0.1917", 
	 "526", "0.0762",  "527", "0.8423",  "528", "0.8213",  "529", "0.4352",  "530", "0.6273", 
	 "531", "0.1842",  "532", "0.511",  "533", "0.6656",  "534", "0.0387",  "535", "0.0997", 
	 "536", "0.2853",  "537", "0.2176",  "538", "0.511",  "539", "0.4368",  "540", "0.1964", 
	 "541", "0.4794",  "542", "0.0381",  "543", "0.142",  "544", "0.5154",  "545", "0.4081", 
	 "546", "0.2631",  "547", "0.2521",  "548", "0.5912",  "549", "0.597",  "550", "0.3691"};


	
	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("wt10g-ql", new GroundTruth("wt10g-ql", Metric.NDCG20, 50, QL, 0.3407f));

		g.put("wt10g-cascade", new GroundTruth("wt10g-cascade", Metric.NDCG20, 50, CASCADE, 0.3560f));

		g.put("wt10g-adaRank", new GroundTruth("wt10g-adaRank", Metric.NDCG20, 50, adaRank, 0.3549f));

		g.put("wt10g-featureprune", new GroundTruth("wt10g-featureprune", Metric.NDCG20, 50, featurePrune, 0.3486f));

		GradedQrels qrels = new GradedQrels("data/wt10g/qrels.wt10g.all");

    String[] params = new String[] {
            "data/wt10g/run.wt10g.SIGIR2011.xml",
            "data/wt10g/queries.wt10g.501-550.xml" };

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
		return new JUnit4TestAdapter(Wt10g_SIGIR2011.class);
	}
}
