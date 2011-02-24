package ivory.regression;

import ivory.eval.Qrels;
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

/* Note: different metrics are optimized separately */

public class Web09catB_CIKM2010_Desc_Indep {

	private static final Logger sLogger = Logger.getLogger(Web09catB_CIKM2010_Desc_Indep.class);

	private static String[] x10_rawAP = new String[] {
		"26","0.0018","27","0.0973","28","0.1221","29","0.0001","30","0.0003",
		"31","0.2696","32","0.0000","33","0.4774","34","0.0258","35","0.4027",
		"36","0.0063","37","0.0196","38","0.0358","39","0.1218","40","0.0014",
		"41","0.0280","42","0.0000","43","0.7531","44","0.0004","45","0.1693",
		"46","0.7055","47","0.0317","48","0.1003","49","0.1109","50","0.0004"};

	private static String[] x15_rawAP = new String[] {
		"26","0.1394","27","0.1832","28","0.1052","29","0.0002","30","0.0002",
		"31","0.2658","32","0.0000","33","0.3082","34","0.0377","35","0.2839",
		"36","0.0066","37","0.0844","38","0.0082","39","0.1566","40","0.0112",
		"41","0.1663","42","0.0000","43","0.7228","44","0.0374","45","0.0380",
		"46","0.6954","47","0.2511","48","0.4555","49","0.0152","50","0.0887"};

	private static String[] x20_rawAP = new String[] {
		"26","0.1411","27","0.1167","28","0.1046","29","0.0000","30","0.0001",
		"31","0.1450","32","0.0001","33","0.2696","34","0.0271","35","0.2255",
		"36","0.0039","37","0.1122","38","0.0075","39","0.1556","40","0.0158",
		"41","0.1819","42","0.0000","43","0.7443","44","0.0277","45","0.0376",
		"46","0.6511","47","0.1864","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x25_rawAP = new String[] {
		"26","0.1588","27","0.1192","28","0.0718","29","0.0000","30","0.0005",
		"31","0.1101","32","0.0000","33","0.2468","34","0.0255","35","0.2301",
		"36","0.0099","37","0.0859","38","0.0075","39","0.1439","40","0.0158",
		"41","0.1812","42","0.0000","43","0.7484","44","0.0277","45","0.0404",
		"46","0.6620","47","0.2385","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x30_rawAP = new String[] {
		"26","0.2143","27","0.1148","28","0.0585","29","0.0000","30","0.0004",
		"31","0.1085","32","0.0000","33","0.2435","34","0.0207","35","0.1915",
		"36","0.0129","37","0.0856","38","0.0075","39","0.1439","40","0.0158",
		"41","0.2030","42","0.0000","43","0.7479","44","0.0141","45","0.0623",
		"46","0.6602","47","0.2867","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x35_rawAP = new String[] {
		"26","0.2143","27","0.1148","28","0.0492","29","0.0000","30","0.0002",
		"31","0.1034","32","0.0000","33","0.2435","34","0.0283","35","0.3026",
		"36","0.0111","37","0.0854","38","0.0075","39","0.1439","40","0.0158",
		"41","0.1880","42","0.0104","43","0.7479","44","0.0192","45","0.0623",
		"46","0.6593","47","0.2859","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x40_rawAP = new String[] {
		"26","0.2143","27","0.1148","28","0.0578","29","0.0000","30","0.0002",
		"31","0.0746","32","0.0000","33","0.2435","34","0.0283","35","0.3026",
		"36","0.0111","37","0.0854","38","0.0075","39","0.1439","40","0.0158",
		"41","0.1897","42","0.0104","43","0.7479","44","0.0361","45","0.0623",
		"46","0.6593","47","0.2805","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x45_rawAP = new String[] {
		"26","0.2143","27","0.1148","28","0.0631","29","0.0000","30","0.0002",
		"31","0.0499","32","0.0000","33","0.2435","34","0.0283","35","0.3026",
		"36","0.0111","37","0.0854","38","0.0075","39","0.1439","40","0.0158",
		"41","0.1926","42","0.0104","43","0.7479","44","0.0361","45","0.0623",
		"46","0.6593","47","0.2805","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x50_rawAP = new String[] {
		"26","0.2143","27","0.1148","28","0.0631","29","0.0000","30","0.0002",
		"31","0.0578","32","0.0000","33","0.2435","34","0.0283","35","0.3026",
		"36","0.0111","37","0.0854","38","0.0075","39","0.1439","40","0.0158",
		"41","0.1949","42","0.0088","43","0.7479","44","0.0361","45","0.0623",
		"46","0.6593","47","0.2805","48","0.3951","49","0.0046","50","0.0616"};

	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("indep-x1.0", new GroundTruth("indep-x1.0", Metric.AP, 25, x10_rawAP, 0.1393f));
		g.put("indep-x1.5", new GroundTruth("indep-x1.5", Metric.AP, 25, x15_rawAP, 0.1625f));
		g.put("indep-x2.0", new GroundTruth("indep-x2.0", Metric.AP, 25, x20_rawAP, 0.1446f));
		g.put("indep-x2.5", new GroundTruth("indep-x2.5", Metric.AP, 25, x25_rawAP, 0.1434f));
		g.put("indep-x3.0", new GroundTruth("indep-x3.0", Metric.AP, 25, x30_rawAP, 0.1461f));
		g.put("indep-x3.5", new GroundTruth("indep-x3.5", Metric.AP, 25, x35_rawAP, 0.1502f));
		g.put("indep-x4.0", new GroundTruth("indep-x4.0", Metric.AP, 25, x40_rawAP, 0.1499f));
		g.put("indep-x4.5", new GroundTruth("indep-x4.5", Metric.AP, 25, x45_rawAP, 0.1492f));
		g.put("indep-x5.0", new GroundTruth("indep-x5.0", Metric.AP, 25, x50_rawAP, 0.1496f));

		Qrels qrels = new Qrels("docs/data/clue/qrels.web09catB.txt");

		String[] params = new String[] { "docs/data/clue/run.clue.CIKM2010.desc.indep.xml",
				"docs/data/clue/queries.web09.26-50.desc.xml" };

		FileSystem fs = FileSystem.getLocal(new Configuration());

		BatchQueryRunner qr = new BatchQueryRunner(params, fs);

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
		return new JUnit4TestAdapter(Web09catB_CIKM2010_Desc_Indep.class);
	}
}
