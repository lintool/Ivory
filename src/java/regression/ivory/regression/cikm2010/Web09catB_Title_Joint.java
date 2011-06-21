package ivory.regression.cikm2010;

import ivory.eval.Qrels;
import ivory.regression.GroundTruth;
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

public class Web09catB_Title_Joint {

	private static final Logger sLogger = Logger.getLogger(Web09catB_Title_Joint.class);

	private static String[] x10_rawAP = new String[] {
			"26", "0.0539", "27", "0.1993", "28", "0.2890", "29", "0.0123", "30", "0.2322",
			"31", "0.4254", "32", "0.0718", "33", "0.4791", "34", "0.0245", "35", "0.4572",
			"36", "0.1041", "37", "0.0624", "38", "0.1025", "39", "0.1427", "40", "0.1838",
			"41", "0.2528", "42", "0.0181", "43", "0.5076", "44", "0.0500", "45", "0.2759",
			"46", "0.7204", "47", "0.5047", "48", "0.1471", "49", "0.2458", "50", "0.0774" };

	private static String[] x15_rawAP = new String[] {
			"26", "0.0539", "27", "0.1993", "28", "0.2890", "29", "0.0123", "30", "0.2322",
			"31", "0.4254", "32", "0.0718", "33", "0.4791", "34", "0.0245", "35", "0.4572",
			"36", "0.1041", "37", "0.0624", "38", "0.1025", "39", "0.1427", "40", "0.1838",
			"41", "0.2513", "42", "0.0181", "43", "0.5076", "44", "0.0500", "45", "0.2759",
			"46", "0.7206", "47", "0.5047", "48", "0.1471", "49", "0.2458", "50", "0.0774" };

	private static String[] x20_rawAP = new String[] {
			"26", "0.0764", "27", "0.1978", "28", "0.2795", "29", "0.0159", "30", "0.2520",
			"31", "0.4256", "32", "0.0704", "33", "0.4791", "34", "0.0245", "35", "0.4332",
			"36", "0.1030", "37", "0.0624", "38", "0.1078", "39", "0.1561", "40", "0.1844",
			"41", "0.2359", "42", "0.0553", "43", "0.5366", "44", "0.0492", "45", "0.2747",
			"46", "0.7206", "47", "0.5114", "48", "0.1586", "49", "0.2720", "50", "0.0819" };

	private static String[] x25_rawAP = new String[] {
			"26", "0.1332", "27", "0.1978", "28", "0.2795", "29", "0.0227", "30", "0.2520",
			"31", "0.4256", "32", "0.0528", "33", "0.4791", "34", "0.0245", "35", "0.4332",
			"36", "0.1030", "37", "0.0624", "38", "0.1078", "39", "0.1561", "40", "0.1844",
			"41", "0.2453", "42", "0.0553", "43", "0.5366", "44", "0.0181", "45", "0.2747",
			"46", "0.7205", "47", "0.5114", "48", "0.1586", "49", "0.2720", "50", "0.0819" };

	private static String[] x30_rawAP = new String[] {
			"26", "0.1705", "27", "0.1978", "28", "0.2795", "29", "0.0358", "30", "0.2459",
			"31", "0.4256", "32", "0.0604", "33", "0.4440", "34", "0.0365", "35", "0.4332",
			"36", "0.1030", "37", "0.0700", "38", "0.0868", "39", "0.1676", "40", "0.1844",
			"41", "0.2779", "42", "0.0973", "43", "0.4486", "44", "0.0231", "45", "0.3028",
			"46", "0.7167", "47", "0.5774", "48", "0.1031", "49", "0.2703", "50", "0.0528" };

	private static String[] x35_rawAP = new String[] {
			"26", "0.1355", "27", "0.1978", "28", "0.2795", "29", "0.0476", "30", "0.2459",
			"31", "0.4256", "32", "0.0286", "33", "0.4440", "34", "0.0365", "35", "0.4332",
			"36", "0.1030", "37", "0.0700", "38", "0.0868", "39", "0.1676", "40", "0.1844",
			"41", "0.3287", "42", "0.0973", "43", "0.4486", "44", "0.0853", "45", "0.3028",
			"46", "0.7167", "47", "0.5774", "48", "0.1031", "49", "0.2703", "50", "0.0528" };

	private static String[] x40_rawAP = new String[] {
			"26", "0.1355", "27", "0.1978", "28", "0.2795", "29", "0.0476", "30", "0.2459",
			"31", "0.4256", "32", "0.0286", "33", "0.4440", "34", "0.0365", "35", "0.4332",
			"36", "0.1030", "37", "0.0700", "38", "0.0868", "39", "0.1676", "40", "0.1844",
			"41", "0.3568", "42", "0.0973", "43", "0.4486", "44", "0.0853", "45", "0.3028",
			"46", "0.7200", "47", "0.5774", "48", "0.1031", "49", "0.2703", "50", "0.0528" };

	private static String[] x45_rawAP = new String[] {
			"26", "0.1355", "27", "0.1978", "28", "0.2795", "29", "0.0476", "30", "0.2459",
			"31", "0.4256", "32", "0.0286", "33", "0.4440", "34", "0.0365", "35", "0.4332",
			"36", "0.1030", "37", "0.0700", "38", "0.0868", "39", "0.1676", "40", "0.1844",
			"41", "0.3568", "42", "0.0973", "43", "0.4486", "44", "0.0853", "45", "0.3028",
			"46", "0.7200", "47", "0.5774", "48", "0.1031", "49", "0.2703", "50", "0.0528" };

	private static String[] x50_rawAP = new String[] {
			"26", "0.1355", "27", "0.1978", "28", "0.2795", "29", "0.0476", "30", "0.2459",
			"31", "0.4256", "32", "0.0286", "33", "0.4440", "34", "0.0365", "35", "0.4332",
			"36", "0.1030", "37", "0.0700", "38", "0.0868", "39", "0.1676", "40", "0.1844",
			"41", "0.3568", "42", "0.0973", "43", "0.4486", "44", "0.0853", "45", "0.3028",
			"46", "0.7200", "47", "0.5774", "48", "0.1031", "49", "0.2703", "50", "0.0528" };

	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("joint-x1.0", new GroundTruth("joint-x1.0", Metric.AP, 25, x10_rawAP, 0.2256f));
		g.put("joint-x1.5", new GroundTruth("joint-x1.5", Metric.AP, 25, x15_rawAP, 0.2256f));
		g.put("joint-x2.0", new GroundTruth("joint-x2.0", Metric.AP, 25, x20_rawAP, 0.2306f));
		g.put("joint-x2.5", new GroundTruth("joint-x2.5", Metric.AP, 25, x25_rawAP, 0.2315f));
		g.put("joint-x3.0", new GroundTruth("joint-x3.0", Metric.AP, 25, x30_rawAP, 0.2324f));
		g.put("joint-x3.5", new GroundTruth("joint-x3.5", Metric.AP, 25, x35_rawAP, 0.2348f));
		g.put("joint-x4.0", new GroundTruth("joint-x4.0", Metric.AP, 25, x40_rawAP, 0.2360f));
		g.put("joint-x4.5", new GroundTruth("joint-x4.5", Metric.AP, 25, x45_rawAP, 0.2360f));
		g.put("joint-x5.0", new GroundTruth("joint-x5.0", Metric.AP, 25, x50_rawAP, 0.2360f));

		Qrels qrels = new Qrels("data/clue/qrels.web09catB.txt");

    String[] params = new String[] {
            "data/clue/run.clue.CIKM2010.title.joint.xml",
            "data/clue/queries.web09.26-50.xml" };

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
		return new JUnit4TestAdapter(Web09catB_Title_Joint.class);
	}
}
