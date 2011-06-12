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

public class Web09catB_Desc_Joint {

	private static final Logger sLogger = Logger.getLogger(Web09catB_Desc_Joint.class);

	private static String[] x10_rawAP = new String[] {
		"26","0.1793","27","0.1321","28","0.1239","29","0.0000","30","0.0020",
                "31","0.2665","32","0.0030","33","0.3488","34","0.0312","35","0.3113",
                "36","0.0152","37","0.0851","38","0.0106","39","0.1725","40","0.0111",
                "41","0.1811","42","0.0000","43","0.6895","44","0.0399","45","0.0817",
                "46","0.6728","47","0.3392","48","0.3904","49","0.0271","50","0.0924"};

	private static String[] x15_rawAP = new String[] {
		"26","0.1954","27","0.1292","28","0.1036","29","0.0000","30","0.0022",
                "31","0.1841","32","0.0000","33","0.3407","34","0.0269","35","0.2725",
                "36","0.0154","37","0.1180","38","0.0120","39","0.1744","40","0.0112",
                "41","0.1894","42","0.0002","43","0.7341","44","0.0397","45","0.0904",
                "46","0.6562","47","0.1613","48","0.3769","49","0.0279","50","0.0450"};

	private static String[] x20_rawAP = new String[] {
		"26","0.2312","27","0.1060","28","0.0816","29","0.0000","30","0.0005",
                "31","0.1804","32","0.0001","33","0.3053","34","0.0227","35","0.2271",
                "36","0.0139","37","0.0853","38","0.0075","39","0.1514","40","0.0158",
                "41","0.1885","42","0.0124","43","0.7160","44","0.0238","45","0.0754",
                "46","0.6542","47","0.3714","48","0.3951","49","0.0046","50","0.0616"};

	private static String[] x25_rawAP = new String[] {
		"26","0.2062","27","0.1208","28","0.0639","29","0.0001","30","0.0004",
                "31","0.1617","32","0.0000","33","0.2468","34","0.0342","35","0.2443",
                "36","0.0058","37","0.1126","38","0.0081","39","0.1439","40","0.0165",
                "41","0.1950","42","0.0139","43","0.7499","44","0.0317","45","0.0404",
                "46","0.6790","47","0.3714","48","0.3950","49","0.0096","50","0.0433"};

	private static String[] x30_rawAP = new String[] {
		"26","0.2143","27","0.1148","28","0.0598","29","0.0000","30","0.0004",
                "31","0.1205","32","0.0000","33","0.2435","34","0.0289","35","0.3182",
                "36","0.0058","37","0.1130","38","0.0082","39","0.1491","40","0.0165",
                "41","0.1901","42","0.0117","43","0.7479","44","0.0315","45","0.0574",
                "46","0.6602","47","0.3262","48","0.4388","49","0.0124","50","0.0325"};

	private static String[] x35_rawAP = new String[] {
		"26","0.2369","27","0.1471","28","0.0597","29","0.0000","30","0.0002",
                "31","0.1017","32","0.0001","33","0.2462","34","0.0253","35","0.3026",
                "36","0.0060","37","0.0867","38","0.0088","39","0.1421","40","0.0145",
                "41","0.2138","42","0.0115","43","0.7627","44","0.0192","45","0.0569",
                "46","0.6593","47","0.2859","48","0.4784","49","0.0122","50","0.0405"};

	private static String[] x40_rawAP = new String[] {
		"26","0.2064","27","0.1286","28","0.0578","29","0.0000","30","0.0004",
                "31","0.1254","32","0.0001","33","0.2533","34","0.0200","35","0.3362",
                "36","0.0126","37","0.1143","38","0.0088","39","0.1370","40","0.0145",
                "41","0.1948","42","0.0105","43","0.7844","44","0.0361","45","0.0570",
                "46","0.6749","47","0.2805","48","0.4576","49","0.0108","50","0.0464"};

	private static String[] x45_rawAP = new String[] {
		"26","0.2006","27","0.1096","28","0.0699","29","0.0000","30","0.0004",
                "31","0.1287","32","0.0001","33","0.2316","34","0.0240","35","0.3236",
                "36","0.0071","37","0.1144","38","0.0083","39","0.1362","40","0.0144",
                "41","0.1926","42","0.0085","43","0.7856","44","0.0241","45","0.0540",
                "46","0.6751","47","0.2961","48","0.4576","49","0.0099","50","0.0447"};

	private static String[] x50_rawAP = new String[] {
		"26","0.1762","27","0.0930","28","0.0750","29","0.0000","30","0.0002",
                "31","0.0966","32","0.0001","33","0.2298","34","0.0294","35","0.3259",
                "36","0.0071","37","0.1700","38","0.0080","39","0.1331","40","0.0144",
                "41","0.1932","42","0.0088","43","0.7857","44","0.0153","45","0.0527",
                "46","0.6749","47","0.3060","48","0.4518","49","0.0097","50","0.0457"};

	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("joint-x1.0", new GroundTruth("joint-x1.0", Metric.AP, 25, x10_rawAP, 0.1683f));
		g.put("joint-x1.5", new GroundTruth("joint-x1.5", Metric.AP, 25, x15_rawAP, 0.1563f));
		g.put("joint-x2.0", new GroundTruth("joint-x2.0", Metric.AP, 25, x20_rawAP, 0.1573f));
		g.put("joint-x2.5", new GroundTruth("joint-x2.5", Metric.AP, 25, x25_rawAP, 0.1558f));
		g.put("joint-x3.0", new GroundTruth("joint-x3.0", Metric.AP, 25, x30_rawAP, 0.1561f));
		g.put("joint-x3.5", new GroundTruth("joint-x3.5", Metric.AP, 25, x35_rawAP, 0.1567f));
		g.put("joint-x4.0", new GroundTruth("joint-x4.0", Metric.AP, 25, x40_rawAP, 0.1587f));
		g.put("joint-x4.5", new GroundTruth("joint-x4.5", Metric.AP, 25, x45_rawAP, 0.1567f));
		g.put("joint-x5.0", new GroundTruth("joint-x5.0", Metric.AP, 25, x50_rawAP, 0.1561f));

    Qrels qrels = new Qrels("data/clue/qrels.web09catB.txt");

    String[] params = new String[] {
            "data/clue/run.clue.CIKM2010.desc.joint.xml",
            "data/clue/queries.web09.26-50.desc.xml" };

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
		return new JUnit4TestAdapter(Web09catB_Desc_Joint.class);
	}
}
