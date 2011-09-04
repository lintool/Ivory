package ivory.regression.cikm2010;

import ivory.core.eval.Qrels;
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

public class Web09catB_Title_Indep {

	private static final Logger sLogger = Logger.getLogger(Web09catB_Title_Indep.class);

	private static String[] x10_rawAP = new String[] {
			"26", "0.0014", "27", "0.1981", "28", "0.2793", "29", "0.0054", "30", "0.0345",
			"31", "0.4260", "32", "0.0131", "33", "0.2768", "34", "0.0013", "35", "0.4336",
			"36", "0.1028", "37", "0.0104", "38", "0.0000", "39", "0.0124", "40", "0.1879",
			"41", "0.1257", "42", "0.0000", "43", "0.0022", "44", "0.0004", "45", "0.0288",
			"46", "0.5721", "47", "0.0886", "48", "0.0006", "49", "0.0339", "50", "0.0000" };

	private static String[] x15_rawAP = new String[] {
			"26", "0.0017", "27", "0.1981", "28", "0.2793", "29", "0.0115", "30", "0.1859",
			"31", "0.4260", "32", "0.0141", "33", "0.4828", "34", "0.0241", "35", "0.4336",
			"36", "0.1028", "37", "0.0631", "38", "0.0916", "39", "0.1285", "40", "0.1879",
			"41", "0.0970", "42", "0.0093", "43", "0.3674", "44", "0.0004", "45", "0.2380",
			"46", "0.6659", "47", "0.3542", "48", "0.1428", "49", "0.2120", "50", "0.0722" };

	private static String[] x20_rawAP = new String[] {
			"26", "0.1390", "27", "0.1978", "28", "0.2795", "29", "0.0269", "30", "0.1962",
			"31", "0.4256", "32", "0.0674", "33", "0.4774", "34", "0.0243", "35", "0.4332",
			"36", "0.1030", "37", "0.0635", "38", "0.0908", "39", "0.1374", "40", "0.1844",
			"41", "0.1918", "42", "0.0115", "43", "0.3689", "44", "0.0427", "45", "0.2427",
			"46", "0.6926", "47", "0.3754", "48", "0.1452", "49", "0.2549", "50", "0.0716" };

	private static String[] x25_rawAP = new String[] {
			"26", "0.1481", "27", "0.1978", "28", "0.2795", "29", "0.0320", "30", "0.1962",
			"31", "0.4256", "32", "0.0658", "33", "0.4774", "34", "0.0243", "35", "0.4332",
			"36", "0.1030", "37", "0.0635", "38", "0.0908", "39", "0.1374", "40", "0.1844",
			"41", "0.2282", "42", "0.0115", "43", "0.3689", "44", "0.0444", "45", "0.2427",
			"46", "0.6926", "47", "0.3754", "48", "0.1452", "49", "0.2549", "50", "0.0716" };

	private static String[] x30_rawAP = new String[] {
			"26", "0.1481", "27", "0.1978", "28", "0.2795", "29", "0.0320", "30", "0.2020",
			"31", "0.4256", "32", "0.0658", "33", "0.4775", "34", "0.0243", "35", "0.4332",
			"36", "0.1030", "37", "0.0631", "38", "0.0968", "39", "0.1429", "40", "0.1844",
			"41", "0.2793", "42", "0.0223", "43", "0.3724", "44", "0.0444", "45", "0.2434",
			"46", "0.6883", "47", "0.3818", "48", "0.1537", "49", "0.2600", "50", "0.0753" };

	private static String[] x35_rawAP = new String[] {
			"26", "0.1093", "27", "0.1978", "28", "0.2795", "29", "0.0408", "30", "0.2020",
			"31", "0.4256", "32", "0.0455", "33", "0.4775", "34", "0.0243", "35", "0.4332",
			"36", "0.1030", "37", "0.0631", "38", "0.0968", "39", "0.1429", "40", "0.1844",
			"41", "0.2794", "42", "0.0223", "43", "0.3724", "44", "0.0786", "45", "0.2434",
			"46", "0.6883", "47", "0.3818", "48", "0.1537", "49", "0.2600", "50", "0.0753" };

	private static String[] x40_rawAP = new String[] {
			"26", "0.1259", "27", "0.1978", "28", "0.2795", "29", "0.0500", "30", "0.2237",
			"31", "0.4256", "32", "0.0466", "33", "0.4695", "34", "0.0282", "35", "0.4332",
			"36", "0.1030", "37", "0.0635", "38", "0.0918", "39", "0.1534", "40", "0.1844",
			"41", "0.2793", "42", "0.0739", "43", "0.4324", "44", "0.0789", "45", "0.2607",
			"46", "0.6937", "47", "0.4021", "48", "0.1198", "49", "0.2606", "50", "0.0665" };

	private static String[] x45_rawAP = new String[] {
			"26", "0.1799", "27", "0.1978", "28", "0.2795", "29", "0.0601", "30", "0.2237",
			"31", "0.4256", "32", "0.0525", "33", "0.4695", "34", "0.0282", "35", "0.4332",
			"36", "0.1030", "37", "0.0635", "38", "0.0918", "39", "0.1534", "40", "0.1844",
			"41", "0.2874", "42", "0.0739", "43", "0.4324", "44", "0.0732", "45", "0.2607",
			"46", "0.6967", "47", "0.4021", "48", "0.1198", "49", "0.2606", "50", "0.0665" };

	private static String[] x50_rawAP = new String[] {
			"26", "0.1799", "27", "0.1978", "28", "0.2795", "29", "0.0601", "30", "0.2237",
			"31", "0.4256", "32", "0.0525", "33", "0.4695", "34", "0.0282", "35", "0.4332",
			"36", "0.1030", "37", "0.0635", "38", "0.0918", "39", "0.1534", "40", "0.1844",
			"41", "0.2942", "42", "0.0739", "43", "0.4324", "44", "0.0732", "45", "0.2607",
			"46", "0.6967", "47", "0.4021", "48", "0.1198", "49", "0.2606", "50", "0.0665" };

	@Test
	public void runRegression() throws Exception {
		Map<String, GroundTruth> g = new HashMap<String, GroundTruth>();

		g.put("indep-x1.0", new GroundTruth("indep-x1.0", Metric.AP, 25, x10_rawAP, 0.1134f));
		g.put("indep-x1.5", new GroundTruth("indep-x1.5", Metric.AP, 25, x15_rawAP, 0.1916f));
		g.put("indep-x2.0", new GroundTruth("indep-x2.0", Metric.AP, 25, x20_rawAP, 0.2097f));
		g.put("indep-x2.5", new GroundTruth("indep-x2.5", Metric.AP, 25, x25_rawAP, 0.2118f));
		g.put("indep-x3.0", new GroundTruth("indep-x3.0", Metric.AP, 25, x30_rawAP, 0.2159f));
		g.put("indep-x3.5", new GroundTruth("indep-x3.5", Metric.AP, 25, x35_rawAP, 0.2152f));
		g.put("indep-x4.0", new GroundTruth("indep-x4.0", Metric.AP, 25, x40_rawAP, 0.2218f));
		g.put("indep-x4.5", new GroundTruth("indep-x4.5", Metric.AP, 25, x45_rawAP, 0.2248f));
		g.put("indep-x5.0", new GroundTruth("indep-x5.0", Metric.AP, 25, x50_rawAP, 0.2250f));

		Qrels qrels = new Qrels("data/clue/qrels.web09catB.txt");

		String[] params = new String[] {
		    "data/clue/run.clue.CIKM2010.title.indep.xml",
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
		return new JUnit4TestAdapter(Web09catB_Title_Indep.class);
	}
}
