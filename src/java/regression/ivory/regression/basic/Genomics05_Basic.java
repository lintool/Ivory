package ivory.regression.basic;

import ivory.core.eval.Qrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.Map;
import java.util.Set;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class Genomics05_Basic {
    private static final Logger LOG = Logger.getLogger(Genomics05_Basic.class);

    private static String[] sDirBaseRawAP = new String[] {
"100", "0.2060",
"101", "0.0844",
"102", "0.0127",
"103", "0.1192",
"104", "0.1280",
"105", "0.1947",
"106", "0.0232",
"107", "0.5105",
"108", "0.0988",
"109", "0.7033",
"110", "0.0053",
"111", "0.3166",
"112", "0.3427",
"113", "0.5873",
"114", "0.4534",
"115", "0.0005",
"116", "0.0069",
"117", "0.5059",
"118", "0.1432",
"119", "0.7091",
"120", "0.5367",
"121", "0.7389",
"122", "0.1332",
"123", "0.0128",
"124", "0.1628",
"125", "0.0003",
"126", "0.2265",
"127", "0.0248",
"128", "0.1591",
"129", "0.0883",
"130", "0.6528",
"131", "0.7245",
"132", "0.1072",
"133", "0.0193",
"134", "0.2130",
"135", "0.0000",
"136", "0.0014",
"137", "0.0560",
"138", "0.2025",
"139", "0.5158",
"140", "0.4106",
"141", "0.3442",
"142", "0.5229",
"143", "0.0020",
"144", "0.1000",
"145", "0.3619",
"146", "0.6649",
"147", "0.0112",
"148", "0.0395",
"149", "0.0370",
};

    private static String[] sDirBaseRawP10 = new String[] {
"100", "0.5000",
"101", "0.2000",
"102", "0.0000",
"103", "0.2000",
"104", "0.1000",
"105", "0.8000",
"106", "0.3000",
"107", "1.0000",
"108", "0.5000",
"109", "0.9000",
"110", "0.0000",
"111", "0.6000",
"112", "0.5000",
"113", "0.6000",
"114", "0.9000",
"115", "0.0000",
"116", "0.0000",
"117", "0.9000",
"118", "0.5000",
"119", "1.0000",
"120", "1.0000",
"121", "0.9000",
"122", "0.5000",
"123", "0.0000",
"124", "0.6000",
"125", "0.0000",
"126", "0.5000",
"127", "0.0000",
"128", "0.6000",
"129", "0.1000",
"130", "1.0000",
"131", "0.9000",
"132", "0.2000",
"133", "0.0000",
"134", "0.3000",
"135", "0.0000",
"136", "0.0000",
"137", "0.2000",
"138", "0.2000",
"139", "0.9000",
"140", "0.5000",
"141", "0.6000",
"142", "0.8000",
"143", "0.0000",
"144", "0.1000",
"145", "0.7000",
"146", "0.9000",
"147", "0.0000",
"148", "0.0000",
"149", "0.2000",
};

    private static String[] sBm25BaseRawAP = new String[] {
"100", "0.1532",
"101", "0.1097",
"102", "0.0092",
"103", "0.0700",
"104", "0.0739",
"105", "0.1705",
"106", "0.0328",
"107", "0.4942",
"108", "0.1332",
"109", "0.7034",
"110", "0.0228",
"111", "0.3283",
"112", "0.3506",
"113", "0.6508",
"114", "0.4667",
"115", "0.0005",
"116", "0.0278",
"117", "0.4671",
"118", "0.3668",
"119", "0.7016",
"120", "0.5815",
"121", "0.6925",
"122", "0.1555",
"123", "0.0205",
"124", "0.1187",
"125", "0.0000",
"126", "0.1128",
"127", "0.0143",
"128", "0.1410",
"129", "0.1058",
"130", "0.6072",
"131", "0.6856",
"132", "0.1052",
"133", "0.0259",
"134", "0.2198",
"135", "0.0000",
"136", "0.0015",
"137", "0.0414",
"138", "0.1972",
"139", "0.4548",
"140", "0.4500",
"141", "0.3095",
"142", "0.5366",
"143", "0.0014",
"144", "0.5000",
"145", "0.4002",
"146", "0.6623",
"147", "0.0134",
"148", "0.0613",
"149", "0.0321",
};

    private static String[] sBm25BaseRawP10 = new String[] {
"100", "0.4000",
"101", "0.2000",
"102", "0.0000",
"103", "0.2000",
"104", "0.1000",
"105", "0.6000",
"106", "0.3000",
"107", "1.0000",
"108", "0.6000",
"109", "0.9000",
"110", "0.1000",
"111", "0.7000",
"112", "0.4000",
"113", "0.6000",
"114", "1.0000",
"115", "0.0000",
"116", "0.0000",
"117", "1.0000",
"118", "0.8000",
"119", "1.0000",
"120", "1.0000",
"121", "0.8000",
"122", "0.6000",
"123", "0.0000",
"124", "0.4000",
"125", "0.0000",
"126", "0.4000",
"127", "0.0000",
"128", "0.5000",
"129", "0.3000",
"130", "0.9000",
"131", "0.9000",
"132", "0.3000",
"133", "0.1000",
"134", "0.3000",
"135", "0.0000",
"136", "0.0000",
"137", "0.2000",
"138", "0.2000",
"139", "0.7000",
"140", "0.6000",
"141", "0.5000",
"142", "0.8000",
"143", "0.0000",
"144", "0.1000",
"145", "0.7000",
"146", "1.0000",
"147", "0.0000",
"148", "0.0000",
"149", "0.1000",
};

  @Test
      public void runRegression() throws Exception {
      String[] params = new String[] {
	  "data/medline/run.genomics05.xml",
	  "data/medline/queries.genomics05.xml"};

      FileSystem fs = FileSystem.getLocal(new Configuration());

      BatchQueryRunner qr = new BatchQueryRunner(params, fs);

      long start = System.currentTimeMillis();
      qr.runQueries();
      long end = System.currentTimeMillis();

      LOG.info("Total query time: " + (end - start) + "ms");

      verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
		       new Qrels("data/medline/qrels.genomics05.txt"));
  }

    public static void verifyAllResults(Set<String> models,
					Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

	Map<String, GroundTruth> g = Maps.newHashMap();
	// One topic didn't contain qrels, so trec_eval only picked up 49.
	g.put("genomics05-dir-base", new GroundTruth(Metric.AP, 49, sDirBaseRawAP, 0.2494f));
	g.put("genomics05-bm25-base", new GroundTruth(Metric.AP, 49, sBm25BaseRawAP, 0.2568f));

	Map<String, GroundTruth> h = Maps.newHashMap();
	// One topic didn't contain qrels, so trec_eval only picked up 49.
	h.put("genomics05-dir-base", new GroundTruth(Metric.P10, 49, sDirBaseRawP10, 0.4327f));
	h.put("genomics05-bm25-base", new GroundTruth(Metric.P10, 49, sBm25BaseRawP10, 0.4347f));

	for (String model : models) {
	    LOG.info("Verifying results of model \"" + model + "\"");

	    Map<String, Accumulator[]> r = results.get(model);
	    g.get(model).verify(r, mapping, qrels);
	    h.get(model).verify(r, mapping, qrels);

	    LOG.info("Done!");
	}
    }

    public static junit.framework.Test suite() {
	return new JUnit4TestAdapter(Genomics05_Basic.class);
    }
}
