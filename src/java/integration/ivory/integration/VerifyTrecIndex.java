package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.core.driver.BuildIPIndex;
import ivory.core.driver.PreprocessTREC;

import java.io.File;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

public class VerifyTrecIndex {
	private static final Logger sLogger = Logger.getLogger(VerifyTrecIndex.class);

  private Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
  private String index = "/tmp/" + this.getClass().getCanonicalName() + "-index";

	private static Qrels sQrels;
	private static DocnoMapping sMapping;

    @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "bespin00.umiacs.umd.edu:8021");
    conf.set("fs.default.name", "hdfs://bespin00.umiacs.umd.edu:8020");

    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    FileSystem.get(conf).delete(new Path(index), true);

    String cloud9Jar = IntegrationUtils.getJar("lib", "cloud9");
    String guavaJar = IntegrationUtils.getJar("lib", "guava");
    String ivoryJar = IntegrationUtils.getJar("dist", "ivory");

    String libjars = String.format("-libjars=%s,%s,%s", cloud9Jar, guavaJar, ivoryJar);
    System.out.println(libjars);

    PreprocessTREC.main(new String[] { libjars, "-Dmapred.job.tracker=bespin00.umiacs.umd.edu:8021",
        "-Dfs.default.name=hdfs://bespin00.umiacs.umd.edu:8020", collectionPath.toString(), index });
    BuildIPIndex.main(new String[] { libjars, "-Dmapred.job.tracker=bespin00.umiacs.umd.edu:8021",
        "-Dfs.default.name=hdfs://bespin00.umiacs.umd.edu:8020", index, "10" });
  }

  @Test
  public void verifyResults() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "bespin00.umiacs.umd.edu:8021");
    conf.set("fs.default.name", "hdfs://bespin00.umiacs.umd.edu:8020");

		sQrels = new Qrels("data/trec/qrels.robust04.noCRFR.txt");
		FileSystem fs = FileSystem.get(conf);

		fs.copyFromLocalFile(false, true, new Path("data/trec/run.robust04.basic.xml"), 
				     new Path(index + "/" + "run.robust04.basic.xml"));
		fs.copyFromLocalFile(false, true, new Path("data/trec/queries.robust04.xml"), 
				     new Path(index + "/" + "queries.robust04.xml"));
		
		String[] params = new String[] {
		    index + "/run.robust04.basic.xml",
		    index + "/queries.robust04.xml" };

		BatchQueryRunner qr = new BatchQueryRunner(params, fs, index);

		long start = System.currentTimeMillis();
		qr.runQueries();
		long end = System.currentTimeMillis();

		sLogger.info("Total query time: " + (end - start) + "ms");

		sMapping = qr.getDocnoMapping();

		for (String model : qr.getModels()) {
			sLogger.info("Verifying results of model \"" + model + "\"");

			Map<String, Accumulator[]> results = qr.getResults(model);

			//verifyResults(model, results, AllModelsAPScores.get(model), AllModelsP10Scores.get(model));

			sLogger.info("Done!");
		}
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyTrecIndex.class);
  }
}
