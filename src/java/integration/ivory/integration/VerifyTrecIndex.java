package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.core.driver.BuildIPIndex;
import ivory.core.driver.PreprocessTREC;
import ivory.core.eval.Qrels;
import ivory.regression.basic.Robust04_Basic;
import ivory.smrf.retrieval.BatchQueryRunner;
import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

public class VerifyTrecIndex {
  private static final Logger LOG = Logger.getLogger(VerifyTrecIndex.class);

  private Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
  private String index = "/tmp/" + this.getClass().getCanonicalName() + "-index";

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);

    String cloud9Jar = IntegrationUtils.getJar("lib", "cloud9");
    String guavaJar = IntegrationUtils.getJar("lib", "guava");
    String ivoryJar = IntegrationUtils.getJar("dist", "ivory");

    String libjars = String.format("-libjars=%s,%s,%s", cloud9Jar, guavaJar, ivoryJar);

    PreprocessTREC.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        collectionPath.toString(), index });
    BuildIPIndex.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        index, "10" });
  }

  @Test
  public void verifyResults() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
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

    LOG.info("Total query time: " + (end - start) + "ms");

    Robust04_Basic.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/trec/qrels.robust04.noCRFR.txt"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyTrecIndex.class);
  }
}
