package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.core.driver.BuildIPIndex;
import ivory.core.driver.PreprocessGov2;
import ivory.core.eval.Qrels;
import ivory.regression.basic.Gov2_Basic;
import ivory.smrf.retrieval.BatchQueryRunner;
import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

public class VerifyGov2Index {
  private static final Logger LOG = Logger.getLogger(VerifyGov2Index.class);

  private Path collectionPath = new Path("/shared/collections/gov2/collection.compressed.block");
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

    PreprocessGov2.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        collectionPath.toString(), index });
    BuildIPIndex.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        index, "100" });
  }

  @Test
  public void verifyResults() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    fs.copyFromLocalFile(false, true, new Path("data/gov2/run.gov2.basic.xml"),
        new Path(index + "/" + "run.gov2.basic.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/gov2/gov2.title.701-775"),
        new Path(index + "/" + "gov2.title.701-775"));
    fs.copyFromLocalFile(false, true, new Path("data/gov2/gov2.title.776-850"),
        new Path(index + "/" + "gov2.title.776-850"));

    String[] params = new String[] {
            index + "/run.gov2.basic.xml",
            index + "/gov2.title.701-775",
            index + "/gov2.title.776-850"};

    BatchQueryRunner qr = new BatchQueryRunner(params, fs, index);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    Gov2_Basic.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/gov2/qrels.gov2.all"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyGov2Index.class);
  }
}
