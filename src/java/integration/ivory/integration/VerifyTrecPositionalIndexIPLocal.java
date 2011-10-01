package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.core.driver.BuildPositionalIndexIP;
import ivory.core.driver.PreprocessTREC;
import ivory.core.eval.Qrels;
import ivory.regression.basic.Robust04_Basic;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class VerifyTrecPositionalIndexIPLocal {
  private static final Logger LOG = Logger.getLogger(VerifyTrecPositionalIndexIPLocal.class);

  private Path collectionPath = new Path("/scratch0/collections/trec/trec4-5_noCRFR.xml");
  private String index = this.getClass().getCanonicalName() + "-index";

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessTREC.main(new String[] { libjars, IntegrationUtils.D_JT_LOCAL,
        IntegrationUtils.D_NN_LOCAL, collectionPath.toString(), index });
    BuildPositionalIndexIP.main(new String[] { libjars, IntegrationUtils.D_JT_LOCAL,
        IntegrationUtils.D_NN_LOCAL, index, "10" });
  }

  @Test
  public void verifyResults() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    String[] params = new String[] {
        "data/trec/run.robust04.basic.xml",
        "data/trec/queries.robust04.xml" };

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
    return new JUnit4TestAdapter(VerifyTrecPositionalIndexIPLocal.class);
  }
}
