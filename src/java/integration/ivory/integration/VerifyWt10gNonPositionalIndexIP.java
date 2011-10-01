package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.core.driver.BuildNonPositionalIndexIP;
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

public class VerifyWt10gNonPositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyWt10gNonPositionalIndexIP.class);

  private Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
  private String index = "/tmp/" + this.getClass().getCanonicalName() + "-index";

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

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

    PreprocessTREC.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        collectionPath.toString(), index });
    BuildNonPositionalIndexIP.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN, index, "10" });
  }

  @Test
  public void verifyResults() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    fs.copyFromLocalFile(false, true,
        new Path("data/trec/run.robust04.nonpositional.baselines.xml"),
        new Path(index + "/run.robust04.nonpositional.baselines.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/trec/queries.robust04.xml"),
        new Path(index + "/" + "queries.robust04.xml"));

    String[] params = new String[] {
            index + "/run.robust04.nonpositional.baselines.xml",
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
    return new JUnit4TestAdapter(VerifyWt10gNonPositionalIndexIP.class);
  }
}
