package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildPositionalIndexIP;
import ivory.app.PreprocessWt10g;
import ivory.core.eval.Qrels;
import ivory.regression.basic.Wt10g_Basic;
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

public class VerifyWt10gPositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyWt10gPositionalIndexIP.class);

  private Path collectionPath = new Path("/shared/collections/wt10g/collection.compressed.block");
  private String index = "/tmp/" + this.getClass().getCanonicalName() + "-index";

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "kamikaze"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWt10g.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        collectionPath.toString(), index });
    BuildPositionalIndexIP.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        index, "10" });

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/wt10g/run.wt10g.basic.xml"),
        new Path(index + "/" + "run.wt10g.basic.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/wt10g/queries.wt10g.451-500.xml"),
        new Path(index + "/" + "queries.wt10g.451-500.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/wt10g/queries.wt10g.501-550.xml"),
        new Path(index + "/" + "queries.wt10g.501-550.xml"));

    String[] params = new String[] {
            index + "/run.wt10g.basic.xml",
            index + "/queries.wt10g.451-500.xml",
            index + "/queries.wt10g.501-550.xml"};

    BatchQueryRunner qr = new BatchQueryRunner(params, fs, index);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    Wt10g_Basic.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/wt10g/qrels.wt10g.all"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWt10gPositionalIndexIP.class);
  }
}
