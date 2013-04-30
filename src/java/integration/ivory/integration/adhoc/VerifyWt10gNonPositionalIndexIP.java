package ivory.integration.adhoc;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildIndex;
import ivory.app.PreprocessCollection;
import ivory.core.eval.Qrels;
import ivory.integration.IntegrationUtils;
import ivory.regression.basic.Wt10g_NonPositional_Baselines;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.List;
import java.util.Random;

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
  private static final Random RANDOM = new Random();

  private Path collectionPath = new Path("/shared/collections/wt10g/collection.compressed.block");
  private String index = this.getClass().getCanonicalName() + "-index-" + RANDOM.nextInt(10000);

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-14"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessWt10g.class.getCanonicalName(), libjars,
        "-" + PreprocessCollection.COLLECTION_PATH, collectionPath.toString(),
        "-" + PreprocessCollection.INDEX_PATH, index };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), libjars,
        "-" + BuildIndex.NONPOSITIONAL_INDEX_IP,
        "-" + BuildIndex.INDEX_PATH, index,
        "-" + BuildIndex.INDEX_PARTITIONS, "10" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/wt10g/run.wt10g.nonpositional.baselines.xml"),
        new Path(index + "/" + "run.wt10g.nonpositional.baselines.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/wt10g/queries.wt10g.451-500.xml"),
        new Path(index + "/" + "queries.wt10g.451-500.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/wt10g/queries.wt10g.501-550.xml"),
        new Path(index + "/" + "queries.wt10g.501-550.xml"));

    String[] params = new String[] {
            index + "/run.wt10g.nonpositional.baselines.xml",
            index + "/queries.wt10g.451-500.xml",
            index + "/queries.wt10g.501-550.xml"};

    BatchQueryRunner qr = new BatchQueryRunner(params, fs, index);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    Wt10g_NonPositional_Baselines.verifyAllResults(qr.getModels(), qr.getAllResults(),
        qr.getDocnoMapping(), new Qrels("data/wt10g/qrels.wt10g.all"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWt10gNonPositionalIndexIP.class);
  }
}
