package ivory.integration.adhoc;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildIndex;
import ivory.app.PreprocessCollection;
import ivory.core.eval.Qrels;
import ivory.integration.IntegrationUtils;
import ivory.regression.basic.Gov2_Basic;
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

public class VerifyGov2PositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyGov2PositionalIndexIP.class);
  private static final Random RANDOM = new Random();

  private Path collectionPath = new Path("/shared/collections/gov2/collection.compressed.block");
  private String index = this.getClass().getCanonicalName() + "-index-" + RANDOM.nextInt(10000);

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "kamikaze"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessGov2.class.getCanonicalName(), libjars,
        "-" + PreprocessCollection.COLLECTION_PATH, collectionPath.toString(),
        "-" + PreprocessCollection.INDEX_PATH, index };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), libjars,
        "-" + BuildIndex.POSITIONAL_INDEX_IP,
        "-" + BuildIndex.INDEX_PATH, index,
        "-" + BuildIndex.INDEX_PARTITIONS, "100" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/gov2/run.gov2.basic.xml"),
        new Path(index + "/" + "run.gov2.basic.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/gov2/queries.gov2.title.701-775.xml"),
        new Path(index + "/" + "queries.gov2.title.701-775.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/gov2/queries.gov2.title.776-850.xml"),
        new Path(index + "/" + "queries.gov2.title.776-850.xml"));

    String[] params = new String[] {
            index + "/run.gov2.basic.xml",
            index + "/queries.gov2.title.701-775.xml",
            index + "/queries.gov2.title.776-850.xml"};

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
    return new JUnit4TestAdapter(VerifyGov2PositionalIndexIP.class);
  }
}
