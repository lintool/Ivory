package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildIndex;
import ivory.app.PreprocessCollection;
import ivory.app.PreprocessTrec45;
import ivory.core.eval.Qrels;
import ivory.regression.basic.Robust04_Basic;
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

public class VerifyTrecNonPositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyTrecNonPositionalIndexIP.class);
  private static final Random RANDOM = new Random();

  private Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
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
    jars.add(IntegrationUtils.getJar("lib", "guava-r09"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessTrec45.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + PreprocessCollection.COLLECTION_PATH, collectionPath.toString(),
        "-" + PreprocessCollection.INDEX_PATH, index });
    BuildIndex.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + BuildIndex.NONPOSITIONAL_INDEX_IP,
        "-" + BuildIndex.INDEX_PATH, index,
        "-" + BuildIndex.INDEX_PARTITIONS, "10" });

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true,
        new Path("data/trec/run.robust04.nonpositional.baselines.xml"),
        new Path(index + "/run.robust04.nonpositional.baselines.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/trec/queries.robust04.xml"),
        new Path(index + "/queries.robust04.xml"));

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
    return new JUnit4TestAdapter(VerifyTrecNonPositionalIndexIP.class);
  }
}
