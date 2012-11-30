package ivory.integration.adhoc;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildIndex;
import ivory.app.PreprocessClueWebEnglish;
import ivory.core.eval.Qrels;
import ivory.integration.IntegrationUtils;
import ivory.regression.basic.Web09catB_All;
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

public class VerifyCluePositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyCluePositionalIndexIP.class);

  private Path collectionPath =
      new Path("/shared/collections/ClueWeb09/collection.compressed.block/en.01");
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

    PreprocessClueWebEnglish.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN, collectionPath.toString(), index, "1" });
    BuildIndex.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-" + BuildIndex.POSITIONAL_INDEX_IP,
        "-" + BuildIndex.INDEX_PATH, index,
        "-" + BuildIndex.INDEX_PARTITIONS, "200" });

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/clue/run.web09catB.all.xml"),
        new Path(index + "/" + "run.web09catB.all.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/clue/queries.web09.xml"),
        new Path(index + "/" + "queries.web09.xml"));

    String[] params = new String[] {
            index + "/run.web09catB.all.xml",
            index + "/queries.web09.xml" };

    BatchQueryRunner qr = new BatchQueryRunner(params, fs, index);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    Web09catB_All.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/clue/qrels.web09catB.txt"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyCluePositionalIndexIP.class);
  }
}
