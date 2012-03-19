package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildNonPositionalIndexIP;
import ivory.app.PreprocessClueWebEnglish;
import ivory.core.eval.Qrels;
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

public class VerifyClueNonPositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyClueNonPositionalIndexIP.class);

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
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    fs.copyFromLocalFile(false, true, new Path("data/clue/docno-mapping.dat"),
        new Path(index + "/" + "docno-mapping.dat"));

    PreprocessClueWebEnglish.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN, collectionPath.toString(), index, "1" });
    BuildNonPositionalIndexIP.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN, index, "200" });

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true,
        new Path("data/clue/run.web09catB.nonpositional.baselines.xml"),
        new Path(index + "/run.web09catB.nonpositional.baselines.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/clue/queries.web09.xml"),
        new Path(index + "/queries.web09.xml"));

    String[] params = new String[] {
            index + "/run.web09catB.nonpositional.baselines.xml",
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
    return new JUnit4TestAdapter(VerifyClueNonPositionalIndexIP.class);
  }
}
