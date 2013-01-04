package ivory.integration.shard;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildIndex;
import ivory.app.PreprocessCollection;
import ivory.integration.IntegrationUtils;

import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class VerifyGenomics05ShardedIndex {
  private static final Logger LOG = Logger.getLogger(VerifyGenomics05ShardedIndex.class);
  private static final Random RANDOM = new Random();

  private static final Map<String, String> collectionPaths = 
      new ImmutableMap.Builder<String, String>()
      .put("A", "/shared/collections/medline04/2004_TREC_XML_MEDLINE_A")
      .put("B", "/shared/collections/medline04/2004_TREC_XML_MEDLINE_B")
      .put("C", "/shared/collections/medline04/2004_TREC_XML_MEDLINE_C")
      .put("D", "/shared/collections/medline04/2004_TREC_XML_MEDLINE_D")
      .put("E", "/shared/collections/medline04/2004_TREC_XML_MEDLINE_E")
      .build();
  
  private static final String TMP =
      VerifyGenomics05ShardedIndex.class.getCanonicalName() + "-index-" + RANDOM.nextInt(10000);

  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    for (String p : collectionPaths.values()) {
      assertTrue(fs.exists(new Path(p)));
    }

    for (String s : collectionPaths.keySet()) {
      fs.delete(new Path(TMP + "-" + s), true);
    }

    for (String s : collectionPaths.keySet()) {
      LOG.info("Building  " + collectionPaths.get(s));
      buildPartition(collectionPaths.get(s), TMP + "-" + s);
    }
  }

  private void buildPartition(String collectionPath, String indexPath) throws Exception {
    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "kamikaze"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessMedline.class.getCanonicalName(), libjars,
        "-" + PreprocessCollection.COLLECTION_PATH, collectionPath,
        "-" + PreprocessCollection.INDEX_PATH, indexPath };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), libjars,
        "-" + BuildIndex.POSITIONAL_INDEX_IP,
        "-" + BuildIndex.INDEX_PATH, indexPath,
        "-" + BuildIndex.INDEX_PARTITIONS, "10" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyGenomics05ShardedIndex.class);
  }
}
