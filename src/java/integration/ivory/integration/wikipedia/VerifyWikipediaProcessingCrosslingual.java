package ivory.integration.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.core.driver.PreprocessWikipedia;
import ivory.integration.IntegrationUtils;

import java.util.List;
import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.umd.cloud9.io.map.HMapSFW;

public class VerifyWikipediaProcessingCrosslingual {
  private static final String vocabPath = "vocab";
  private static final String enwikiPath = 
    "/shared/collections/wikipedia/raw/enwiki-20110115-pages-articles.xml";
  private static final String enwikiRepacked = "enwiki-20110115.repacked";
  private static final String enwikiEn = "enwiki.en";

  // en side: part 00000, key = 92101
  private ImmutableMap<String, Float> enTermDocVector1 = ImmutableMap.of(
      "total", 0.048103902f, "external", 0.004541542f, "time", 0.033348884f, "refer", -0.011754768f);

  // en side: part 00010, key = 138960
  private ImmutableMap<String, Float> enTermDocVector2 = ImmutableMap.of(
      "external", 0.004776824f, "cofound", 0.09919491f, "he", 0.023234092f, "devianc", 0.18071339f);

  // en side: part 00002, key = 150251
  private ImmutableMap<Integer, Float> enIntDocVector1 =
    ImmutableMap.of(35202, 0.034152746f, 34129, 0.054591186f, 27261, 0.039973103f, 34140, 0.08634214f);

  // en side: part 00011, key = 184192
  private ImmutableMap<Integer, Float> enIntDocVector2 =
    ImmutableMap.of(8777, 0.10271761f, 73827, 0.077015184f, 75933, -0.016551014f, 44992, 0.11264816f);

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20110131-pages-articles.xml";
  private static final String dewikiRepacked = "dewiki-20110131.repacked";
  private static final String dewikiEn = "dewiki.en";

  // de side: part 00000, key = 1001242228
  private ImmutableMap<String, Float> deTermDocVector1 = ImmutableMap.of(
      "auction", 0.00926886f, "total", 0.011755229f, "hors", 0.06490202f, "store", 0.003023784f);

  // de side: part 00010, key = 1000034130
  private ImmutableMap<String, Float> deTermDocVector2 = ImmutableMap.of(
      "portray", 0.02833135f, "profession", 0.007643698f, "asund", 0.025962f, "suitabl", 0.02116417f);

  // de side: part 00002, key = 1000943946
  private ImmutableMap<Integer, Float> deIntDocVector1 =
    ImmutableMap.of(27255, 0.034241054f, 59321, 0.19270006f, 39099, 0.08531962f, 37992, 0.006224899f);

  // de side: part 00011, key = 1000347854
  private ImmutableMap<Integer, Float> deIntDocVector2 =
    ImmutableMap.of(2110, 0.02419825f, 14287, 0.27120075f, 75805, 0.15010615f, 49109, 0.20328416f);

  @Test
  public void runBuildIndexEnSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(enwikiPath)));

    fs.delete(new Path(enwikiEn), true);
    fs.delete(new Path(enwikiRepacked), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab"), new Path(vocabPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        enwikiEn, enwikiPath, enwikiRepacked,
        ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "en",
        "vocab/en-token.bin", "vocab/vocab.en-de.en"});
  }

  @Test
  public void verifyTermDocVectorsEn() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs,
        new Path(enwikiEn + "/wt-term-doc-vectors/part-00000"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(enTermDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(enwikiEn + "/wt-term-doc-vectors/part-00010"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(enTermDocVector2, value);
  }

  @Test
  public void verifyIntDocVectorsEn() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs,
        new Path(enwikiEn + "/wt-int-doc-vectors/part-00002"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(enIntDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(enwikiEn + "/wt-int-doc-vectors/part-00011"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(enIntDocVector2, value);
  }

  @Test
  public void runBuildIndexDeSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(dewikiPath)));

    fs.delete(new Path(dewikiEn), true);
    fs.delete(new Path(dewikiRepacked), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab"), new Path(vocabPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        dewikiEn, dewikiPath, dewikiRepacked,
        ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "de",
        "vocab/de-token.bin", "vocab/vocab.de-en.de", "vocab/vocab.de-en.en", "vocab/ttable.de-en",
        "vocab/vocab.en-de.en", "vocab/vocab.en-de.de", "vocab/ttable.en-de"});
  }

  @Test
  public void verifyTermDocVectorsDe() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs,
        new Path(dewikiEn + "/wt-term-doc-vectors/part-00000"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(deTermDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(dewikiEn + "/wt-term-doc-vectors/part-00010"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(deTermDocVector2, value);
  }

  @Test
  public void verifyIntDocVectorsDe() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs,
        new Path(dewikiEn + "/wt-int-doc-vectors/part-00002"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(deIntDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(dewikiEn + "/wt-int-doc-vectors/part-00011"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(deIntDocVector2, value);
  }

  private void verifyTermDocVector(Map<String, Float> doc, HMapSFW value) {
    for (Map.Entry<String, Float> entry : doc.entrySet()) {
      assertTrue(value.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), value.get(entry.getKey()), 10e-6);
    }
  }

  private void verifyIntDocVector(Map<Integer, Float> doc, WeightedIntDocVector value) {
    for (Map.Entry<Integer, Float> entry : doc.entrySet()) {
      assertTrue(value.containsTerm(entry.getKey()));
      assertEquals(entry.getValue(), value.getWeight(entry.getKey()), 10e-6);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWikipediaProcessingCrosslingual.class);
  }
}
