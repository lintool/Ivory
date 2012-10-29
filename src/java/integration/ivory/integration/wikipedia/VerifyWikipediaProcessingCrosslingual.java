package ivory.integration.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.core.driver.PreprocessWikipedia;
import ivory.integration.IntegrationUtils;

import java.util.List;
import java.util.Map;
import java.util.Random;

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
  private static final Random rand = new Random();
  private static final String tmp = "tmp-" + VerifyWikipediaProcessingCrosslingual.class.getSimpleName() + rand.nextInt(10000);

  private static final String vocabPath = tmp + "/vocab";
  private static final String enwikiPath = 
    "/shared/collections/wikipedia/raw/enwiki-20110115-pages-articles.xml";
  private static final String enwikiRepacked = tmp + "/enwiki-20110115.repacked";
  private static final String enwikiEn = tmp + "/enwiki.en";

  // en side: part 00000, key = 92101
  private ImmutableMap<String, Float> enTermDocVector1 = ImmutableMap.of(
      "total", 0.053489465f, "extern", 0.0047085993f, "side", 0.051586498f, "refer", -0.013051073f);

  // en side: part 00010, key = 138960
  private ImmutableMap<String, Float> enTermDocVector2 = ImmutableMap.of(
      "extern", 0.004598105f, "perspect", 0.09756525f, "deal", 0.07042515f, "devianc", 0.18643877f);

  // en side: part 00002, key = 150251
  private ImmutableMap<Integer, Float> enIntDocVector1 =
    ImmutableMap.of(42846, 0.059367422f, 1518, 0.038326096f, 307, 0.04928433f, 63715, 0.037885573f);

  // en side: part 00011, key = 184192
  private ImmutableMap<Integer, Float> enIntDocVector2 =
    ImmutableMap.of(19446, 0.098102935f, 175, 0.05113173f, 31837, 0.18484645f, 4936, -0.016991744f);

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20110131-pages-articles.xml";
  private static final String dewikiRepacked = tmp + "/dewiki-20110131.repacked";
  private static final String dewikiEn = tmp + "/dewiki.en";

  // de side: part 00000, key = 1001242228
  private ImmutableMap<String, Float> deTermDocVector1 = ImmutableMap.of(
      "everyon", 0.19224185f, "1938", 0.29180187f, "wealthiest", 0.09108173f, "anoth", 0.1812021f);

  // de side: part 00010, key = 1000034130
  private ImmutableMap<String, Float> deTermDocVector2 = ImmutableMap.of(
      "de", 0.050196763f, "mechan", 0.12113027f, "kinet", 0.15653715f, "great", 0.0015684455f);

  // de side: part 00002, key = 1000943946
  private ImmutableMap<Integer, Float> deIntDocVector1 =
    ImmutableMap.of(15320, 0.36188218f, 1009, 0.12278663f, 3985, 0.4062647f, 5164, 0.012887947f);

  // de side: part 00011, key = 1000347854
  private ImmutableMap<Integer, Float> deIntDocVector2 =
    ImmutableMap.of(51742, 0.2103673f, 2308, 0.08852731f, 9996, 0.01679619f, 1286, 0.0069967806f);

  @Test
  public void runBuildIndexEnSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(enwikiPath)));

    fs.delete(new Path(enwikiEn), true);
    fs.delete(new Path(enwikiRepacked), true);
    fs.delete(new Path(vocabPath), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab"), new Path(vocabPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-index="+enwikiEn, "-xml="+enwikiPath, "-compressed="+enwikiRepacked,
        "tokenizerclass="+ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "-lang=en",
        "-tokenizermodel="+vocabPath + "/en-token.bin",	"-collectionvocab="+vocabPath + "/vocab.de-en.en", "-mode=crosslingE"});
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
    fs.delete(new Path(vocabPath), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab"), new Path(vocabPath));

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-index="+dewikiEn, "-xml="+dewikiPath, "-compressed="+dewikiRepacked,
        "tokenizerclass="+ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "-lang=de",
        "-tokenizermodel="+vocabPath + "/de-token.bin", "-e_e2f_vocab="+vocabPath + "/vocab.en-de.en",
        "-f_e2f_vocab="+vocabPath + "/vocab.en-de.de", "-f_f2e_vocab="+vocabPath + "/vocab.de-en.de",
        "-e_f2e_vocab="+vocabPath + "/vocab.de-en.en", "-f2e_ttable="+vocabPath + "/ttable.de-en",   
        "-e2f_ttable="+vocabPath + "/ttable.en-de", "-collectionvocab="+vocabPath + "/vocab.de-en.en", 
        "-mode=crosslingF", "-targetindex="+enwikiEn});
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
