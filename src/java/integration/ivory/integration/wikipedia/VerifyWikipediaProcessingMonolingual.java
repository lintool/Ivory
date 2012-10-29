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

public class VerifyWikipediaProcessingMonolingual {
  private static final Random rand = new Random();
  private static final String tmp = "tmp-" + VerifyWikipediaProcessingMonolingual.class.getSimpleName() + rand.nextInt(10000);

  private static final String collectionPath = 
    "/shared/collections/wikipedia/raw/enwiki-20110115-pages-articles.xml";
  private static final String collectionRepacked = tmp + "/enwiki-20110115.repacked";
  private static final String galagoIndex = tmp + "/enwiki.galago";

  // Galago: part 00000, key = 92101
  private ImmutableMap<String, Float> galagoTermDocVector1 = ImmutableMap.of(
      "2004", 0.039252512f, "replica", 0.11475211f, "refer", -0.014842824f, "first", 0.017736943f);

  // Galago: part 00010, key = 34222
  private ImmutableMap<String, Float> galagoTermDocVector2 = ImmutableMap.of(
      "brigadi", 0.026055908f, "elvii", 0.034582853f, "film", 0.024715992f, "cultur", 0.017777082f);

  // Galago: part 00002, key = 100984
  private ImmutableMap<Integer, Float> galagoIntDocVector1 =
    ImmutableMap.of(5, 0.017312722f, 1618, 0.07800972f, 7979, 0.12044795f, 3685, 0.12459397f);

  // Galago: part 00011, key = 138960
  private ImmutableMap<Integer, Float> galagoIntDocVector2 =
    ImmutableMap.of(2, 0.0062202225f, 526, 0.109307125f, 2442, 0.16429637f, 421, 0.10314735f);

  private static final String opennlpIndex = tmp + "/enwiki.opennlp";
  private static final String vocabPath = tmp + "/vocab";

  // Opennlp: part 00000, key = 92101
  private ImmutableMap<String, Float> opennlpTermDocVector1 = ImmutableMap.of(
      "extern", 0.0047085057f, "zero", 0.102504365f, "theorem", 0.12850066f, "prime", 0.082574375f);

  // Opennlp: part 00010, key = 34222
  private ImmutableMap<String, Float> opennlpTermDocVector2 = ImmutableMap.of(
      "cycl", 0.090582855f, "scholar", 0.08342686f, "229", 0.13324969f, "opinion", 0.09194601f);

  // Opennlp: part 00002, key = 100984
  private ImmutableMap<Integer, Float> opennlpIntDocVector1 =
    ImmutableMap.of(1, -0.0034231273f, 12, 0.022128861f, 102, 0.008630077f, 5, 0.01352463f);

  // Opennlp: part 00011, key = 34222, (terms: conjunto, histori, film, cultur)
  private ImmutableMap<Integer, Float> opennlpIntDocVector2 =
    ImmutableMap.of(3677, 0.048551466f, 14080, 0.07085314f, 260, 0.024108753f, 2365, 0.09521676f);

  @Test
  public void runBuildIndexGalago() throws Exception {

    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(galagoIndex), true);
    fs.delete(new Path(collectionRepacked), true);
    fs.delete(new Path(vocabPath), true);

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
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-mode=mono", "-index="+galagoIndex, "-xml="+collectionPath, "-compressed="+collectionRepacked,
        "-tokenizerclass="+ivory.core.tokenize.GalagoTokenizer.class.getCanonicalName(), "-lang=en",
        "-tokenizermodel="+vocabPath + "/en-token.bin"});

  }

  @Test
  public void verifyTermDocVectorsGalago() throws Exception {
    System.out.println("verifyTermDocVectorsGalago");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs,
        new Path(galagoIndex + "/wt-term-doc-vectors/part-00000"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(galagoTermDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(galagoIndex + "/wt-term-doc-vectors/part-00010"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(galagoTermDocVector2, value);
  }

  @Test
  public void verifyIntDocVectorsGalago() throws Exception {
    System.out.println("verifyIntDocVectorsGalago");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs,
        new Path(galagoIndex + "/wt-int-doc-vectors/part-00002"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(galagoIntDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(galagoIndex + "/wt-int-doc-vectors/part-00011"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(galagoIntDocVector2, value);
  }

  @Test
  public void runBuildIndexOpennlp() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(opennlpIndex), true);
    fs.delete(new Path(collectionRepacked), true);
    fs.delete(new Path(vocabPath), true);

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
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-mode=mono", "-index="+opennlpIndex, "-xml="+collectionPath, "-compressed="+collectionRepacked,
        "-tokenizerclass="+ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "-lang=en",
        "-tokenizermodel="+vocabPath + "/en-token.bin", "-collectionvocab="+vocabPath + "/vocab.de-en.en"});
  }

  @Test
  public void verifyTermDocVectorsOpennlp() throws Exception {
    System.out.println("verifyTermDocVectorsOpennlp");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs,
        new Path(opennlpIndex + "/wt-term-doc-vectors/part-00000"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(opennlpTermDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(opennlpIndex + "/wt-term-doc-vectors/part-00010"), fs.getConf());
    reader.next(key, value);
    verifyTermDocVector(opennlpTermDocVector2, value);
  }

  @Test
  public void verifyIntDocVectorsOpennlp() throws Exception {
    System.out.println("verifyIntDocVectorsOpennlp");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs,
        new Path(opennlpIndex + "/wt-int-doc-vectors/part-00002"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(opennlpIntDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(opennlpIndex + "/wt-int-doc-vectors/part-00011"), fs.getConf());
    reader.next(key, value);
    verifyIntDocVector(opennlpIntDocVector2, value);
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
    return new JUnit4TestAdapter(VerifyWikipediaProcessingMonolingual.class);
  }
}
