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

  // Galago: part 00000, key = 91805
  private ImmutableMap<String, Float> galagoTermDocVector1 = ImmutableMap.of(
      "theori", 0.05498378f, "extern", 0.0021626172f, "refer", -0.01075585f, "50", 0.056969844f);

  // Galago: part 00010, key = 34096
  private ImmutableMap<String, Float> galagoTermDocVector2 = ImmutableMap.of(
      "extern", 0.005528182f, "librari", 0.12763943f, "film", 0.104365f, "cultur", 0.10795438f);

  // Galago: part 00002, key = 100585
  private ImmutableMap<Integer, Float> galagoIntDocVector1 =
    ImmutableMap.of(5, 0.016333312f, 1660, 0.07839933f, 7889, 0.120369025f, 3792, 0.09966967f);

  // Galago: part 00011, key = 34096
  private ImmutableMap<Integer, Float> galagoIntDocVector2 =
    ImmutableMap.of(2, 0.0037445724f, 521, 0.11334762f, 2430, 0.17121725f, 421, 0.10686168f);

  private static final String opennlpIndex = tmp + "/enwiki.opennlp";
  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";

  // Opennlp: part 00000, key = 91805
  private ImmutableMap<String, Float> opennlpTermDocVector1 = ImmutableMap.of(
      "extern", 0.0031357605f, "zero", 0.101532206f, "theorem", 0.1276031f, "prime", 0.08161251f);

  // Opennlp: part 00010, key = 137938
  private ImmutableMap<String, Float> opennlpTermDocVector2 = ImmutableMap.of(
      "cycl", 0.092633486f, "scholar", 0.085225f, "problem", 0.06802353f, "opinion", 0.09402901f);

  // Opennlp: part 00002, key = 4764
  private ImmutableMap<Integer, Float> opennlpIntDocVector1 =
    ImmutableMap.of(5, 0.03059925f, 11206, 0.20217548f, 23, 0.041687272f, 879, 0.11775808f);

  // Opennlp: part 00011, key = 148600
  private ImmutableMap<Integer, Float> opennlpIntDocVector2 =
    ImmutableMap.of(1, -0.0068670297f, 4868, 0.05292152f, 3248, 0.046914175f, 2177, 0.0760979f);

  @Test
  public void runBuildIndexGalago() throws Exception {

    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(galagoIndex), true);
    fs.delete(new Path(collectionRepacked), true);
    fs.delete(new Path(vocabPath), true);
    fs.delete(new Path(tokenizerPath), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab"), new Path(vocabPath));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer"), new Path(tokenizerPath));

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
        "-mode=mono", "-index="+galagoIndex, "-xml="+collectionPath, "-compressed="+collectionRepacked,
        "-tokenizerclass="+ivory.core.tokenize.GalagoTokenizer.class.getCanonicalName(), "-lang=en",
        "-tokenizermodel="+tokenizerPath + "/en-token.bin"});
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
    fs.delete(new Path(tokenizerPath), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab"), new Path(vocabPath));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer"), new Path(tokenizerPath));

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
        "-mode=mono", "-index="+opennlpIndex, "-xml="+collectionPath, "-compressed="+collectionRepacked,
        "-tokenizerclass=" + ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "-lang=en",
        "-tokenizermodel=" + tokenizerPath + "/en-token.bin", "-collectionvocab=" + vocabPath + "/vocab.de-en.en", "-e_stopword=" + tokenizerPath + "/en.stop.stemmed"});
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
    System.out.println("opennlpterm1\n"+value);
    verifyTermDocVector(opennlpTermDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(opennlpIndex + "/wt-term-doc-vectors/part-00010"), fs.getConf());
    reader.next(key, value);
    System.out.println("opennlpterm2\n"+value);
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
    System.out.println("opennlpInt1\n"+value);
    verifyIntDocVector(opennlpIntDocVector1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(opennlpIndex + "/wt-int-doc-vectors/part-00011"), fs.getConf());
    reader.next(key, value);
    System.out.println("opennlpInt2\n"+value);
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
