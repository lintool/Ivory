package ivory.integration.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import ivory.core.data.document.WeightedIntDocVector;
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
  private static final Random RAND = new Random();
  private static final String tmp =
      VerifyWikipediaProcessingMonolingual.class.getCanonicalName() + RAND.nextInt(10000);

  private static final String collectionPath = 
    "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles.xml";
  private static final String collectionRepacked = tmp + "/enwiki-20121201.repacked";
  private static final String galagoIndex = tmp + "/enwiki.galago";

  // Galago: part 00010, key = 34096
  private int galagoTermDocVector1Id = 34096;
  private ImmutableMap<String, Float> galagoTermDocVector1 = ImmutableMap.of(
      "time", 0.018549806f, "refer", -0.021184113f, "greec", 0.09738249f, "sparta", 0.12279472f);

  // Galago: part 00000, key = 91805
  private int galagoTermDocVector2Id = 91805;
  private ImmutableMap<String, Float> galagoTermDocVector2 = ImmutableMap.of(
      "religi", 0.04332288f, "lubric", 0.06086864f, "time", 0.016003875f, "refer", -0.013383096f);

  // Galago: part 00011, key = 34096
  private int galagoIntDocVector1Id = 34096;
  private ImmutableMap<Integer, Float> galagoIntDocVector1 =
    ImmutableMap.of(1, -0.021184111f, 23917, 0.14610383f, 5, 0.01883354f, 9, 0.018549804f);

  // Galago: part 00002, key = 100585
  private int galagoIntDocVector2Id = 100585;
  private ImmutableMap<Integer, Float> galagoIntDocVector2 =
    ImmutableMap.of(41851, 0.059388004f, 1101, 0.024443226f, 5, 0.00780255f, 3282, 0.03333674f);

  private static final String opennlpIndex = tmp + "/enwiki.opennlp";
  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";

  // Opennlp: part 00000, key = 91805
  private int opennlpTermDocVector1Id = 91805;
  private ImmutableMap<String, Float> opennlpTermDocVector1 = ImmutableMap.of(
      "religi", 0.056898247f, "lubric", 0.07892087f, "time", 0.021438342f, "refer", -0.017549722f);

  // Opennlp: part 00010, key = 137938
  private int opennlpTermDocVector2Id = 137938;
  private ImmutableMap<String, Float> opennlpTermDocVector2 = ImmutableMap.of(
      "stori", 0.034548897f, "2006", 0.023635013f, "nineti", 0.076754145f, "time", 0.019773208f);

  // Opennlp: part 00002, key = 4764
  private int opennlpIntDocVector1Id = 4764;
  private ImmutableMap<Integer, Float> opennlpIntDocVector1 =
    ImmutableMap.of(4, 0.019922445f, 8, 0.027526723f, 1095, 0.104451805f, 1028, 0.102825336f);

  // Opennlp: part 00011, key = 148600
  private int opennlpIntDocVector2Id = 148600;
  private ImmutableMap<Integer, Float> opennlpIntDocVector2 =
    ImmutableMap.of(2, 0.0059410483f, 1102, 0.16451068f, 88, 0.09218009f, 140, 0.098902896f);

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
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessWikipedia.class.getCanonicalName(), libjars,
        "-mode=mono",
        "-index=" + galagoIndex,
        "-xml=" + collectionPath,
        "-compressed=" + collectionRepacked,
        "-tokenizerclass=" + ivory.core.tokenize.GalagoTokenizer.class.getCanonicalName(),
        "-lang=en",
        "-tokenizermodel=" + tokenizerPath + "/en-token.bin"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + galagoIndex + "/wt-term-doc-vectors",
        "-output=" + galagoIndex + "/test_wt-term-doc-vectors",
        "-keys=" + galagoTermDocVector1Id + "," + galagoTermDocVector2Id,
        "-valueclass=" + edu.umd.cloud9.io.map.HMapSFW.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + galagoIndex + "/wt-int-doc-vectors",
        "-output=" + galagoIndex + "/test_wt-int-doc-vectors",
        "-keys=" + galagoIntDocVector1Id + "," + galagoIntDocVector2Id,
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  @Test
  public void verifyTermDocVectorsGalago() throws Exception {
    System.out.println("verifyTermDocVectorsGalago");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(galagoIndex + "/test_wt-term-doc-vectors/part-00000")));
    reader.next(key, value);
    System.out.println("galagoTerm1\n"+key+";"+value);
    verifyTermDocVector(galagoTermDocVector1, value);
    reader.next(key, value);
    System.out.println("galagoTerm2\n"+key+";"+value);
    verifyTermDocVector(galagoTermDocVector2, value);
    reader.close();
  }

  @Test
  public void verifyIntDocVectorsGalago() throws Exception {
    System.out.println("verifyIntDocVectorsGalago");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(galagoIndex + "/test_wt-int-doc-vectors/part-00000")));
    reader.next(key, value);
    System.out.println("galagoInt1\n"+key+";"+value);
    verifyIntDocVector(galagoIntDocVector1, value);
    reader.next(key, value);
    System.out.println("galagoInt2\n"+key+";"+value);
    verifyIntDocVector(galagoIntDocVector2, value);
    reader.close();
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
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessWikipedia.class.getCanonicalName(), libjars,
        "-mode=mono",
        "-index=" + opennlpIndex,
        "-xml=" + collectionPath,
        "-compressed=" + collectionRepacked,
        "-tokenizerclass=" + ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(),
        "-lang=en",
        "-tokenizermodel=" + tokenizerPath + "/en-token.bin",
        "-collectionvocab=" + vocabPath + "/vocab.de-en.en",
        "-e_stopword=" + tokenizerPath + "/en.stop"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + opennlpIndex + "/wt-term-doc-vectors",
        "-output=" + opennlpIndex + "/test_wt-term-doc-vectors",
        "-keys=" + opennlpTermDocVector1Id + "," + opennlpTermDocVector2Id,
        "-valueclass=" + edu.umd.cloud9.io.map.HMapSFW.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + opennlpIndex + "/wt-int-doc-vectors",
        "-output=" + opennlpIndex + "/test_wt-int-doc-vectors",
        "-keys=" + opennlpIntDocVector1Id + "," + opennlpIntDocVector2Id,
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  @Test
  public void verifyTermDocVectorsOpennlp() throws Exception {
    System.out.println("verifyTermDocVectorsOpennlp");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(opennlpIndex + "/test_wt-term-doc-vectors/part-00000")));
    reader.next(key, value);
    System.out.println("opennlpterm1\n"+key+";"+value);
    verifyTermDocVector(opennlpTermDocVector1, value);
    reader.next(key, value);
    System.out.println("opennlpterm2\n"+key+";"+value);
    verifyTermDocVector(opennlpTermDocVector2, value);
    reader.close();
  }

  @Test
  public void verifyIntDocVectorsOpennlp() throws Exception {
    System.out.println("verifyIntDocVectorsOpennlp");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(opennlpIndex + "/test_wt-int-doc-vectors/part-00000")));
    reader.next(key, value);
    System.out.println("opennlpInt1\n"+key+";"+value);
    verifyIntDocVector(opennlpIntDocVector1, value);
    reader.next(key, value);
    System.out.println("opennlpInt2\n"+key+";"+value);
    verifyIntDocVector(opennlpIntDocVector2, value);
    reader.close();
  }

  private void verifyTermDocVector(Map<String, Float> doc, HMapSFW value) {
    assertTrue(value != null);
    for (Map.Entry<String, Float> entry : doc.entrySet()) {
      assertTrue(value.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), value.get(entry.getKey()), 10e-6);
    }
  }

  private void verifyIntDocVector(Map<Integer, Float> doc, WeightedIntDocVector value) {
    assertTrue(value != null);
    for (Map.Entry<Integer, Float> entry : doc.entrySet()) {
      assertTrue(value.containsTerm(entry.getKey()));
      assertEquals(entry.getValue(), value.getWeight(entry.getKey()), 10e-6);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWikipediaProcessingMonolingual.class);
  }
}
