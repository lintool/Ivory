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
      "beneventum", 0.19515267f, "pre", 0.06326606f, "egypt", 0.08399084f, "coloni", 0.070443f);

  // Galago: part 00000, key = 91805
  private int galagoTermDocVector2Id = 91805;
  private ImmutableMap<String, Float> galagoTermDocVector2 = ImmutableMap.of(
      "entir", 0.012336632f, "91", 0.027462268f, "pollak", 0.09643835f, "found", 0.006558274f);

  // Galago: part 00011, key = 34096
  private int galagoIntDocVector1Id = 34096;
  private ImmutableMap<Integer, Float> galagoIntDocVector1 =
    ImmutableMap.of(1, -0.0206428f, 756, 0.059395142f, 51217, 0.18819067f, 982, 0.063754365f);

  // Galago: part 00002, key = 100585
  private int galagoIntDocVector2Id = 100585;
  private ImmutableMap<Integer, Float> galagoIntDocVector2 =
    ImmutableMap.of(5, 0.01021795f, 585, 0.028274508f, 45242, 0.13892333f, 3414, 0.045721285f);

  private static final String opennlpIndex = tmp + "/enwiki.opennlp";
  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";

  // Opennlp: part 00000, key = 91805
  private int opennlpTermDocVector1Id = 91805;
  private ImmutableMap<String, Float> opennlpTermDocVector1 = ImmutableMap.of(
      "clutter", 0.043639377f, "zoom", 0.060861073f, "portray", 0.022965258f, "refer", -0.0062234555f);

  // Opennlp: part 00010, key = 137938
  private int opennlpTermDocVector2Id = 137938;
  private ImmutableMap<String, Float> opennlpTermDocVector2 = ImmutableMap.of(
      "histor", 0.018175913f, "vigilant", 0.11764987f, "augment", 0.04146363f, "time", 0.01755069f);

  // Opennlp: part 00002, key = 4764
  private int opennlpIntDocVector1Id = 4764;
  private ImmutableMap<Integer, Float> opennlpIntDocVector1 =
    ImmutableMap.of(4, 0.019955039f, 8, 0.027558785f, 2066, 0.12415717f, 1072, 0.12747908f);

  // Opennlp: part 00011, key = 148600
  private int opennlpIntDocVector2Id = 148600;
  private ImmutableMap<Integer, Float> opennlpIntDocVector2 =
    ImmutableMap.of(1, -0.02064175f, 9783, 0.1542827f, 1103, 0.06609425f, 5468, 0.1312336f);

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
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
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
