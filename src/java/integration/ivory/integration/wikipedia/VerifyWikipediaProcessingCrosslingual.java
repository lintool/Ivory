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

public class VerifyWikipediaProcessingCrosslingual {
  private static final Random RAND = new Random();
  private static final String tmp =
      VerifyWikipediaProcessingCrosslingual.class.getCanonicalName() + RAND.nextInt(10000);

  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";
  private static final String enwikiPath = "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles";
  private static final String enwikiRepacked = tmp + "/enwiki-20121201.repacked";
  private static final String enwikiEn = tmp + "/enwiki.en";

  // en side: part 00000, key = 91805
  private int enTermDocVector1Id = 91805;
  private ImmutableMap<String, Float> enTermDocVector1 = ImmutableMap.of(
      "religi", 0.056898247f, "lubric", 0.07892087f, "time", 0.021438342f, "refer", -0.017549722f);

  // en side: part 00010, key = 137938
  private int enTermDocVector2Id = 137938;
  private ImmutableMap<String, Float> enTermDocVector2 = ImmutableMap.of(
      "stori", 0.034548897f, "2006", 0.023635013f, "nineti", 0.076754145f, "time", 0.019773208f);

  // en side: part 00002, key = 148600
  private int enIntDocVector1Id = 148600;
  private ImmutableMap<Integer, Float> enIntDocVector1 =
    ImmutableMap.of(3310, 0.0071687745f, 4479, 0.09890289f, 7599, 0.24106947f, 2063, 0.16018048f);

  // en side: part 00011, key = 181342
  private int enIntDocVector2Id = 181342;
  private ImmutableMap<Integer, Float> enIntDocVector2 =
    ImmutableMap.of(6569, 0.044599857f, 4393, 0.019540867f, 16527, 0.05980431f, 9764, 0.045334294f);

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20121117-pages-articles.xml";
  private static final String dewikiRepacked = tmp + "/dewiki-20121117.repacked";
  private static final String dewikiEn = tmp + "/dewiki.en";

  // de side: part 00010, key = 1000010078
  private int deTermDocVector1Id = 1000010078;
  private ImmutableMap<String, Float> deTermDocVector1 = ImmutableMap.of(
      "total", 0.007482552f, "need", 0.06130964f, "big", 0.014260361f, "histor", 0.0714205f);
  // de side: part 00000, key = 1000960467
  private int deTermDocVector2Id = 1000960467;
  private ImmutableMap<String, Float> deTermDocVector2 = ImmutableMap.of(
      "2008", 0.033327986f, "role", 0.008505447f, "bolkestein", 0.009285147f, "ordinari", 0.0077467756f);

  // de side: part 00002, key = 1000131394
  private int deIntDocVector1Id = 1000131394;
  private ImmutableMap<Integer, Float> deIntDocVector1 =
    ImmutableMap.of(1100, 0.04779704f, 6585, 0.018187178f, 21, 0.007229667f, 2194, 0.009517357f);

  // de side: part 00011, key = 1000210390
  private int deIntDocVector2Id = 1000210390;
  private ImmutableMap<Integer, Float> deIntDocVector2 =
    ImmutableMap.of(6585, 0.0050360947f, 15, 0.0047478294f, 2200, 0.040175833f, 6566, 0.013208171f);

  @Test
  public void runBuildIndexEnSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(enwikiPath)));

    fs.delete(new Path(enwikiEn), true);
    fs.delete(new Path(enwikiRepacked), true);
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
        "-index=" + enwikiEn,
        "-xml=" + enwikiPath,
        "-compressed=" + enwikiRepacked,
        "tokenizerclass=" + ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(),
        "-lang=en",
        "-tokenizermodel=" + tokenizerPath + "/en-token.bin",
        "-collectionvocab=" + vocabPath + "/vocab.de-en.en", 
        "-mode=crosslingE",
        "-e_stopword=" + tokenizerPath + "/en.stop"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + enwikiEn + "/wt-term-doc-vectors", 
        "-output=" + enwikiEn + "/test_wt-term-doc-vectors", 
        "-keys=" + enTermDocVector1Id + "," + enTermDocVector2Id, 
        "-valueclass=" + edu.umd.cloud9.io.map.HMapSFW.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + enwikiEn + "/wt-int-doc-vectors", 
        "-output=" + enwikiEn + "/test_wt-int-doc-vectors", 
        "-keys=" + enIntDocVector1Id + "," + enIntDocVector2Id, 
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  @Test
  public void verifyTermDocVectorsEn() throws Exception {
    System.out.println("verifyTermDocVectorsEn");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(enwikiEn + "/test_wt-term-doc-vectors/part-00000")));
    reader.next(key, value);
    verifyTermDocVector(enTermDocVector1, value);
    System.out.println("enTermDocVector1\n"+key+","+value);
    reader.next(key, value);
    verifyTermDocVector(enTermDocVector2, value);
    System.out.println("enTermDocVector2\n"+key+","+value);
    reader.close();
  }

  @Test
  public void verifyIntDocVectorsEn() throws Exception {
    System.out.println("verifyIntDocVectorsEn");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(enwikiEn + "/test_wt-int-doc-vectors/part-00000")));
    reader.next(key, value);
    verifyIntDocVector(enIntDocVector1, value);
    System.out.println("enIntDocVector1\n"+key+","+value);
    reader.next(key, value);
    verifyIntDocVector(enIntDocVector2, value);
    System.out.println("enIntDocVector2\n"+key+","+value);
    reader.close();
  }

  @Test
  public void runBuildIndexDeSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(dewikiPath)));

    fs.delete(new Path(dewikiEn), true);
    fs.delete(new Path(dewikiRepacked), true);
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
        "-index=" + dewikiEn,
        "-xml=" + dewikiPath,
        "-compressed=" + dewikiRepacked,
        "tokenizerclass=" + ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(),
        "-lang=de",
        "-tokenizermodel=" + tokenizerPath + "/de-token.bin",
        "-e_e2f_vocab=" + vocabPath + "/vocab.en-de.en",
        "-f_e2f_vocab=" + vocabPath + "/vocab.en-de.de",
        "-f_f2e_vocab=" + vocabPath + "/vocab.de-en.de",
        "-e_f2e_vocab=" + vocabPath + "/vocab.de-en.en",
        "-f2e_ttable=" + vocabPath + "/ttable.de-en",   
        "-e2f_ttable=" + vocabPath + "/ttable.en-de",
        "-collectionvocab=" + vocabPath + "/vocab.de-en.en", 
        "-mode=crosslingF",
        "-targetindex=" + enwikiEn,
        "-e_stopword=" + tokenizerPath + "/en.stop", 
        "-f_stopword=" + tokenizerPath + "/de.stop",
        "-e_tokenizermodel=" + tokenizerPath + "/en-token.bin",
    "-target_lang=en"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + dewikiEn + "/wt-term-doc-vectors", 
        "-output=" + dewikiEn + "/test_wt-term-doc-vectors", 
        "-keys=" + deTermDocVector1Id + "," + deTermDocVector2Id, 
        "-valueclass=" + edu.umd.cloud9.io.map.HMapSFW.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + dewikiEn + "/wt-int-doc-vectors", 
        "-output=" + dewikiEn + "/test_wt-int-doc-vectors", 
        "-keys=" + deIntDocVector1Id + "," + deIntDocVector2Id, 
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  @Test
  public void verifyTermDocVectorsDe() throws Exception {
    System.out.println("verifyTermDocVectorsDe");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(dewikiEn + "/test_wt-term-doc-vectors/part-00000")));
    reader.next(key, value);
    verifyTermDocVector(deTermDocVector1, value);
    System.out.println("deTermDocVector1\n"+key+","+value);
    reader.next(key, value);
    verifyTermDocVector(deTermDocVector2, value);
    System.out.println("deTermDocVector2\n"+key+","+value);
    reader.close();
  }

  @Test
  public void verifyIntDocVectorsDe() throws Exception {
    System.out.println("verifyIntDocVectorsDe");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(dewikiEn + "/test_wt-int-doc-vectors/part-00000")));
    reader.next(key, value);
    verifyIntDocVector(deIntDocVector1, value);
    System.out.println("deIntDocVector1\n"+key+","+value);
    reader.next(key, value);
    verifyIntDocVector(deIntDocVector2, value);
    System.out.println("deIntDocVector2\n"+key+","+value);
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
    return new JUnit4TestAdapter(VerifyWikipediaProcessingCrosslingual.class);
  }
}
