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

import tl.lin.data.map.HMapIFW;
import tl.lin.data.map.HMapSFW;
import tl.lin.data.map.MapIF;
import tl.lin.data.map.MapKF;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class VerifyWikipediaProcessingCrosslingual {
  private static final Random RAND = new Random();
  private static final String tmp =
      VerifyWikipediaProcessingCrosslingual.class.getCanonicalName() + RAND.nextInt(10000);

  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";
  private static final String enwikiPath = "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles";
  private static final String enwikiRepacked = tmp + "/enwiki-20121201.repacked";
  private static final String enwikiEn = tmp + "/enwiki.en";

  // docno 1000 = docid 2508: Artillery
  // docno 12345 = docid 26094: Romanticism

  private int enTermDocVector1Id = 1000;
  private ImmutableMap<String, Float> enTermDocVector1 =
      new ImmutableMap.Builder<String, Float>()
          .put("projectil", 0.1401f)
          .put("artilleri", 0.1297f)
          .put("mortar", 0.1213f)
          .put("ammunit", 0.1111f)
          .put("propel", 0.1083f)
          .put("barrel", 0.1068f)
          .put("muzzl", 0.1067f)
          .put("batteri", 0.1063f)
          .put("cannon", 0.1034f)
          .put("indirect", 0.0984f)
          .build();

  private int enTermDocVector2Id = 12345;
  private ImmutableMap<String, Float> enTermDocVector2 =
      new ImmutableMap.Builder<String, Float>()
          .put("romantic", 0.1747f)
          .put("romant", 0.1203f)
          .put("byron", 0.0994f)
          .put("friedrich", 0.0911f)
          .put("realism", 0.0879f)
          .put("goya", 0.0878f)
          .put("enlighten", 0.0835f)
          .put("literatur", 0.0785f)
          .put("norton", 0.0779f)
          .put("paint", 0.0778f)
          .build();

  private int enIntDocVector1Id = 1000;
  private ImmutableMap<Integer, Float> enIntDocVector1 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(52122, 0.1401f)
          .put(6985, 0.1297f)
          .put(11772, 0.1213f)
          .put(22735, 0.1111f)
          .put(20210, 0.1083f)
          .put(16577, 0.1068f)
          .put(11868, 0.1067f)
          .put(3814, 0.1063f)
          .put(20221, 0.1034f)
          .put(25506, 0.0984f)
          .build();

  private int enIntDocVector2Id = 12345;
  private ImmutableMap<Integer, Float> enIntDocVector2 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(6653, 0.1747f)
          .put(38336, 0.1203f)
          .put(66533, 0.0994f)
          .put(17415, 0.0911f)
          .put(22488, 0.0879f)
          .put(25108, 0.0878f)
          .put(16466, 0.0835f)
          .put(20556, 0.0785f)
          .put(62521, 0.0779f)
          .put(8216, 0.0778f)
          .build();

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20121117-pages-articles.xml";

  private static final String dewikiRepacked = tmp + "/dewiki-20121117.repacked";
  private static final String dewikiEn = tmp + "/dewiki.en";

  // docno 500 = docid 918: Cha-Cha-Cha
  // docno 1024 = docid 1964: Gene Roddenberry

  private int deTermDocVector1Id = 1000000500;
  private ImmutableMap<String, Float> deTermDocVector1 =
      new ImmutableMap.Builder<String, Float>()
          .put("sopexa", 0.3736f)
          .put("optimal", 0.2171f)
          .put("unite", 0.1982f)
          .put("bandwagon", 0.1843f)
          .put("paladium", 0.1743f)
          .put("trampl", 0.1719f)
          .put("cha", 0.1615f)
          .put("flout", 0.1580f)
          .put("cuban", 0.1348f)
          .put("semiopen", 0.1242f)
          .build();

  private int deTermDocVector2Id = 1000001024;
  private ImmutableMap<String, Float> deTermDocVector2 =
      new ImmutableMap.Builder<String, Float>()
          .put("october", 0.3206f)
          .put("office", 0.1620f)
          .put("spaceship", 0.1377f)
          .put("melk", 0.1351f)
          .put("barrett", 0.1330f)
          .put("cinematograph", 0.1299f)
          .put("angele", 0.1275f)
          .put("spacecraft", 0.1259f)
          .put("aire", 0.1167f)
          .put("gene", 0.1114f)
          .build();

  private int deIntDocVector1Id = 1000000500;
  private ImmutableMap<Integer, Float> deIntDocVector1 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(28750, 0.3736f)
          .put(8397, 0.2171f)
          .put(3208, 0.1982f)
          .put(43524, 0.1843f)
          .put(22251, 0.1743f)
          .put(7084, 0.1719f)
          .put(1999, 0.1615f)
          .put(13827, 0.1580f)
          .put(13481, 0.1348f)
          .put(42106, 0.1242f)
          .build();

  private int deIntDocVector2Id = 1000001024;
  private ImmutableMap<Integer, Float> deIntDocVector2 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(28044, 0.3206f)
          .put(771, 0.1620f)
          .put(66841, 0.1377f)
          .put(8524, 0.1351f)
          .put(15531, 0.1330f)
          .put(15257, 0.1299f)
          .put(28417, 0.1275f)
          .put(58008, 0.1259f)
          .put(26069, 0.1167f)
          .put(7512, 0.1114f)
          .build();

  @Test
  public void runTests() throws Exception {
    runBuildIndexEnSide();
    verifyTermDocVectorsEn();
    verifyIntDocVectorsEn();

    runBuildIndexDeSide();
    verifyTermDocVectorsDe();
    verifyIntDocVectorsDe();
  }

  public void runBuildIndexEnSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(enwikiPath)));

    fs.delete(new Path(enwikiEn), true);
    fs.delete(new Path(enwikiRepacked), true);
    fs.delete(new Path(vocabPath), true);
    fs.delete(new Path(tokenizerPath), true);
   
    fs.copyFromLocalFile(false, true, new Path("data/vocab/other/de-en.wmt10"), new Path(vocabPath));
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

    String[] args;
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
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
        "-valueclass=" + HMapSFW.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + enwikiEn + "/wt-int-doc-vectors", 
        "-output=" + enwikiEn + "/test_wt-int-doc-vectors", 
        "-keys=" + enIntDocVector1Id + "," + enIntDocVector2Id, 
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

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
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(enTermDocVector1, value);

    reader.next(key, value);
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(enTermDocVector2, value);
    reader.close();
  }

  public void verifyIntDocVectorsEn() throws Exception {
    System.out.println("verifyIntDocVectorsEn");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapIFW map = new HMapIFW();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(enwikiEn + "/test_wt-int-doc-vectors/part-00000")));

    reader.next(key, value);
    System.out.println("*** top 10 terms ***");
    map = value.getWeightedTerms();
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(enIntDocVector1, value);

    reader.next(key, value);
    System.out.println("*** top 10 terms ***");
    map = value.getWeightedTerms();
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(enIntDocVector2, value);
    reader.close();
  }

  public void runBuildIndexDeSide() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(dewikiPath)));

    fs.delete(new Path(dewikiEn), true);
    fs.delete(new Path(dewikiRepacked), true);
    fs.delete(new Path(vocabPath), true);
    fs.delete(new Path(tokenizerPath), true);

    fs.copyFromLocalFile(false, true, new Path("data/vocab/other/de-en.wmt10"), new Path(vocabPath));
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

    String[] args;
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
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
        "-valueclass=" + HMapSFW.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + dewikiEn + "/wt-int-doc-vectors", 
        "-output=" + dewikiEn + "/test_wt-int-doc-vectors", 
        "-keys=" + deIntDocVector1Id + "," + deIntDocVector2Id, 
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName()};
    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

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
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(deTermDocVector1, value);

    reader.next(key, value);
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(deTermDocVector2, value);
    reader.close();
  }

  public void verifyIntDocVectorsDe() throws Exception {
    System.out.println("verifyIntDocVectorsDe");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapIFW map = new HMapIFW();
    WeightedIntDocVector value = new WeightedIntDocVector();

    reader = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(dewikiEn + "/test_wt-int-doc-vectors/part-00000")));
    reader.next(key, value);
    System.out.println("*** top 10 terms ***");
    map = value.getWeightedTerms();
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(deIntDocVector1, value);

    reader.next(key, value);
    System.out.println("*** top 10 terms ***");
    map = value.getWeightedTerms();
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(deIntDocVector2, value);
    reader.close();
  }

  private void verifyTermDocVector(Map<String, Float> doc, HMapSFW value) {
    assertTrue(value != null);
    for (Map.Entry<String, Float> entry : doc.entrySet()) {
      assertTrue(value.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), value.get(entry.getKey()), 10e-4);
    }
  }

  private void verifyIntDocVector(Map<Integer, Float> doc, WeightedIntDocVector value) {
    assertTrue(value != null);
    for (Map.Entry<Integer, Float> entry : doc.entrySet()) {
      assertTrue(value.containsTerm(entry.getKey()));
      assertEquals(entry.getValue(), value.getWeight(entry.getKey()), 10e-4);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWikipediaProcessingCrosslingual.class);
  }
}
