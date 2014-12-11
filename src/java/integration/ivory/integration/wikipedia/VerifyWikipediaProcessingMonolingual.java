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

public class VerifyWikipediaProcessingMonolingual {
  private static final Random RAND = new Random();
  private static final String tmp = VerifyWikipediaProcessingMonolingual.class.getCanonicalName() + RAND.nextInt(10000);

  private static final String collectionPath = "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles";
  private static final String collectionRepacked = tmp + "/enwiki-20121201.repacked";
  private static final String galagoIndex = tmp + "/enwiki.galago";

  // docno 1: Anarchism
  // docno 2: Autism

  private int galagoTermDocVector1Id = 1;
  private ImmutableMap<String, Float> galagoTermDocVector1 = 
      new ImmutableMap.Builder<String, Float>()
          .put("anarch", 0.1396f)
          .put("anarcho", 0.1389f)
          .put("anarchist", 0.1270f)
          .put("individualist", 0.1235f)
          .put("stirner", 0.1218f)
          .put("proudhon", 0.1189f)
          .put("syndicalist", 0.1152f)
          .put("insurrectionari", 0.1049f)
          .put("collectivist", 0.0986f)
          .put("libertarian", 0.0980f)
          .build();
  
  private int galagoTermDocVector2Id = 2;
  private ImmutableMap<String, Float> galagoTermDocVector2 =
      new ImmutableMap.Builder<String, Float>()
          .put("asd", 0.1578f)
          .put("autist", 0.1485f)
          .put("autism", 0.1457f)
          .put("pdd", 0.1231f)
          .put("asperg", 0.1200f)
          .put("mns", 0.1184f)
          .put("symptom", 0.0976f)
          .put("nonsoci", 0.0968f)
          .put("syndrom", 0.0936f)
          .put("diagnosi", 0.0895f)
          .build();

  private int galagoIntDocVector1Id = 1;
  private ImmutableMap<Integer, Float> galagoIntDocVector1 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(17836, 0.1396f)
          .put(28723, 0.1389f)
          .put(10641, 0.1270f)
          .put(23480, 0.1235f)
          .put(95282, 0.1218f)
          .put(68145, 0.1189f)
          .put(38972, 0.1152f)
          .put(84485, 0.1049f)
          .put(56020, 0.0986f)
          .put(10242, 0.0980f)
          .build();

  private int galagoIntDocVector2Id = 2;
  private ImmutableMap<Integer, Float> galagoIntDocVector2 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(33608, 0.1578f)
          .put(26081, 0.1485f)
          .put(18041, 0.1457f)
          .put(124021, 0.1231f)
          .put(42434, 0.1200f)
          .put(93174, 0.1184f)
          .put(5349, 0.0976f)
          .put(588669, 0.0968f)
          .put(5233, 0.0936f)
          .put(6676, 0.0895f)
          .build();

  private static final String opennlpIndex = tmp + "/enwiki.opennlp";
  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";

  private int opennlpTermDocVector1Id = 1;
  private ImmutableMap<String, Float> opennlpTermDocVector1 = 
      new ImmutableMap.Builder<String, Float>()
          .put("anarch", 0.1824f)
          .put("anarchist", 0.1663f)
          .put("individualist", 0.1588f)
          .put("syndicalist", 0.1463f)
          .put("libertarian", 0.1259f)
          .put("collectivist", 0.1235f)
          .put("anarchi", 0.1229f)
          .put("communism", 0.1138f)
          .put("bolshevik", 0.0900f)
          .put("woodcock", 0.0884f)
          .build();

  private int opennlpTermDocVector2Id = 2;
  private ImmutableMap<String, Float> opennlpTermDocVector2 =
      new ImmutableMap.Builder<String, Float>()
          .put("autist", 0.1791f)
          .put("autism", 0.1773f)
          .put("symptom", 0.1168f)
          .put("syndrom", 0.1114f)
          .put("diagnosi", 0.1054f)
          .put("disord", 0.1048f)
          .put("impair", 0.1045f)
          .put("genet", 0.0985f)
          .put("diagnos", 0.0952f)
          .put("diagnost", 0.0944f)
          .build();

  private int opennlpIntDocVector1Id = 1;
  private ImmutableMap<Integer, Float> opennlpIntDocVector1 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(13536, 0.1824f)
          .put(9150, 0.1663f)
          .put(16224, 0.1588f)
          .put(21027, 0.1463f)
          .put(8939, 0.1259f)
          .put(23931, 0.1235f)
          .put(10558, 0.1229f)
          .put(7006, 0.1138f)
          .put(9483, 0.0900f)
          .put(15808, 0.0884f)
          .build();

  private int opennlpIntDocVector2Id = 2;
  private ImmutableMap<Integer, Float> opennlpIntDocVector2 =
      new ImmutableMap.Builder<Integer, Float>()
          .put(17355, 0.1791f)
          .put(13641, 0.1773f)
          .put(5043, 0.1168f)
          .put(4936, 0.1114f)
          .put(6187, 0.1054f)
          .put(3595, 0.1048f)
          .put(6407, 0.1045f)
          .put(3421, 0.0985f)
          .put(4560, 0.0952f)
          .put(6833, 0.0944f)
          .build();

  @Test
  public void runBuildIndexGalago() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(galagoIndex), true);
    fs.delete(new Path(collectionRepacked), true);
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
    jars.add(IntegrationUtils.getJar("lib", "lintools-datatypes-1.0.0"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args;
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
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
        "-valueclass=" + HMapSFW.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + galagoIndex + "/wt-int-doc-vectors",
        "-output=" + galagoIndex + "/test_wt-int-doc-vectors",
        "-keys=" + galagoIntDocVector1Id + "," + galagoIntDocVector2Id,
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    System.out.println("verifyTermDocVectorsGalago");
    IntWritable key1 = new IntWritable();
    HMapSFW value1 = new HMapSFW();

    SequenceFile.Reader reader1 = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(galagoIndex + "/test_wt-term-doc-vectors/part-00000")));

    reader1.next(key1, value1);
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value1.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(galagoTermDocVector1, value1);

    reader1.next(key1, value1);

    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value1.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }

    verifyTermDocVector(galagoTermDocVector2, value1);
    reader1.close();

    System.out.println("verifyIntDocVectorsGalago");
    IntWritable key2 = new IntWritable();
    WeightedIntDocVector value2 = new WeightedIntDocVector();
    HMapIFW map = new HMapIFW();

    SequenceFile.Reader reader2 = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(galagoIndex + "/test_wt-int-doc-vectors/part-00000")));

    reader2.next(key2, value2);
    System.out.println("*** top 10 terms ***");
    map = value2.getWeightedTerms();
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(galagoIntDocVector1, value2);

    reader2.next(key2, value2);
    System.out.println("*** top 10 terms ***");
    map = value2.getWeightedTerms();
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(galagoIntDocVector2, value2);
    reader2.close();
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
    jars.add(IntegrationUtils.getJar("lib", "lintools-datatypes-1.0.0"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args;
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
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
        "-valueclass=" + HMapSFW.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + opennlpIndex + "/wt-int-doc-vectors",
        "-output=" + opennlpIndex + "/test_wt-int-doc-vectors",
        "-keys=" + opennlpIntDocVector1Id + "," + opennlpIntDocVector2Id,
        "-valueclass=" + ivory.core.data.document.WeightedIntDocVector.class.getCanonicalName() };
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    System.out.println("verifyTermDocVectorsOpennlp");
    IntWritable key1 = new IntWritable();
    HMapSFW value1 = new HMapSFW();

    SequenceFile.Reader reader1 = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(opennlpIndex + "/test_wt-term-doc-vectors/part-00000")));

    reader1.next(key1, value1);
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value1.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(opennlpTermDocVector1, value1);

    reader1.next(key1, value1);
    System.out.println("*** top 10 terms ***");
    for (MapKF.Entry<String> entry : value1.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyTermDocVector(opennlpTermDocVector2, value1);
    reader1.close();

    System.out.println("verifyIntDocVectorsOpennlp");
    IntWritable key2 = new IntWritable();
    WeightedIntDocVector value2 = new WeightedIntDocVector();
    HMapIFW map = new HMapIFW();

    SequenceFile.Reader reader2 = new SequenceFile.Reader(fs.getConf(),
        SequenceFile.Reader.file(new Path(opennlpIndex + "/test_wt-int-doc-vectors/part-00000")));
    reader2.next(key2, value2);
    map = value2.getWeightedTerms();
    System.out.println("*** top 10 terms ***");
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(opennlpIntDocVector1, value2);

    reader2.next(key2, value2);
    map = value2.getWeightedTerms();
    System.out.println("*** top 10 terms ***");
    for ( MapIF.Entry entry : map.getEntriesSortedByValue(10)) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    verifyIntDocVector(opennlpIntDocVector2, value2);
    reader2.close();
  }

  private void verifyTermDocVector(Map<String, Float> doc, HMapSFW value) {
    assertTrue(value != null);
    for (Map.Entry<String, Float> entry : doc.entrySet()) {
      System.out.println("checking " + entry.getKey() + ": expected = " + entry.getValue() + ", actual = " + value.get(entry.getKey()));
      assertTrue(value.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), value.get(entry.getKey()), 10e-4);
    }
  }

  private void verifyIntDocVector(Map<Integer, Float> doc, WeightedIntDocVector value) {
    assertTrue(value != null);
    for (Map.Entry<Integer, Float> entry : doc.entrySet()) {
      System.out.println("checking " + entry.getKey() + ": expected = " + entry.getValue() + ", actual = " + value.getWeight(entry.getKey()));
      assertTrue(value.containsTerm(entry.getKey()));
      assertEquals(entry.getValue(), value.getWeight(entry.getKey()), 10e-4);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWikipediaProcessingMonolingual.class);
  }
}
