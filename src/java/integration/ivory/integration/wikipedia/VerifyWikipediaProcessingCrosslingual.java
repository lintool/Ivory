package ivory.integration.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.integration.IntegrationUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
  private static final String tokenizerPath = tmp + "/tokenizer";
  private static final String enwikiPath = 
    "/shared/collections/wikipedia/raw/enwiki-20110115-pages-articles.xml";
  private static final String enwikiRepacked = tmp + "/enwiki-20110115.repacked";
  private static final String enwikiEn = tmp + "/enwiki.en";

  // en side: part 00000, key = 91805
  private int enTermDocVector1Id = 91805;
  private ImmutableMap<String, Float> enTermDocVector1 = ImmutableMap.of(
      "total", 0.052498523f, "extern", 0.0031357605f, "side", 0.050600808f, "refer", -0.015544628f);

  // en side: part 00010, key = 137938
  private int enTermDocVector2Id = 137938;
  private ImmutableMap<String, Float> enTermDocVector2 = ImmutableMap.of(
      "extern", 0.0031639708f, "perspect", 0.09983705f, "deal", 0.07179488f, "devianc", 0.19152483f);

  // en side: part 00002, key = 148600
  private int enIntDocVector1Id = 148600;
  private ImmutableMap<Integer, Float> enIntDocVector1 =
    ImmutableMap.of(42846, 0.059275027f, 1518, 0.038548473f, 307, 0.048701696f, 63715, 0.038108308f);

  // en side: part 00011, key = 181342
  private int enIntDocVector2Id = 181342;
  private ImmutableMap<Integer, Float> enIntDocVector2 =
    ImmutableMap.of(19446, 0.09696824f, 175, 0.049885243f, 31837, 0.18476018f, 4936, -0.020277286f);

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20110131-pages-articles.xml";
  private static final String dewikiRepacked = tmp + "/dewiki-20110131.repacked";
  private static final String dewikiEn = tmp + "/dewiki.en";

  // de side: part 00000, key = 1000960467
  private int deTermDocVector1Id = 1000960467;
  private ImmutableMap<String, Float> deTermDocVector1 = ImmutableMap.of(
      "stauffenberg", 0.46342498f, "2007", 0.054127622f, "consider", 0.003471215f, "famili", 0.068220295f);

  // de side: part 00010, key = 1000010078
  private int deTermDocVector2Id = 1000010078;
  private ImmutableMap<String, Float> deTermDocVector2 = ImmutableMap.of(
      "...]", 0.041361164f, "zbigniev", 0.07212809f, "da", 0.0030834419f, "buzek", 0.21578267f);

  // de side: part 00002, key = 1000131394
  private int deIntDocVector1Id = 1000131394;
  private ImmutableMap<Integer, Float> deIntDocVector1 =
    ImmutableMap.of(29656, 0.28162858f, 3265, 0.0063452665f, 13338, 0.24359147f, 9189, 0.045881663f);

  // de side: part 00011, key = 1000210390
  private int deIntDocVector2Id = 1000210390;
  private ImmutableMap<Integer, Float> deIntDocVector2 =
    ImmutableMap.of(3225, 0.0065635326f, 101, 0.60966045f, 9315, 0.054609142f, 46972, 0.080685005f);

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
        "-index=" + enwikiEn,
        "-xml=" + enwikiPath,
        "-compressed=" + enwikiRepacked,
        "tokenizerclass=" + ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(),
        "-lang=en",
        "-tokenizermodel=" + tokenizerPath + "/en-token.bin",
        "-collectionvocab=" + vocabPath + "/vocab.de-en.en", 
        "-mode=crosslingE",
        "-e_stopword=" + tokenizerPath + "/en.stop.stemmed"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  @Test
  public void verifyTermDocVectorsEn() throws Exception {
    System.out.println("verifyTermDocVectorsEn");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    HMapSFW value = findTermDocVector(enTermDocVector1Id, 0, fs, enwikiEn + "/wt-term-doc-vectors");
    System.out.println("enTermDocVector1\n"+value);
    verifyTermDocVector(enTermDocVector1, value);

    value = findTermDocVector(enTermDocVector2Id, 10, fs, enwikiEn + "/wt-term-doc-vectors");
    System.out.println("enTermDocVector2\n"+value);
    verifyTermDocVector(enTermDocVector2, value);
  }

  @Test
  public void verifyIntDocVectorsEn() throws Exception {
    System.out.println("verifyIntDocVectorsEn");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    WeightedIntDocVector value = findIntDocVector(enIntDocVector1Id, 2, fs, enwikiEn + "/wt-int-doc-vectors");
    System.out.println("enIntDocVector1\n"+value);
    verifyIntDocVector(enIntDocVector1, value);

    value = findIntDocVector(enIntDocVector2Id, 11, fs, enwikiEn + "/wt-int-doc-vectors");
    System.out.println("enIntDocVector2\n"+value);
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
        "-e_stopword=" + tokenizerPath + "/en.stop.stemmed", 
        "-f_stopword=" + tokenizerPath + "/de.stop.stemmed",
        "-e_tokenizermodel=" + tokenizerPath + "/en-token.bin",
        "-target_lang=en"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  @Test
  public void verifyTermDocVectorsDe() throws Exception {
    System.out.println("verifyTermDocVectorsDe");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    HMapSFW value = findTermDocVector(deTermDocVector1Id, 0, fs, dewikiEn + "/wt-term-doc-vectors");
    System.out.println("deTermDocVector1\n"+value);
    verifyTermDocVector(deTermDocVector1, value);

    value = findTermDocVector(deTermDocVector2Id, 10, fs, dewikiEn + "/wt-term-doc-vectors");
    System.out.println("deTermDocVector2\n"+value);
    verifyTermDocVector(deTermDocVector2, value);
  }

  @Test
  public void verifyIntDocVectorsDe() throws Exception {
    System.out.println("verifyIntDocVectorsDe");
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    WeightedIntDocVector value = findIntDocVector(deIntDocVector1Id, 2, fs, dewikiEn + "/wt-int-doc-vectors");
    System.out.println("deIntDocVector1\n"+value);
    verifyIntDocVector(deIntDocVector1, value);

    value = findIntDocVector(deIntDocVector2Id, 11, fs, dewikiEn + "/wt-int-doc-vectors");
    System.out.println("deIntDocVector2\n"+value);
    verifyIntDocVector(deIntDocVector2, value);
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

  private HMapSFW findTermDocVector(int docno, int startFrom, FileSystem fs, String dir) {
    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    HMapSFW value = new HMapSFW();
    try {
      FileStatus[] paths = fs.listStatus(new Path(dir));
      System.err.println("length " + paths.length);
      for (FileStatus path : paths) {
        if (!path.getPath().getName().contains("part"))  continue;
        
        String[] arr = path.getPath().getName().split("part-");
        int partNo = Integer.parseInt(arr[arr.length-1]);
        if (partNo != startFrom)  continue;
        
        System.err.println("Reading " + path.getPath().getName());
        key.set(0);
        reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(path.getPath()));
        while (key.get() < docno) {
          reader.next(key, value);
        }
        if (key.get() == docno) {
          reader.close();
          return value;
        }
        System.err.println("key:"+key.get()+" != docno:"+docno);
        startFrom++;
        reader.close();
      }
    } catch (IOException e) {
      Assert.fail("Error: reading term doc vectors from + " + dir + ", docno = " + docno);
    }
    return null;
  }
  
  private WeightedIntDocVector findIntDocVector(int docno, int startFrom, FileSystem fs, String dir) {
    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    WeightedIntDocVector value = new WeightedIntDocVector();
    try {
      FileStatus[] paths = fs.listStatus(new Path(dir));
      System.err.println("length " + paths.length);
      for (FileStatus path : paths) {
        if (!path.getPath().getName().contains("part"))  continue;
        
        String[] arr = path.getPath().getName().split("part-");
        int partNo = Integer.parseInt(arr[arr.length-1]);
        if (partNo != startFrom)  continue;
        
        System.err.println("Reading " + path.getPath().getName());
        key.set(0);
        reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(path.getPath()));
        while (key.get() < docno) {
          reader.next(key, value);
        }
        if (key.get() == docno) {
          reader.close();
          return value;
        }
        startFrom++;
        reader.close();
      }
    } catch (IOException e) {
      Assert.fail("Error: reading int doc vectors from + " + dir + ", docno = " + docno);
    }
    return null;
  }
  
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyWikipediaProcessingCrosslingual.class);
  }
}
