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
  private static final Random rand = new Random();
  private static final String tmp = "tmp-" + VerifyWikipediaProcessingCrosslingual.class.getSimpleName() + rand.nextInt(10000);

  private static final String vocabPath = tmp + "/vocab";
  private static final String tokenizerPath = tmp + "/tokenizer";
  private static final String enwikiPath = 
    "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles.xml";
  private static final String enwikiRepacked = tmp + "/enwiki-20121201.repacked";
  private static final String enwikiEn = tmp + "/enwiki.en";

  // en side: part 00000, key = 91805
  private int enTermDocVector1Id = 91805;
  private ImmutableMap<String, Float> enTermDocVector1 = ImmutableMap.of(
      "clutter", 0.043639377f, "zoom", 0.060861073f, "portray", 0.022965258f, "refer", -0.0062234555f);

  // en side: part 00010, key = 137938
  private int enTermDocVector2Id = 137938;
  private ImmutableMap<String, Float> enTermDocVector2 = ImmutableMap.of(
      "histor", 0.018175913f, "vigilant", 0.11764987f, "augment", 0.04146363f, "time", 0.01755069f);

  // en side: part 00002, key = 148600
  private int enIntDocVector1Id = 148600;
  private ImmutableMap<Integer, Float> enIntDocVector1 =
    ImmutableMap.of(9767, 0.09995478f, 9855, 0.09942109f, 3265, 0.049947068f, 10928, 0.11828814f);

  // en side: part 00011, key = 181342
  private int enIntDocVector2Id = 181342;
  private ImmutableMap<Integer, Float> enIntDocVector2 =
    ImmutableMap.of(9788, 0.013477361f, 4393, 0.033166625f, 6576, 0.11134256f, 32, 0.07029859f);

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20121117-pages-articles.xml";
  private static final String dewikiRepacked = tmp + "/dewiki-20121117.repacked";
  private static final String dewikiEn = tmp + "/dewiki.en";

  // de side: part 00010, key = 1000010078
  private int deTermDocVector1Id = 1000010078;
  private ImmutableMap<String, Float> deTermDocVector1 = ImmutableMap.of(
      "total", 0.0029225545f, "2008", 0.032985184f, "...", 0.014565759f, "histor", 0.021209072f);
  // de side: part 00000, key = 1000960467
  private int deTermDocVector2Id = 1000960467;
  private ImmutableMap<String, Float> deTermDocVector2 = ImmutableMap.of(
      "profession", 0.020181179f, "role", 0.0075097345f, "seminar", 0.13284045f, "categori", 0.08937906f);

  // de side: part 00002, key = 1000131394
  private int deIntDocVector1Id = 1000131394;
  private ImmutableMap<Integer, Float> deIntDocVector1 =
    ImmutableMap.of(3281, 0.002396266f, 6585, 0.029588304f, 21, 0.009042849f, 3264, 0.0037004524f);

  // de side: part 00011, key = 1000210390
  private int deIntDocVector2Id = 1000210390;
  private ImmutableMap<Integer, Float> deIntDocVector2 =
    ImmutableMap.of(6585, 0.005131551f, 1141, 0.043817703f, 2210, 0.0308043f, 11912, 0.07676228f);

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
        "-e_stopword=" + tokenizerPath + "/en.stop"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + enwikiEn + "/wt-term-doc-vectors", 
        "-output=" + enwikiEn + "/test_wt-term-doc-vectors", 
        "-keys=" + enTermDocVector1Id + "," + enTermDocVector2Id, 
    "-valueclass=edu.umd.cloud9.io.map.HMapSFW"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + enwikiEn + "/wt-int-doc-vectors", 
        "-output=" + enwikiEn + "/test_wt-int-doc-vectors", 
        "-keys=" + enIntDocVector1Id + "," + enIntDocVector2Id, 
    "-valueclass=ivory.core.data.document.WeightedIntDocVector"};
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
    System.out.println("enTermDocVector1\n"+value);
    reader.next(key, value);
    verifyTermDocVector(enTermDocVector2, value);
    System.out.println("enTermDocVector2\n"+value);
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
    System.out.println("enIntDocVector1\n"+value);
    reader.next(key, value);
    verifyIntDocVector(enIntDocVector2, value);
    System.out.println("enIntDocVector2\n"+value);
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
    "-valueclass=edu.umd.cloud9.io.map.HMapSFW"};
    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.integration.wikipedia.SearchSequenceFiles.class.getCanonicalName(), libjars,
        "-input=" + dewikiEn + "/wt-int-doc-vectors", 
        "-output=" + dewikiEn + "/test_wt-int-doc-vectors", 
        "-keys=" + deIntDocVector1Id + "," + deIntDocVector2Id, 
    "-valueclass=ivory.core.data.document.WeightedIntDocVector"};
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
