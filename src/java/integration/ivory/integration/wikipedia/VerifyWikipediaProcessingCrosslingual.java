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
      "total", 0.0521711f, "extern", 0.0045928364f, "side", 0.052283954f, "refer", -0.012635737f);

  // en side: part 00010, key = 138960
  private ImmutableMap<String, Float> enTermDocVector2 = ImmutableMap.of(
      "extern", 0.004580953f, "perspect", 0.097292185f, "deal", 0.07025129f, "devianc", 0.18621536f);

  // en side: part 00002, key = 150251
  private ImmutableMap<Integer, Float> enIntDocVector1 =
    ImmutableMap.of(35202, 0.033555865f, 34129, 0.053415705f, 27261, 0.039032873f, 34140, 0.08449726f);

  // en side: part 00011, key = 184192
  private ImmutableMap<Integer, Float> enIntDocVector2 =
    ImmutableMap.of(8777, 0.103838794f, 73827, 0.07746056f, 9147, -0.016266173f, 44992, 0.11387909f);

  private static final String dewikiPath = 
    "/shared/collections/wikipedia/raw/dewiki-20110131-pages-articles.xml";
  private static final String dewikiRepacked = tmp + "/dewiki-20110131.repacked";
  private static final String dewikiEn = tmp + "/dewiki.en";

  // de side: part 00000, key = 1001242228
  private ImmutableMap<String, Float> deTermDocVector1 = ImmutableMap.of(
      "foundat", 0.0034755506f, "external", 0.024032094f, "programm", 0.08090772f, "htv", 0.26859477f);

  // de side: part 00010, key = 1000034130
  private ImmutableMap<String, Float> deTermDocVector2 = ImmutableMap.of(
      "ombudswomen", 0.18138693f, "profession", 0.039470334f, "ascrib", 0.016959665f, "great", 0.0019023749f);

  // de side: part 00002, key = 1000943946
  private ImmutableMap<Integer, Float> deIntDocVector1 =
    ImmutableMap.of(34132, 0.004382227f, 26285, 0.009954007f, 33034, 0.028362243f, 66084, 0.07469488f);

  // de side: part 00011, key = 1000347854
  private ImmutableMap<Integer, Float> deIntDocVector2 =
    ImmutableMap.of(51505, 0.33379892f, 16336, 0.21256998f, 80281, 0.7150921f, 78262, 0.37479895f);

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

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        enwikiEn, enwikiPath, enwikiRepacked,
        ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "en",
        vocabPath + "/en-token.bin", vocabPath + "/vocab.en-de.en"});
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

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessWikipedia.main(new String[] { libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        dewikiEn, dewikiPath, dewikiRepacked,
        ivory.core.tokenize.OpenNLPTokenizer.class.getCanonicalName(), "de",
        vocabPath + "/de-token.bin",
        vocabPath + "/vocab.de-en.de", vocabPath + "/vocab.de-en.en", vocabPath + "/ttable.de-en",
        vocabPath + "/vocab.en-de.en", vocabPath + "/vocab.en-de.de", vocabPath + "/ttable.en-de"});
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
