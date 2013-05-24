package ivory.integration.clir;

import static org.junit.Assert.assertTrue;
import ivory.core.eval.Qrels;
import ivory.core.tokenize.OpenNLPTokenizer;
import ivory.integration.IntegrationUtils;
import ivory.sqe.retrieval.QueryEngine;
import ivory.sqe.retrieval.RunQueryEngine;
import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class VerifyNtcirChinesePositionalIndexIP {

  private Path collectionPath = new Path("/shared/collections/clir/ntcir/gigaword-xin.2002-06.zh-cleaned.xml");
  private String index = this.getClass().getCanonicalName() + "-index";
  private static String PATH = "en-zh.ntcir8";
  private static String LANGUAGE = "zh";
  private static String MTMODEL = "cdec";
  private static int numTopics = 100;
  
  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);
    
    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);
     
    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "commons-cli"));
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));
    
    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-" + LANGUAGE + ".en"),
        new Path(index + "/vocab.en-" + LANGUAGE + ".en"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-" + LANGUAGE + "." + LANGUAGE + ""),
        new Path(index + "/vocab.en-" + LANGUAGE + "." + LANGUAGE + ""));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/ttable.en-" + LANGUAGE + ""),
        new Path(index + "/ttable.en-" + LANGUAGE + ""));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/" + LANGUAGE + "-token.bin"),
        new Path(index + "/" + LANGUAGE + "-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en-token.bin"),
        new Path(index + "/en-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/" + LANGUAGE + ".stop.stemmed"),
        new Path(index + "/" + LANGUAGE + ".stop.stemmed"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en.stop.stemmed"),
        new Path(index + "/en.stop.stemmed"));
    for (int i = 0; i < numTopics; i++) {
      fs.copyFromLocalFile(false, true, new Path("data/" + PATH + "/" + MTMODEL + ".grammar/grammar." + i),
        new Path(index + "/grammar." + i));
    }
    fs.copyFromLocalFile(false, true, new Path("data/" + PATH + "/" + MTMODEL + "/title_en-" 
        + LANGUAGE + "-trans10-filtered-integration.xml"),
        new Path(index + "/title_en-" + LANGUAGE + "-trans10-filtered.xml"));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessTrecForeign.class.getCanonicalName(), libjars,
        "-input=" + collectionPath.toString(), "-index=" + index, 
        "-lang=fr" , "-tokenizerclass=" + OpenNLPTokenizer.class.getCanonicalName(),
        "-tokenizermodel=" + index + "/" + LANGUAGE + "-token.bin", "-name=" 
        + VerifyClefFrenchPositionalIndexIP.class.getCanonicalName()};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), libjars,
        "-index=" + index, "-indexPartitions=10", "-positionalIndexIP" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));
     
    QueryEngine qr = new QueryEngine();

    for (int heuristic=0; heuristic<=2; heuristic++) {
      conf = RunQueryEngine.parseArgs(new String[] {
          "-index=" + index,
          "-queries_path=" + index + "/title_en-" + LANGUAGE + "-trans10-filtered.xml",
          "-run=en-" + LANGUAGE + ".interp",
          "-query_type=mtN",
          "-doc_lang=" + LANGUAGE + "",
          "-query_lang=en", 
          "-doc_tokenizer=" + index + "/" + LANGUAGE + "-token.bin",
          "-query_tokenizer=" + index + "/en-token.bin",
          "-query_vocab=" + index + "/vocab.en-" + LANGUAGE + ".en",
          "-doc_vocab=" + index + "/vocab.en-" + LANGUAGE + "." + LANGUAGE + "",
          "-f2eProbs=" + index + "/ttable.en-" + LANGUAGE + "",
          "-LexProbThreshold=0.005", 
          "-CumProbThreshold=0.95",  
          "-mt_weight=0.3",
          "-grammar_weight=0.4",
          "-bitext_weight=0.3",
          "-token_weight=1",
          "-phrase_weight=0",
          "-kBest=10", 
          "-doc_stemmed_stopwordlist=" + index + "/" + LANGUAGE + ".stop.stemmed",
          "-query_stemmed_stopwordlist=" + index + "/en.stop.stemmed",
          "--one2many=" + heuristic
      }, fs, conf);

      long start = System.currentTimeMillis();
      qr.init(conf, fs);
      qr.runQueries(conf);
      long end = System.currentTimeMillis();

      System.err.println("Total query time for heuristic " + heuristic + ":" + (end - start) + "ms");
    }
    ivory.regression.sigir2013.cdec.EnZh_NTCIR8.initialize();
    ivory.regression.sigir2013.cdec.EnZh_NTCIR8.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/" + PATH + "/qrels." + PATH+ ".txt"));

    System.err.println("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyNtcirChinesePositionalIndexIP.class);
  }
}