package ivory.integration;

import static org.junit.Assert.assertTrue;

import ivory.app.BuildIndex;
import ivory.app.PreprocessTrecForeign;
import ivory.core.eval.Qrels;
import ivory.core.tokenize.StanfordChineseTokenizer;
import ivory.regression.coling2012.EnZh_NTCIR8;
import ivory.sqe.retrieval.QueryEngine;
import ivory.sqe.retrieval.RunQueryEngine;

import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class VerifyNtcirChinesePositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyNtcirChinesePositionalIndexIP.class);

  private Path collectionPath = new Path("/shared/collections/clir/ntcir/gigaword-xin.2002-06.zh-cleaned.xml");
  private String index = this.getClass().getCanonicalName() + "-index";
  
  @Test
  public void runBuildIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    fs.delete(new Path(index), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
    jars.add(IntegrationUtils.getJar("lib", "guava-r09"));
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
    jars.add(IntegrationUtils.getJar("lib", "stanford-chinese-segmenter"));
    jars.add(IntegrationUtils.getJar("dist", "ivory"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));
    
    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-zh.en"),
        new Path(index + "/vocab.en-zh.en"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-zh.zh"),
        new Path(index + "/vocab.en-zh.zh"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/ttable.en-zh"),
        new Path(index + "/ttable.en-zh"));
    fs.copyFromLocalFile(false, true, new Path("data/en-zh.ntcir8/grammar.en-zh.ntcir8"),
        new Path(index + "/grammar.en-zh.ntcir8"));
    fs.copyFromLocalFile(false, true, new Path("data/en-zh.ntcir8/queries.en-zh.k10.ntcir8.xml"),
        new Path(index + "/queries.en-zh.k10.ntcir8.xml"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/zh-token.bin"),
        new Path(index + "/zh-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en-token.bin"),
        new Path(index + "/en-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en.stop.stemmed"),
        new Path(index + "/en.stop.stemmed"));

    PreprocessTrecForeign.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-input=" + collectionPath.toString(), "-index=" + index, 
        "-lang=zh" , "-tokenizerclass=" + StanfordChineseTokenizer.class.getCanonicalName(),
        "-tokenizermodel=" + index + "/zh-token.bin", "-name=NTCIR8.Chinese"
    });

    BuildIndex.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-index=" + index, "-indexPartitions=10", "-positionalIndexIP" });

    conf = RunQueryEngine.parseArgs(new String[] {
        "-index=" + index,
        "-queries_path=" + index + "/queries.en-zh.k10.ntcir8.xml",
        "-run=en-zh.gridbest",
        "-query_type=mtN",
        "-doc_lang=zh",
        "-query_lang=en", 
        "-doc_tokenizer=" + index + "/zh-token.bin",
        "-query_tokenizer=" + index + "/en-token.bin",
        "-query_vocab=" + index + "/vocab.en-zh.en",
        "-doc_vocab=" + index + "/vocab.en-zh.zh",
        "-f2eProbs=" + index + "/ttable.en-zh",
        "-LexProbThreshold=0.005", 
        "-CumProbThreshold=0.95",  
        "-mt_weight=0.2",
        "-scfg_weight=0.7",
        "-bitext_weight=0.1",
        "-token_weight=1",
        "-phrase_weight=0",
        "-scfg_path=" + index + "/grammar.en-zh.ntcir8",
        "-kBest=10", 
        "-query_stemmed_stopwordlist=" + index + "/en.stop.stemmed"
    }, fs, conf);
    QueryEngine qr = new QueryEngine();
    qr.init(conf, fs);

    long start = System.currentTimeMillis();
    qr.runQueries(conf);
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    EnZh_NTCIR8.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/en-zh.ntcir8/qrels.en-zh.ntcir8.txt"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyNtcirChinesePositionalIndexIP.class);
  }
}
