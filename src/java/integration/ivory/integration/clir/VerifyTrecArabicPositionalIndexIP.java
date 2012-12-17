package ivory.integration.clir;

import static org.junit.Assert.assertTrue;
import ivory.core.eval.Qrels;
import ivory.core.tokenize.LuceneArabicAnalyzer;
import ivory.integration.IntegrationUtils;
import ivory.regression.coling2012.EnAr_TREC02;
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

public class VerifyTrecArabicPositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyTrecArabicPositionalIndexIP.class);

  private Path collectionPath = new Path("/shared/collections/clir/trec/ldc2001t55.ar-cleaned.xml");
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
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "jsap"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));
    
    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-ar.en"),
        new Path(index + "/vocab.en-ar.en"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-ar.ar"),
        new Path(index + "/vocab.en-ar.ar"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/ttable.en-ar"),
        new Path(index + "/ttable.en-ar"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/ar-token.bin"),
        new Path(index + "/ar-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en-token.bin"),
        new Path(index + "/en-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/ar.stop.stemmed"),
        new Path(index + "/ar.stop.stemmed"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en.stop.stemmed"),
        new Path(index + "/en.stop.stemmed"));
    fs.copyFromLocalFile(false, true, new Path("data/en-ar.trec02/grammar.en-ar.trec02"),
        new Path(index + "/grammar.en-ar.trec02"));
    fs.copyFromLocalFile(false, true, new Path("data/en-ar.trec02/queries.en-ar.k10.trec02.xml"),
        new Path(index + "/queries.en-ar.k10.trec02.xml"));
  
    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessTrecForeign.class.getCanonicalName(), libjars,
        "-input=" + collectionPath.toString(), "-index=" + index, 
        "-lang=ar" , "-tokenizerclass=" + LuceneArabicAnalyzer.class.getCanonicalName(),
        "-tokenizermodel=" + index + "/ar-token.bin", "-name=TREC2001-02.Arabic" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), libjars,
        "-index=" + index, "-indexPartitions=10", "-positionalIndexIP" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    // Done with indexing, now do retrieval run.
    conf = RunQueryEngine.parseArgs(new String[] {
        "-index=" + index,
        "-queries_path=" + index + "/queries.en-ar.k10.trec02.xml",
        "-run=en-ar.gridbest",
        "-query_type=mtN",
        "-doc_lang=ar",
        "-query_lang=en", 
        "-doc_tokenizer=" + index + "/ar-token.bin",
        "-query_tokenizer=" + index + "/en-token.bin",
        "-query_vocab=" + index + "/vocab.en-ar.en",
        "-doc_vocab=" + index + "/vocab.en-ar.ar",
        "-f2eProbs=" + index + "/ttable.en-ar",
        "-LexProbThreshold=0.005", 
        "-CumProbThreshold=0.95",  
        "-mt_weight=0",
        "-scfg_weight=1",
        "-bitext_weight=0",
        "-token_weight=1",
        "-phrase_weight=0",
        "-scfg_path=" + index + "/grammar.en-ar.trec02",
        "-kBest=10", 
        "-doc_stemmed_stopwordlist=" + index + "/ar.stop.stemmed",
        "-query_stemmed_stopwordlist=" + index + "/en.stop.stemmed"
    }, fs, conf);
    QueryEngine qr = new QueryEngine();
    qr.init(conf, fs);

    long start = System.currentTimeMillis();
    qr.runQueries(conf);
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    EnAr_TREC02.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/en-ar.trec02/qrels.en-ar.trec02.txt"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyTrecArabicPositionalIndexIP.class);
  }
}
