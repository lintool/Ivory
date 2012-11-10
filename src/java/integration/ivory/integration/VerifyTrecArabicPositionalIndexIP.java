package ivory.integration;

import static org.junit.Assert.assertTrue;
import ivory.app.BuildPositionalIndexIP;
import ivory.app.PreprocessTrecForeign;
import ivory.core.eval.Qrels;
import ivory.core.tokenize.LuceneArabicAnalyzer;
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
  private String arTokenizerFile = "/user/fture/data/token/ar-token.bin";
  private String enTokenizerFile = "/user/fture/data/token/en-token.bin";
  
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
    jars.add(IntegrationUtils.getJar("dist", "ivory"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-analyzers"));
    jars.add(IntegrationUtils.getJar("lib", "lucene-core"));
    
    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    PreprocessTrecForeign.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-input=" + collectionPath.toString(), "-index=" + index, 
        "-lang=ar" , "-tokenizerclass=" + LuceneArabicAnalyzer.class.getCanonicalName(),
        "-tokenizermodel=" + arTokenizerFile, "-name=TREC2001-02.Arabic"
    });
    BuildPositionalIndexIP.main(new String[] { libjars, IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        index, "10" });

    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-ar.en"),
        new Path(index + "/vocab.en-ar.en"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-ar.ar"),
        new Path(index + "/vocab.en-ar.ar"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/ttable.en-ar"),
        new Path(index + "/ttable.en-ar"));
    fs.copyFromLocalFile(false, true, new Path("data/en-ar.trec02/grammar.en-ar.trec02"),
        new Path(index + "/grammar.en-ar.trec02"));
    fs.copyFromLocalFile(false, true, new Path("data/en-ar.trec02/queries.en-ar.k10.trec02.xml"),
        new Path(index + "/queries.en-ar.k10.trec02.xml"));

    // Done with indexing, now do retrieval run.
    conf = RunQueryEngine.parseArgs(new String[] {
        "-index=" + index,
        "-queries_path=" + index + "/queries.en-ar.k10.trec02.xml",
        "-run=en-ar.gridbest",
        "-query_type=mtN",
        "-doc_lang=ar",
        "-query_lang=en", 
        "-doc_tokenizer=" + arTokenizerFile,
        "-query_tokenizer=" + enTokenizerFile,
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
        "-scfg_path=data/en-ar.trec02/grammar.en-ar.trec02",
        "-kBest=10", 
        "-doc_stemmed_stopwordlist=data/tokenizer/ar.stop.stemmed",
        "-query_stemmed_stopwordlist=data/tokenizer/en.stop.stemmed"
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
