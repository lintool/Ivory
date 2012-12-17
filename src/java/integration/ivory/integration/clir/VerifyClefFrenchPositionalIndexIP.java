package ivory.integration.clir;

import static org.junit.Assert.assertTrue;
import ivory.core.eval.Qrels;
import ivory.core.tokenize.OpenNLPTokenizer;
import ivory.integration.IntegrationUtils;
import ivory.regression.coling2012.EnFr_CLEF06;
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

public class VerifyClefFrenchPositionalIndexIP {
  private static final Logger LOG = Logger.getLogger(VerifyClefFrenchPositionalIndexIP.class);

  private Path collectionPath = new Path("/shared/collections/clir/clef/lemonde94-95+sda94-95.fr-cleaned.xml");
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
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "commons-cli"));
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    // Done with indexing, now do retrieval run.
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-fr.en"),
        new Path(index + "/vocab.en-fr.en"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/vocab.en-fr.fr"),
        new Path(index + "/vocab.en-fr.fr"));
    fs.copyFromLocalFile(false, true, new Path("data/vocab/ttable.en-fr"),
        new Path(index + "/ttable.en-fr"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/fr-token.bin"),
        new Path(index + "/fr-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en-token.bin"),
        new Path(index + "/en-token.bin"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/fr.stop.stemmed"),
        new Path(index + "/fr.stop.stemmed"));
    fs.copyFromLocalFile(false, true, new Path("data/tokenizer/en.stop.stemmed"),
        new Path(index + "/en.stop.stemmed"));
    fs.copyFromLocalFile(false, true, new Path("data/en-fr.clef06/grammar.en-fr.clef06"),
        new Path(index + "/grammar.en-fr.clef06"));
    fs.copyFromLocalFile(false, true, new Path("data/en-fr.clef06/queries.en-fr.k10.clef06.xml"),
        new Path(index + "/queries.en-fr.k10.clef06.xml"));
  
    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessTrecForeign.class.getCanonicalName(), libjars,
        "-input=" + collectionPath.toString(), "-index=" + index, 
        "-lang=fr" , "-tokenizerclass=" + OpenNLPTokenizer.class.getCanonicalName(),
        "-tokenizermodel=" + index + "/fr-token.bin", "-name=CLEF2006.French"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), libjars,
        "-index=" + index, "-indexPartitions=10", "-positionalIndexIP" };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    conf = RunQueryEngine.parseArgs(new String[] {
        "-index=" + index,
        "-queries_path=" + index + "/queries.en-fr.k10.clef06.xml",
        "-run=en-fr.gridbest",
        "-query_type=mtN",
        "-doc_lang=fr",
        "-query_lang=en", 
        "-doc_tokenizer=" + index + "/fr-token.bin",
        "-query_tokenizer=" + index + "/en-token.bin",
        "-query_vocab=" + index + "/vocab.en-fr.en",
        "-doc_vocab=" + index + "/vocab.en-fr.fr",
        "-f2eProbs=" + index + "/ttable.en-fr",
        "-LexProbThreshold=0.005", 
        "-CumProbThreshold=0.95",  
        "-mt_weight=0.3",
        "-scfg_weight=0.4",
        "-bitext_weight=0.3",
        "-token_weight=1",
        "-phrase_weight=0",
        "-scfg_path=" + index + "/grammar.en-fr.clef06",
        "-kBest=10", 
        "-doc_stemmed_stopwordlist=" + index + "/fr.stop.stemmed",
        "-query_stemmed_stopwordlist=" + index + "/en.stop.stemmed"
    }, fs, conf);
    QueryEngine qr = new QueryEngine();
    qr.init(conf, fs);

    long start = System.currentTimeMillis();
    qr.runQueries(conf);
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    EnFr_CLEF06.verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/en-fr.clef06/qrels.en-fr.clef06.txt"));

    LOG.info("Done!");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyClefFrenchPositionalIndexIP.class);
  }
}
