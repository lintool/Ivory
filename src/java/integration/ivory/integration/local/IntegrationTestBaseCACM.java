package ivory.integration.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import ivory.app.PreprocessCollection;
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.integration.IntegrationUtils;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public abstract class IntegrationTestBaseCACM {
  private static final Logger LOG = Logger.getLogger(IntegrationTestBaseCACM.class);
  private static final Path collectionPath = new Path("data/cacm/cacm-collection.xml.gz");

  public void runBuildIndex(String index, String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

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
    jars.add(IntegrationUtils.getJar("lib", "kamikaze"));
    jars.add(IntegrationUtils.getJar("lib", "lintools-datatypes-1.0.0"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] cmdArgs = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.PreprocessTrecCollection.class.getCanonicalName(),
        IntegrationUtils.LOCAL_ARGS, libjars,
        "-" + PreprocessCollection.COLLECTION_NAME, "CACM",
        "-" + PreprocessCollection.COLLECTION_PATH, collectionPath.toString(),
        "-" + PreprocessCollection.INDEX_PATH, index };

    IntegrationUtils.exec(Joiner.on(" ").join(cmdArgs));

    cmdArgs = (String[]) ArrayUtils.addAll(new String[] { 
        "hadoop jar", IntegrationUtils.getJar("dist", "ivory"),
        ivory.app.BuildIndex.class.getCanonicalName(), IntegrationUtils.LOCAL_ARGS, libjars}, args);

    IntegrationUtils.exec(Joiner.on(" ").join(cmdArgs));

    // Done with indexing, now do retrieval run.
    String[] params = new String[] {
        "data/cacm/run.cacm.xml",
        "data/cacm/queries.cacm.xml" };

    BatchQueryRunner qr = new BatchQueryRunner(params, fs, index);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
        new Qrels("data/cacm/qrels.cacm.txt"));
  }

  static final ImmutableMap<String, Float> DIR_BASE_AP = new ImmutableMap.Builder<String, Float>()
      .put("1",  0.0648f).put("2",  0.6667f).put("3",  0.0992f).put("4",  0.1031f).put("5",  0.0873f)
      .put("6",  0.4111f).put("7",  0.2920f).put("8",  0.2749f).put("9",  0.1370f).put("10", 0.5866f)
      .put("11", 0.4833f).put("12", 0.4430f).put("13", 0.1831f).put("14", 0.1411f).put("15", 0.1509f)
      .put("16", 0.0595f).put("17", 0.1441f).put("18", 0.0711f).put("19", 0.3650f).put("20", 0.7667f)
      .put("21", 0.3003f).put("22", 0.7008f).put("23", 0.1396f).put("24", 0.1293f).put("25", 0.2931f)
      .put("26", 0.4900f).put("27", 0.2274f).put("28", 0.8103f).put("29", 0.3927f).put("30", 0.2476f)
      .put("31", 1.0000f).put("32", 0.6746f).put("33", 0.2000f).put("36", 0.3216f).put("37", 0.1385f)
      .put("38", 0.2331f).put("39", 0.2417f).put("40", 0.2087f).put("42", 0.0708f).put("43", 0.2513f)
      .put("44", 0.1688f).put("45", 0.3740f).put("48", 0.0595f).put("49", 0.3322f).put("57", 1.0000f)
      .put("58", 0.2976f).put("59", 0.3167f).put("60", 0.2203f).put("61", 0.5400f).put("62", 0.0545f)
      .put("63", 0.5209f).put("64", 0.0010f).build();

  static final ImmutableMap<String, Float> DIR_BASE_P10 = new ImmutableMap.Builder<String, Float>()
      .put("1",  0.0000f).put("2",  0.2000f).put("3",  0.1000f).put("4",  0.1000f).put("5",  0.1000f)
      .put("6",  0.3000f).put("7",  0.6000f).put("8",  0.2000f).put("9",  0.2000f).put("10", 0.9000f)
      .put("11", 0.6000f).put("12", 0.2000f).put("13", 0.2000f).put("14", 0.2000f).put("15", 0.2000f)
      .put("16", 0.2000f).put("17", 0.2000f).put("18", 0.1000f).put("19", 0.5000f).put("20", 0.3000f)
      .put("21", 0.2000f).put("22", 0.7000f).put("23", 0.1000f).put("24", 0.2000f).put("25", 0.5000f)
      .put("26", 0.8000f).put("27", 0.6000f).put("28", 0.4000f).put("29", 0.3000f).put("30", 0.2000f)
      .put("31", 0.2000f).put("32", 0.2000f).put("33", 0.1000f).put("36", 0.5000f).put("37", 0.2000f)
      .put("38", 0.3000f).put("39", 0.3000f).put("40", 0.3000f).put("42", 0.0000f).put("43", 0.6000f)
      .put("44", 0.3000f).put("45", 0.5000f).put("48", 0.1000f).put("49", 0.2000f).put("57", 0.1000f)
      .put("58", 0.6000f).put("59", 0.7000f).put("60", 0.3000f).put("61", 0.9000f).put("62", 0.0000f)
      .put("63", 0.5000f).put("64", 0.0000f).build();

  static final ImmutableMap<String, Float> BM25_BASE_AP = new ImmutableMap.Builder<String, Float>()
      .put("1",  0.0986f).put("2",  0.6667f).put("3",  0.1969f).put("4",  0.1147f).put("5",  0.0908f)
      .put("6",  0.3194f).put("7",  0.3278f).put("8",  0.2869f).put("9",  0.1180f).put("10", 0.5212f)
      .put("11", 0.4984f).put("12", 0.4402f).put("13", 0.1094f).put("14", 0.1305f).put("15", 0.1381f)
      .put("16", 0.0823f).put("17", 0.2006f).put("18", 0.0833f).put("19", 0.4212f).put("20", 0.8095f)
      .put("21", 0.2543f).put("22", 0.6604f).put("23", 0.0935f).put("24", 0.0794f).put("25", 0.3275f)
      .put("26", 0.4228f).put("27", 0.2148f).put("28", 0.7782f).put("29", 0.3509f).put("30", 0.2824f)
      .put("31", 0.3750f).put("32", 0.6720f).put("33", 0.0833f).put("36", 0.3687f).put("37", 0.1432f)
      .put("38", 0.1971f).put("39", 0.2048f).put("40", 0.1994f).put("42", 0.0683f).put("43", 0.2536f)
      .put("44", 0.1513f).put("45", 0.3893f).put("48", 0.0235f).put("49", 0.4152f).put("57", 1.0000f)
      .put("58", 0.2153f).put("59", 0.3775f).put("60", 0.2056f).put("61", 0.4790f).put("62", 0.0579f)
      .put("63", 0.4338f).put("64", 0.0000f).build();

  static final ImmutableMap<String, Float> BM25_BASE_P10 = new ImmutableMap.Builder<String, Float>()
      .put("1",  0.2000f).put("2",  0.2000f).put("3",  0.1000f).put("4",  0.1000f).put("5",  0.2000f)
      .put("6",  0.3000f).put("7",  0.6000f).put("8",  0.3000f).put("9",  0.2000f).put("10", 0.6000f)
      .put("11", 0.8000f).put("12", 0.2000f).put("13", 0.2000f).put("14", 0.2000f).put("15", 0.2000f)
      .put("16", 0.2000f).put("17", 0.3000f).put("18", 0.2000f).put("19", 0.5000f).put("20", 0.3000f)
      .put("21", 0.2000f).put("22", 0.7000f).put("23", 0.0000f).put("24", 0.3000f).put("25", 0.6000f)
      .put("26", 0.6000f).put("27", 0.3000f).put("28", 0.4000f).put("29", 0.3000f).put("30", 0.2000f)
      .put("31", 0.2000f).put("32", 0.2000f).put("33", 0.0000f).put("36", 0.4000f).put("37", 0.1000f)
      .put("38", 0.2000f).put("39", 0.3000f).put("40", 0.2000f).put("42", 0.0000f).put("43", 0.6000f)
      .put("44", 0.2000f).put("45", 0.6000f).put("48", 0.0000f).put("49", 0.3000f).put("57", 0.1000f)
      .put("58", 0.4000f).put("59", 0.7000f).put("60", 0.3000f).put("61", 0.7000f).put("62", 0.0000f)
      .put("63", 0.2000f).put("64", 0.0000f).build();

  private static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {
    Map<String, Map<String, Float>> AllModelsAPScores = Maps.newHashMap();
    AllModelsAPScores.put("cacm-dir-base", DIR_BASE_AP);
    AllModelsAPScores.put("cacm-bm25-base", BM25_BASE_AP);

    Map<String, Map<String, Float>> AllModelsP10Scores = Maps.newHashMap();
    AllModelsP10Scores.put("cacm-dir-base", DIR_BASE_P10);
    AllModelsP10Scores.put("cacm-bm25-base", BM25_BASE_P10);
    
    for (String model : models) {
      LOG.info("Verifying results of model \"" + model + "\"");
      verifyResults(model, results.get(model),
          AllModelsAPScores.get(model), AllModelsP10Scores.get(model), mapping, qrels);
      LOG.info("Done!");
    }
  }

  private static void verifyResults(String model, Map<String, Accumulator[]> results,
      Map<String, Float> apScores, Map<String, Float> p10Scores, DocnoMapping mapping,
      Qrels qrels) {
    float apSum = 0, p10Sum = 0;
    for (String qid : results.keySet()) {
      float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      apSum += ap;
      p10Sum += p10;

      LOG.info("verifying qid " + qid + " for model " + model);

      assertEquals(apScores.get(qid), ap, 10e-6);
      assertEquals(p10Scores.get(qid), p10, 10e-6);
    }

    float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 52f);
    float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / 52f);

    if (model.equals("cacm-dir-base")) {
      assertEquals(0.3171, MAP, 10e-5);
      assertEquals(0.3135, P10Avg, 10e-5);
    } else if (model.equals("cacm-bm25-base")) {
      assertEquals(0.2968, MAP, 10e-5);
      assertEquals(0.2923, P10Avg, 10e-5);
    }
  }
}
