package ivory.regression.sigir2011;

import ivory.cascade.retrieval.CascadeBatchQueryRunner;
import ivory.core.eval.GradedQrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;

import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.Maps;

public class Wt10g_Cascade {
  private static final Logger LOG = Logger.getLogger(Wt10g_Cascade.class);

  private static String[] QL = new String[] {
      "501", "0.3011", "502", "0.2535", "503", "0.1479", "504", "0.7132", "505", "0.3732",
      "506", "0.1186", "507", "0.5530", "508", "0.3835", "509", "0.5171", "510", "0.8517",
      "511", "0.4330", "512", "0.3341", "513", "0.0435", "514", "0.2113", "515", "0.4027",
      "516", "0.0780", "517", "0.1242", "518", "0.2950", "519", "0.3927", "520", "0.1879",
      "521", "0.2466", "522", "0.4229", "523", "0.3672", "524", "0.1659", "525", "0.1637",
      "526", "0.0762", "527", "0.8291", "528", "0.7814", "529", "0.4496", "530", "0.6789",
      "531", "0.0613", "532", "0.5110", "533", "0.4432", "534", "0.0417", "535", "0.0974",
      "536", "0.2402", "537", "0.2198", "538", "0.5110", "539", "0.2710", "540", "0.1964",
      "541", "0.5270", "542", "0.0885", "543", "0.0448", "544", "0.5999", "545", "0.2713",
      "546", "0.1851", "547", "0.2474", "548", "0.6053", "549", "0.5831", "550", "0.3949" };

  private static String[] Cascade = new String[] {
      "501", "0.3193", "502", "0.3123", "503", "0.1036", "504", "0.7406", "505", "0.3852",
      "506", "0.1186", "507", "0.5553", "508", "0.2812", "509", "0.5533", "510", "0.8579",
      "511", "0.5177", "512", "0.3084", "513", "0.0435", "514", "0.2691", "515", "0.2837",
      "516", "0.0780", "517", "0.0767", "518", "0.3953", "519", "0.3367", "520", "0.1743",
      "521", "0.2970", "522", "0.4656", "523", "0.3672", "524", "0.1572", "525", "0.1921",
      "526", "0.0762", "527", "0.8431", "528", "0.8213", "529", "0.4364", "530", "0.6278",
      "531", "0.1826", "532", "0.5110", "533", "0.6656", "534", "0.0387", "535", "0.0981",
      "536", "0.2841", "537", "0.2176", "538", "0.5110", "539", "0.4368", "540", "0.1964",
      "541", "0.4812", "542", "0.0381", "543", "0.1420", "544", "0.5432", "545", "0.4074",
      "546", "0.2747", "547", "0.2521", "548", "0.5912", "549", "0.5653", "550", "0.3659" };

  private static String[] FeaturePrune = new String[] {
      "501", "0.3436", "502", "0.3493", "503", "0.1164", "504", "0.7549", "505", "0.3805",
      "506", "0.1065", "507", "0.5075", "508", "0.4076", "509", "0.5365", "510", "0.8958",
      "511", "0.5016", "512", "0.2170", "513", "0.0435", "514", "0.2926", "515", "0.2575",
      "516", "0.0780", "517", "0.1398", "518", "0.3797", "519", "0.2807", "520", "0.1995",
      "521", "0.2978", "522", "0.4562", "523", "0.3672", "524", "0.0819", "525", "0.2267",
      "526", "0.0762", "527", "0.7536", "528", "0.8213", "529", "0.4671", "530", "0.5008",
      "531", "0.2276", "532", "0.5110", "533", "0.8196", "534", "0.0401", "535", "0.0369",
      "536", "0.2756", "537", "0.0837", "538", "0.5110", "539", "0.4368", "540", "0.1964",
      "541", "0.4612", "542", "0.0279", "543", "0.1420", "544", "0.5066", "545", "0.4311",
      "546", "0.2232", "547", "0.2189", "548", "0.6053", "549", "0.4743", "550", "0.3614" };

  private static String[] AdaRank = new String[] {
      "501", "0.3237", "502", "0.3125", "503", "0.1061", "504", "0.7395", "505", "0.3852",
      "506", "0.1065", "507", "0.5580", "508", "0.2991", "509", "0.5184", "510", "0.8573",
      "511", "0.5177", "512", "0.3090", "513", "0.0435", "514", "0.2691", "515", "0.2813",
      "516", "0.0780", "517", "0.0767", "518", "0.3883", "519", "0.3182", "520", "0.1736",
      "521", "0.2978", "522", "0.4672", "523", "0.3672", "524", "0.1558", "525", "0.1917",
      "526", "0.0762", "527", "0.8423", "528", "0.8213", "529", "0.4352", "530", "0.6273",
      "531", "0.1842", "532", "0.5110", "533", "0.6656", "534", "0.0387", "535", "0.0997",
      "536", "0.2853", "537", "0.2176", "538", "0.5110", "539", "0.4368", "540", "0.1964",
      "541", "0.4794", "542", "0.0381", "543", "0.1420", "544", "0.5154", "545", "0.4081",
      "546", "0.2631", "547", "0.2521", "548", "0.5912", "549", "0.5970", "550", "0.3691" };

  @Test
  public void runRegression() throws Exception {
    Map<String, GroundTruth> g = Maps.newHashMap();
    g.put("Wt10g-QL", new GroundTruth("Wt10g-QL", Metric.NDCG20, 50, QL, 0.3407f));
    g.put("Wt10g-Cascade", new GroundTruth("Wt10g-Cascade", Metric.NDCG20, 50, Cascade, 0.3560f));
    g.put("Wt10g-AdaRank", new GroundTruth("Wt10g-AdaRank", Metric.NDCG20, 50, AdaRank, 0.3549f));
    g.put("Wt10g-FeaturePrune", new GroundTruth("Wt10g-FeaturePrune", Metric.NDCG20, 50,
        FeaturePrune, 0.3486f));

    GradedQrels qrels = new GradedQrels("data/wt10g/qrels.wt10g.all");

    String[] params = new String[] {
        "data/wt10g/run.wt10g.SIGIR2011.xml",
        "data/wt10g/queries.wt10g.501-550.xml" };

    FileSystem fs = FileSystem.getLocal(new Configuration());
    CascadeBatchQueryRunner qr = new CascadeBatchQueryRunner(params, fs);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();
    LOG.info("Total query time: " + (end - start) + "ms");

    for (String model : qr.getModels()) {
      LOG.info("Verifying results of model \"" + model + "\"");
      Map<String, Accumulator[]> results = qr.getResults(model);
      g.get(model).verify(results, qr.getDocnoMapping(), qrels);
      LOG.info("Done!");
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Wt10g_Cascade.class);
  }
}
