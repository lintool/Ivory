package ivory.regression.sigir2013.moses;

import ivory.core.eval.Qrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.retrieval.Constants;
import ivory.sqe.retrieval.QueryEngine;
import ivory.sqe.retrieval.RunQueryEngine;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;
import com.google.common.collect.Maps;
import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.map.HMapSFW;

public class EnFr_CLEF06 {
  private static final Logger LOG = Logger.getLogger(EnFr_CLEF06.class);
  private QueryEngine qe;
  private static String PATH = "en-fr.clef06";
  private static String LANGUAGE = "fr";
  private static Map<Integer,float[]> expectedMAPs = new HashMap<Integer,float[]>();{ 
    expectedMAPs.put(0, new float[]{0.2620f, 0.2421f, 0.2820f});   // "one2none" -> phrase,1best,10best
    expectedMAPs.put(1, new float[]{0.2567f, 0.2966f, 0.3002f});   // "one2one" -> phrase,1best,10best
    expectedMAPs.put(2, new float[]{0.2638f, 0.2967f, 0.2894f});   // "one2many" -> phrase,1best,10best
  };
  private static float expectedTokenMAP = 0.2617f;
  private static int numTopics = 50;

  private static Map<Integer, String[]> phrase_AP = new HashMap<Integer, String[]>();
  {
    phrase_AP.put(0, new String[] {
        "338", "0.1007","339", "0.4868","332", "0.0","333", "0.8698","330", "0.6","331", "0.2193","336", "0.1","337", "0.6275","334", "0.1769","335", "0.5269","349", "0.3468","302", "0.2078","301", "0.2818","304", "0.2678","303", "0.1846","306", "0.0257","341", "0.4251","305", "0.1904","342", "0.018","308", "0.1","343", "0.1126","307", "0.1181","344", "0.3454","345", "0.2396","309", "0.0142","346", "0.0677","347", "0.0589","348", "0.5812","340", "0.1884","318", "0.3877","319", "0.3713","316", "0.3979","317", "0.0242","314", "0.0723","315", "0.2524","312", "6.0E-4","313", "0.3392","310", "0.0030","311", "0.0887","327", "0.4628","328", "0.4485","329", "0.827","323", "0.1092","324", "0.0","325", "0.0165","326", "0.7667","320", "0.0458","350", "0.4277","321", "0.4072","322", "0.1687",
    });
    phrase_AP.put(1, new String[] {
        "338", "0.0453","339", "0.4915","332", "0.0","333", "0.8702","330", "0.55","331", "0.214","336", "0.1","337", "0.6084","334", "0.375","335", "0.4991","349", "0.3477","302", "0.206","301", "0.2857","304", "0.2665","303", "0.1973","306", "0.0116","341", "0.3669","305", "0.1987","342", "0.0209","308", "0.0985","343", "0.1135","307", "0.1228","344", "0.3119","345", "0.2265","309", "0.0142","346", "0.0529","347", "0.0437","348", "0.5874","340", "0.189","318", "0.4024","319", "0.4094","316", "0.3996","317", "0.0273","314", "0.0749","315", "0.1915","312", "5.0E-4","313", "0.1363","310", "0.0030","311", "0.0778","327", "0.4619","328", "0.4536","329", "0.825","323", "0.1288","324", "0.0","325", "0.0158","326", "0.7576","320", "0.0474","350", "0.4342","321", "0.4103","322", "0.1636",
    });
    phrase_AP.put(2, new String[] {
        "338", "0.1155","339", "0.4918","332", "0.0","333", "0.871","330", "0.55","331", "0.212","336", "0.1111","337", "0.6192","334", "0.3205","335", "0.5108","349", "0.3434","302", "0.2078","301", "0.2824","304", "0.2626","303", "0.1923","306", "0.0444","341", "0.4348","305", "0.1986","342", "0.0178","308", "0.0518","343", "0.1147","307", "0.1246","344", "0.3089","345", "0.2408","309", "0.0141","346", "0.0746","347", "0.0584","348", "0.5824","340", "0.1963","318", "0.386","319", "0.3569","316", "0.3979","317", "0.0223","314", "0.0747","315", "0.2627","312", "6.0E-4","313", "0.3714","310", "0.0030","311", "0.076","327", "0.4596","328", "0.4503","329", "0.8305","323", "0.1079","324", "0.0","325", "0.0165","326", "0.7667","320", "0.0449","350", "0.4277","321", "0.415","322", "0.1649",
    });
  };

  private static Map<Integer, String[]> Nbest_AP = new HashMap<Integer, String[]>();
  {
    Nbest_AP.put(0, new String[] {
        "338", "0.1695","339", "0.5331","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.2471","335", "0.3373","349", "0.3257","302", "0.1983","301", "0.4313","304", "0.361","303", "0.1883","306", "0.0484","341", "0.0034","305", "0.2119","342", "0.3465","308", "0.2493","343", "0.103","307", "0.1236","344", "0.2477","345", "0.2447","309", "0.013","346", "0.0213","347", "0.0591","348", "0.5655","340", "0.1542","318", "0.5071","319", "0.5708","316", "0.3926","317", "0.0996","314", "0.0732","315", "0.3695","312", "0.2193","313", "0.355","310", "0.0084","311", "0.0512","327", "0.0507","328", "0.4049","329", "0.7542","323", "0.0296","324", "0.0","325", "0.0070","326", "0.7436","320", "0.0591","350", "0.4134","321", "0.4218","322", "0.0963",
    });
    Nbest_AP.put(1, new String[] {
        "338", "0.1695","339", "0.5331","332", "0.0","333", "0.8714","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.3048","349", "0.3257","302", "0.1983","301", "0.4313","304", "0.361","303", "0.1883","306", "0.0484","341", "0.0034","305", "0.2129","342", "0.3465","308", "0.2712","343", "0.103","307", "0.1236","344", "0.3653","345", "0.2462","309", "0.013","346", "0.0175","347", "0.0591","348", "0.5655","340", "0.1902","318", "0.5071","319", "0.5708","316", "0.3926","317", "0.1557","314", "0.0716","315", "0.3695","312", "0.2193","313", "0.3534","310", "0.0084","311", "0.0512","327", "0.4736","328", "0.4049","329", "0.7542","323", "0.0296","324", "0.0","325", "0.0090","326", "0.7436","320", "0.0591","350", "0.4134","321", "0.4218","322", "0.1679",
    });
    Nbest_AP.put(2, new String[] {
        "338", "0.1695","339", "0.5331","332", "0.0","333", "0.8714","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.3203","349", "0.3257","302", "0.1983","301", "0.4313","304", "0.361","303", "0.1883","306", "0.0484","341", "0.0034","305", "0.2152","342", "0.3465","308", "0.2576","343", "0.103","307", "0.1236","344", "0.3023","345", "0.2437","309", "0.013","346", "0.0243","347", "0.0591","348", "0.5655","340", "0.2534","318", "0.5071","319", "0.5718","316", "0.3926","317", "0.1002","314", "0.0702","315", "0.3695","312", "0.2193","313", "0.3586","310", "0.0084","311", "0.0512","327", "0.0507","328", "0.4049","329", "0.7542","323", "0.0296","324", "0.0","325", "0.0070","326", "0.7436","320", "0.0591","350", "0.4134","321", "0.4218","322", "0.0963",
    });
  }

  private static Map<Integer, String[]> Onebest_AP = new HashMap<Integer, String[]>();
  {
    Onebest_AP.put(0, new String[] {
        "338", "0.1263","339", "0.4954","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.5469","334", "0.2471","335", "0.3373","349", "0.3281","302", "0.1983","301", "0.4308","304", "0.361","303", "0.1883","306", "0.0484","341", "0.0034","305", "0.0010","342", "0.3465","308", "0.3853","343", "0.0333","307", "0.0525","344", "0.0643","345", "0.2443","309", "0.013","346", "0.0233","347", "0.059","348", "0.549","340", "0.0","318", "0.5062","319", "0.5752","316", "0.3894","317", "0.1043","314", "0.0447","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.0477","328", "0.2211","329", "0.7542","323", "4.0E-4","324", "0.0","325", "0.0298","326", "0.7778","320", "0.0263","350", "0.4028","321", "0.4218","322", "0.0184",
    });
    Onebest_AP.put(1, new String[] {
        "338", "0.1263","339", "0.4954","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.3373","349", "0.3281","302", "0.1983","301", "0.4308","304", "0.361","303", "0.1883","306", "0.0484","341", "0.0034","305", "0.1801","342", "0.3465","308", "0.3853","343", "0.103","307", "0.0525","344", "0.3886","345", "0.2443","309", "0.013","346", "0.0233","347", "0.059","348", "0.549","340", "0.227","318", "0.5062","319", "0.5752","316", "0.3894","317", "0.1043","314", "0.0447","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.5084","328", "0.4049","329", "0.7542","323", "4.0E-4","324", "0.0","325", "0.0065","326", "0.7778","320", "0.0263","350", "0.4028","321", "0.4218","322", "0.0963",
    });
    Onebest_AP.put(2, new String[] {
        "338", "0.1263","339", "0.4954","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.3373","349", "0.3281","302", "0.1983","301", "0.4308","304", "0.361","303", "0.1883","306", "0.0484","341", "0.0034","305", "0.1801","342", "0.3465","308", "0.3853","343", "0.103","307", "0.0525","344", "0.3886","345", "0.2443","309", "0.013","346", "0.0233","347", "0.059","348", "0.549","340", "0.2352","318", "0.5062","319", "0.5752","316", "0.3894","317", "0.1043","314", "0.0447","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.5084","328", "0.4049","329", "0.7542","323", "4.0E-4","324", "0.0","325", "0.0065","326", "0.7778","320", "0.0263","350", "0.4028","321", "0.4218","322", "0.0963",
    });
  }

  private static String[] baseline_token_AP = new String[] {
    "338", "0.0336","339", "0.4941","332", "0.0","333", "0.8719","330", "0.6","331", "0.2255","336", "0.5","337", "0.6051","334", "0.4261","335", "0.37","349", "0.3297","302", "0.2168","301", "0.4115","304", "0.2738","303", "0.2342","306", "0.0442","341", "0.4332","305", "0.1813","342", "0.0641","308", "0.2852","343", "0.1029","307", "0.1168","344", "0.3045","345", "0.3336","309", "0.0134","346", "0.0499","347", "0.0647","348", "0.5605","340", "0.1798","318", "0.4432","319", "0.5609","316", "0.3711","317", "0.0334","314", "0.0666","315", "0.1825","312", "0.2237","313", "0.2365","310", "0.0033","311", "0.0668","327", "0.225","328", "0.4386","329", "0.7745","323", "0.0775","324", "0.0","325", "0.0104","326", "0.0","320", "0.0707","350", "0.4197","321", "0.4164","322", "0.1389",
  };

  //  private static String[] Gridbest_AP = phrase_AP_one2none;

  public EnFr_CLEF06() {
    super();
    qe = new QueryEngine();
  }

  @Test
  public void runRegressions() throws Exception {
    runRegression(0);   // "one2none"
    runRegression(1);   // "one2one"
    runRegression(2);   // "one2many"
    verifyAllResults(qe.getModels(), qe.getAllResults(), qe.getDocnoMapping(),
        new Qrels("data/" + PATH + "/qrels." + PATH + ".txt"));
  }

  public void runRegression(int heuristic) throws Exception {
    /////// baseline-token
    Configuration conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".token.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", 
        "--unknown", "data/" + PATH + "/moses/10.unk"});
    FileSystem fs = FileSystem.getLocal(conf);

    conf.setBoolean(Constants.Quiet, true);
        
    // no need to repeat token-based case for other heuristics
    if (heuristic == 0) {
      qe.init(conf, fs);
      qe.runQueries(conf);
    }
    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".1best.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans1-filtered.xml", "--one2many", heuristic + "",
        "--unknown", "data/" + PATH + "/moses/1.unk"});

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// phrase

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".phrase.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "",
        "--unknown", "data/" + PATH + "/moses/10.unk"});

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".10best.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "",
        "--unknown", "data/" + PATH + "/moses/10.unk"});

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grid-best

    //    conf = RunQueryEngine.parseArgs(new String[] {
    //        "--xml", "data/" + PATH + "/run_en-ar.gridbest.xml",
    //        "--queries_path", "data/" + PATH + "/moses/title_en-ar-trans10-filtered.xml", "--one2many", heuristic + "",
    //    "--unknown", "data/" + PATH + "/moses/10.unk"});
    //
    //    qe.init(conf, fs);
    //    qe.runQueries(conf);
  }

  public static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

    Map<String, GroundTruth> g = Maps.newHashMap();

    g.put("en-" + LANGUAGE + ".token_10-0-100-100_0", new GroundTruth(Metric.AP, numTopics, baseline_token_AP, expectedTokenMAP));

    for (int heuristic=0; heuristic <= 2; heuristic++) {
      g.put("en-" + LANGUAGE + ".phrase_10-0-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, phrase_AP.get(heuristic), expectedMAPs.get(heuristic)[0]));
      g.put("en-" + LANGUAGE + ".1best_1-0-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, Onebest_AP.get(heuristic), expectedMAPs.get(heuristic)[1]));
      g.put("en-" + LANGUAGE + ".10best_10-100-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, Nbest_AP.get(heuristic), expectedMAPs.get(heuristic)[2]));
    }

    for (String model : models) {
      LOG.info("Verifying results of model \"" + model + "\"");

      g.get(model).verify(results.get(model), mapping, qrels);

      LOG.info("Done!");
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(EnFr_CLEF06.class);
  }

  public static void main(String[] args) {
    //    HMapSFW gridAPMap = array2Map(Gridbest_AP);
    HMapSFW tenbestAPMap = array2Map(Nbest_AP.get(2));
    HMapSFW onebestAPMap = array2Map(Onebest_AP.get(1));
    HMapSFW phraseAPMap = array2Map(phrase_AP.get(0));
    HMapSFW tokenAPMap = array2Map(baseline_token_AP);
    //    System.out.println(countNumberOfImprovedTopics(tokenAPMap, gridAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, tenbestAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, onebestAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, phraseAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, tokenAPMap));
    //    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, gridAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, tenbestAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, onebestAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, phraseAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, tokenAPMap));
  }

  private static int countNumberOfImprovedTopics(HMapSFW tokenAPMap, HMapSFW gridAPMap) {
    int cnt = 0;
    for (String key : tokenAPMap.keySet()) {
      float difference = gridAPMap.get(key) - tokenAPMap.get(key); 
      if ( difference > 0.001 ) {
        cnt++;
      }
    }
    return cnt;
  }

  private static int countNumberOfNegligibleTopics(HMapSFW tokenAPMap, HMapSFW gridAPMap) {
    int cnt = 0;
    for (String key : tokenAPMap.keySet()) {
      float difference = gridAPMap.get(key) - tokenAPMap.get(key); 
      if ( difference > -0.001 && difference < 0.001 ) {
        cnt++;
      }
    }
    return cnt;
  }

  private static HMapSFW array2Map(String[] array) {
    HMapSFW map = new HMapSFW();
    for ( int i = 0; i < array.length; i += 2 ) {
      map.put(array[i], Float.parseFloat(array[i+1]));
    }
    return map;
  }

}
