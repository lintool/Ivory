package ivory.regression.sigir2013.cdec;

import ivory.core.eval.Qrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
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
    expectedMAPs.put(0, new float[]{0.2916f, 0.2351f, 0.2947f});   // "one2none" -> phrase,1best,10best
    expectedMAPs.put(1, new float[]{0.2881f, 0.2756f, 0.3039f});   // "one2one" -> phrase,1best,10best
    expectedMAPs.put(2, new float[]{0.2965f, 0.2758f, 0.307f});   // "one2many" -> phrase,1best,10best
  };
  private static float expectedTokenMAP = 0.2617f;
  private static int numTopics = 50;

  private static Map<Integer, String[]> phrase_AP = new HashMap<Integer, String[]>();
  {
    phrase_AP.put(0, new String[] {
        "338", "0.0626","339", "0.4926","332", "0.0","333", "0.87","330", "0.6","331", "0.2202","336", "0.1429","337", "0.6079","334", "0.1542","335", "0.5335","349", "0.3377","302", "0.2526","301", "0.4408","304", "0.2722","303", "0.2131","341", "0.3835","306", "0.0474","305", "0.2105","342", "0.3384","343", "0.11","308", "0.2575","344", "0.3351","307", "0.1218","345", "0.2614","346", "0.0305","309", "0.0127","347", "0.0718","348", "0.5596","340", "0.1738","318", "0.505","319", "0.4936","316", "0.4007","317", "0.0789","314", "0.0763","315", "0.3548","312", "0.241","313", "0.3681","310", "0.0063","311", "0.0699","327", "0.7041","328", "0.448","329", "0.8094","323", "0.065","324", "0.0","325", "0.0177","326", "0.7436","320", "0.0519","321", "0.4159","350", "0.4338","322", "0.1797",
    });
    phrase_AP.put(1, new String[] {
        "338", "0.03","339", "0.4853","332", "0.0","333", "0.8708","330", "0.6","331", "0.2195","336", "0.125","337", "0.5982","334", "0.4293","335", "0.3559","349", "0.3503","302", "0.2506","301", "0.4353","304", "0.2823","303", "0.2163","341", "0.2762","306", "0.0458","305", "0.2278","342", "0.3372","343", "0.1129","308", "0.1951","344", "0.3081","307", "0.1257","345", "0.2605","346", "0.0216","309", "0.0127","347", "0.0659","348", "0.5613","340", "0.1744","318", "0.5062","319", "0.5616","316", "0.4","317", "0.1168","314", "0.0736","315", "0.3603","312", "0.2316","313", "0.2151","310", "0.0056","311", "0.0712","327", "0.669","328", "0.4604","329", "0.7951","323", "0.134","324", "0.0","325", "0.0139","326", "0.7436","320", "0.0571","321", "0.4164","350", "0.4288","322", "0.1701",
    });
    phrase_AP.put(2, new String[] {
        "338", "0.071","339", "0.4926","332", "0.0","333", "0.8698","330", "0.6","331", "0.2201","336", "0.1429","337", "0.6055","334", "0.4133","335", "0.5272","349", "0.3347","302", "0.2522","301", "0.4343","304", "0.2668","303", "0.2115","341", "0.3584","306", "0.0477","305", "0.2214","342", "0.338","343", "0.1119","308", "0.2258","344", "0.3072","307", "0.1303","345", "0.2617","346", "0.0324","309", "0.0127","347", "0.0745","348", "0.561","340", "0.1906","318", "0.511","319", "0.4832","316", "0.4007","317", "0.1413","314", "0.0741","315", "0.361","312", "0.2409","313", "0.3605","310", "0.0063","311", "0.0677","327", "0.7031","328", "0.4594","329", "0.7951","323", "0.0648","324", "0.0","325", "0.0182","326", "0.7436","320", "0.052","321", "0.4189","350", "0.4347","322", "0.1719",
    });
  };

  private static Map<Integer, String[]> Nbest_AP = new HashMap<Integer, String[]>();
  {
    Nbest_AP.put(0, new String[] {
        "338", "0.1264","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.2471","335", "0.5359","349", "0.3281","302", "0.1963","301", "0.4281","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1539","342", "0.3465","343", "0.103","308", "0.3853","344", "0.2444","307", "0.0769","345", "0.2356","346", "0.0214","309", "0.013","347", "0.059","348", "0.549","340", "0.1772","318", "0.5062","319", "0.5163","316", "0.3195","317", "0.1633","314", "0.0681","315", "0.3695","312", "0.2329","313", "0.318","310", "0.0084","311", "0.0517","327", "0.6226","328", "0.2211","329", "0.7542","323", "0.0128","324", "0.0","325", "0.0076","326", "0.8095","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Nbest_AP.put(1, new String[] {
        "338", "0.1264","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.5403","349", "0.3281","302", "0.1963","301", "0.4281","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3621","307", "0.0769","345", "0.243","346", "0.0214","309", "0.013","347", "0.059","348", "0.549","340", "0.1739","318", "0.5062","319", "0.5496","316", "0.3195","317", "0.1633","314", "0.0692","315", "0.3695","312", "0.2329","313", "0.318","310", "0.0084","311", "0.0517","327", "0.4934","328", "0.4049","329", "0.7542","323", "0.0128","324", "0.0","325", "0.0076","326", "0.8095","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Nbest_AP.put(2, new String[] {
        "338", "0.1264","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.5403","349", "0.3281","302", "0.1963","301", "0.4281","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0769","345", "0.2356","346", "0.0214","309", "0.013","347", "0.059","348", "0.549","340", "0.2409","318", "0.5062","319", "0.5163","316", "0.3195","317", "0.1633","314", "0.0681","315", "0.3695","312", "0.2329","313", "0.318","310", "0.0084","311", "0.0517","327", "0.6226","328", "0.4049","329", "0.7542","323", "0.0128","324", "0.0","325", "0.0076","326", "0.8095","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
  }

  private static Map<Integer, String[]> Onebest_AP = new HashMap<Integer, String[]>();
  {
    Onebest_AP.put(0, new String[] {
        "338", "0.1263","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.2471","335", "0.5228","349", "0.3281","302", "0.1983","301", "0.3127","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.0010","342", "0.3465","343", "0.103","308", "0.3853","344", "0.2444","307", "0.0771","345", "0.2443","346", "0.0148","309", "0.013","347", "0.059","348", "0.549","340", "0.0","318", "0.5062","319", "0.0","316", "1.0E-4","317", "0.1302","314", "0.0682","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.0477","328", "0.2211","329", "0.7542","323", "0.0068","324", "0.0","325", "0.0049","326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0184",
    });
    Onebest_AP.put(1, new String[] {
        "338", "0.1263","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.4628","335", "0.5228","349", "0.3281","302", "0.1983","301", "0.3127","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0771","345", "0.4164","346", "0.0148","309", "0.013","347", "0.059","348", "0.549","340", "0.227","318", "0.5062","319", "0.3933","316", "1.0E-4","317", "0.1302","314", "0.0682","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.5084","328", "0.4049","329", "0.7542","323", "0.0068","324", "0.0","325", "0.0049","326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Onebest_AP.put(2, new String[] {
        "338", "0.1263","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.4628","335", "0.5228","349", "0.3281","302", "0.1983","301", "0.3127","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0771","345", "0.4164","346", "0.0148","309", "0.013","347", "0.059","348", "0.549","340", "0.2352","318", "0.5062","319", "0.3933","316", "1.0E-4","317", "0.1302","314", "0.0682","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.5084","328", "0.4049","329", "0.7542","323", "0.0068","324", "0.0","325", "0.0049","326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
  }

  private static String[] baseline_token_AP = new String[] {
    "338", "0.0336","339", "0.4941","332", "0.0","333", "0.8719","330", "0.6","331", "0.2255","336", "0.5","337", "0.6051","334", "0.4261","335", "0.37","349", "0.3297","302", "0.2168","301", "0.4115","304", "0.2738","303", "0.2342","341", "0.4332","306", "0.0442","305", "0.1813","342", "0.0641","343", "0.1029","308", "0.2852","344", "0.3045","307", "0.1168","345", "0.3336","346", "0.0499","309", "0.0134","347", "0.0647","348", "0.5605","340", "0.1798","318", "0.4432","319", "0.5609","316", "0.3711","317", "0.0334","314", "0.0666","315", "0.1825","312", "0.2237","313", "0.2365","310", "0.0033","311", "0.0668","327", "0.225","328", "0.4386","329", "0.7745","323", "0.0775","324", "0.0","325", "0.0104","326", "0.0","320", "0.0707","321", "0.4164","350", "0.4197","322", "0.1389",
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
        new Qrels("data/"+ PATH + "/qrels."+ PATH + ".txt"));
  }

  public void runRegression(int heuristic) throws Exception {
    /////// baseline-token
    Configuration conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".token.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "" });
    FileSystem fs = FileSystem.getLocal(conf);

    //    conf.setBoolean(Constants.Quiet, true);

    // no need to repeat token-based case for other heuristics
    if (heuristic == 0) {
      qe.init(conf, fs);
      qe.runQueries(conf);
    }
    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".1best.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans1-filtered.xml", "--one2many", heuristic + "" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// phrase

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".phrase.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".10best.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grid-best

    //    conf = RunQueryEngine.parseArgs(new String[] {
    //        "--xml", "data/en-ar.trec02/run_en-ar.gridbest.xml",
    //        "--queries_path", "data/en-ar.trec02/cdec/title_en-ar-trans10-filtered.xml", "--one2many", heuristic + "" });
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
