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
import org.junit.Test;

import tl.lin.data.map.HMapStFW;

import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class EnFr_CLEF06 {
  private QueryEngine qe;
  private static String PATH = "en-fr.clef06";
  private static String LANGUAGE = "fr";
  private static int numTopics = 50;
  
  private static float expectedTokenMAP = 0.2617f;
  private static Map<Integer,float[]> expectedMAPs = new HashMap<Integer,float[]>();

  private static Map<Integer, String[]> grammar_AP = new HashMap<Integer, String[]>();
  private static Map<Integer, String[]> Nbest_AP = new HashMap<Integer, String[]>();
  private static Map<Integer, String[]> Onebest_AP = new HashMap<Integer, String[]>();
  private static Map<Integer, String[]> Interp_AP = new HashMap<Integer, String[]>();

  public static void initialize() {
    expectedMAPs.put(0, new float[]{0.2916f, 0.2351f, 0.2947f, 0.3112f});   // "one2none" -> grammar,1best,10best,interp
    expectedMAPs.put(1, new float[]{0.2881f, 0.2756f, 0.3039f, 0.3109f});   // "one2one" -> grammar,1best,10best,interp
    expectedMAPs.put(2, new float[]{0.2965f, 0.2758f, 0.307f, 0.3152f});   // "one2many" -> grammar,1best,10best,interp
    grammar_AP.put(0, new String[] {
        "338", "0.0626","339", "0.4926","332", "0.0","333", "0.87","330", "0.6","331", "0.2202","336", "0.1429","337", "0.6079","334", "0.1542","335", "0.5335","349", "0.3377","302", "0.2526","301", "0.4408","304", "0.2722","303", "0.2131","341", "0.3835","306", "0.0474","305", "0.2105","342", "0.3384","343", "0.11","308", "0.2575","344", "0.3351","307", "0.1218","345", "0.2614","346", "0.0305","309", "0.0127","347", "0.0718","348", "0.5596","340", "0.1738","318", "0.505","319", "0.4936","316", "0.4007","317", "0.0789","314", "0.0763","315", "0.3548","312", "0.241","313", "0.3681","310", "0.0063","311", "0.0699","327", "0.7041","328", "0.448","329", "0.8094","323", "0.065","324", "0.0","325", "0.0177","326", "0.7436","320", "0.0519","321", "0.4159","350", "0.4338","322", "0.1797",
    });
    grammar_AP.put(1, new String[] {
        "338", "0.03","339", "0.4853","332", "0.0","333", "0.8708","330", "0.6","331", "0.2195","336", "0.125","337", "0.5982","334", "0.4293","335", "0.3559","349", "0.3503","302", "0.2506","301", "0.4353","304", "0.2823","303", "0.2163","341", "0.2762","306", "0.0458","305", "0.2278","342", "0.3372","343", "0.1129","308", "0.1951","344", "0.3081","307", "0.1257","345", "0.2605","346", "0.0216","309", "0.0127","347", "0.0659","348", "0.5613","340", "0.1744","318", "0.5062","319", "0.5616","316", "0.4","317", "0.1168","314", "0.0736","315", "0.3603","312", "0.2316","313", "0.2151","310", "0.0056","311", "0.0712","327", "0.669","328", "0.4604","329", "0.7951","323", "0.134","324", "0.0","325", "0.0139","326", "0.7436","320", "0.0571","321", "0.4164","350", "0.4288","322", "0.1701",
    });
    grammar_AP.put(2, new String[] {
        "338", "0.071","339", "0.4926","332", "0.0","333", "0.8698","330", "0.6","331", "0.2201","336", "0.1429","337", "0.6055","334", "0.4133","335", "0.5272","349", "0.3347","302", "0.2522","301", "0.4343","304", "0.2668","303", "0.2115","341", "0.3584","306", "0.0477","305", "0.2214","342", "0.338","343", "0.1119","308", "0.2258","344", "0.3072","307", "0.1303","345", "0.2617","346", "0.0324","309", "0.0127","347", "0.0745","348", "0.561","340", "0.1906","318", "0.511","319", "0.4832","316", "0.4007","317", "0.1413","314", "0.0741","315", "0.361","312", "0.2409","313", "0.3605","310", "0.0063","311", "0.0677","327", "0.7031","328", "0.4594","329", "0.7951","323", "0.0648","324", "0.0","325", "0.0182","326", "0.7436","320", "0.052","321", "0.4189","350", "0.4347","322", "0.1719",
    });
    Nbest_AP.put(0, new String[] {
        "338", "0.1264","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.2471","335", "0.5359","349", "0.3281","302", "0.1963","301", "0.4281","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1539","342", "0.3465","343", "0.103","308", "0.3853","344", "0.2444","307", "0.0769","345", "0.2356","346", "0.0214","309", "0.013","347", "0.059","348", "0.549","340", "0.1772","318", "0.5062","319", "0.5163","316", "0.3195","317", "0.1633","314", "0.0681","315", "0.3695","312", "0.2329","313", "0.318","310", "0.0084","311", "0.0517","327", "0.6226","328", "0.2211","329", "0.7542","323", "0.0128","324", "0.0","325", "0.0076","326", "0.8095","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Nbest_AP.put(1, new String[] {
        "338", "0.1264","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.5403","349", "0.3281","302", "0.1963","301", "0.4281","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3621","307", "0.0769","345", "0.243","346", "0.0214","309", "0.013","347", "0.059","348", "0.549","340", "0.1739","318", "0.5062","319", "0.5496","316", "0.3195","317", "0.1633","314", "0.0692","315", "0.3695","312", "0.2329","313", "0.318","310", "0.0084","311", "0.0517","327", "0.4934","328", "0.4049","329", "0.7542","323", "0.0128","324", "0.0","325", "0.0076","326", "0.8095","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Nbest_AP.put(2, new String[] {
        "338", "0.1264","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628","335", "0.5403","349", "0.3281","302", "0.1963","301", "0.4281","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0769","345", "0.2356","346", "0.0214","309", "0.013","347", "0.059","348", "0.549","340", "0.2409","318", "0.5062","319", "0.5163","316", "0.3195","317", "0.1633","314", "0.0681","315", "0.3695","312", "0.2329","313", "0.318","310", "0.0084","311", "0.0517","327", "0.6226","328", "0.4049","329", "0.7542","323", "0.0128","324", "0.0","325", "0.0076","326", "0.8095","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Onebest_AP.put(0, new String[] {
        "338", "0.1263","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.2471","335", "0.5228","349", "0.3281","302", "0.1983","301", "0.3127","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.0010","342", "0.3465","343", "0.103","308", "0.3853","344", "0.2444","307", "0.0771","345", "0.2443","346", "0.0148","309", "0.013","347", "0.059","348", "0.549","340", "0.0","318", "0.5062","319", "0.0","316", "1.0E-4","317", "0.1302","314", "0.0682","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.0477","328", "0.2211","329", "0.7542","323", "0.0068","324", "0.0","325", "0.0049","326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0184",
    });
    Onebest_AP.put(1, new String[] {
        "338", "0.1263","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.4628","335", "0.5228","349", "0.3281","302", "0.1983","301", "0.3127","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0771","345", "0.4164","346", "0.0148","309", "0.013","347", "0.059","348", "0.549","340", "0.227","318", "0.5062","319", "0.3933","316", "1.0E-4","317", "0.1302","314", "0.0682","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.5084","328", "0.4049","329", "0.7542","323", "0.0068","324", "0.0","325", "0.0049","326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Onebest_AP.put(2, new String[] {
        "338", "0.1263","339", "0.4551","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.4628","335", "0.5228","349", "0.3281","302", "0.1983","301", "0.3127","304", "0.361","303", "0.1883","341", "0.0225","306", "0.0484","305", "0.1801","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0771","345", "0.4164","346", "0.0148","309", "0.013","347", "0.059","348", "0.549","340", "0.2352","318", "0.5062","319", "0.3933","316", "1.0E-4","317", "0.1302","314", "0.0682","315", "0.3695","312", "0.2193","313", "0.318","310", "0.0084","311", "0.0512","327", "0.5084","328", "0.4049","329", "0.7542","323", "0.0068","324", "0.0","325", "0.0049","326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963",
    });
    Interp_AP.put(0, new String[] {
        "338", "0.0561","339", "0.4815","332", "0.0","333", "0.875","330", "0.6","331", "0.221","336", "1.0","337", "0.6046","334", "0.23","335", "0.5431","349", "0.3452","302", "0.2223","301", "0.4451","304", "0.318","303", "0.2063","341", "0.4225","306", "0.048","305", "0.1877","342", "0.335","343", "0.1017","308", "0.2975","344", "0.319","307", "0.0876","345", "0.2381","346", "0.0458","309", "0.0135","347", "0.0711","348", "0.5555","340", "0.1976","318", "0.5051","319", "0.5583","316", "0.3794","317", "0.188","314", "0.0722","315", "0.3393","312", "0.2514","313", "0.3652","310", "0.0072","311", "0.062","327", "0.7288","328", "0.4323","329", "0.7579","323", "0.06","324", "0.0","325", "0.0131","326", "0.6667","320", "0.1291","321", "0.4195","350", "0.4198","322", "0.1374"
    });
    Interp_AP.put(1, new String[] {
        "338", "0.0395","339", "0.4809","332", "0.0","333", "0.8749","330", "0.6","331", "0.221","336", "1.0","337", "0.6034","334", "0.4288","335", "0.4896","349", "0.3469","302", "0.2196","301", "0.4415","304", "0.3287","303", "0.2065","341", "0.3984","306", "0.0463","305", "0.2103","342", "0.3326","343", "0.1035","308", "0.2849","344", "0.3212","307", "0.0883","345", "0.24","346", "0.0426","309", "0.0135","347", "0.0665","348", "0.5549","340", "0.1752","318", "0.4977","319", "0.58","316", "0.3784","317", "0.1658","314", "0.0724","315", "0.339","312", "0.2479","313", "0.3677","310", "0.0066","311", "0.0621","327", "0.561","328", "0.4411","329", "0.7579","323", "0.099","324", "0.0","325", "0.0115","326", "0.6677","320", "0.1428","321", "0.4208","350", "0.433","322", "0.1347"    
    });
    Interp_AP.put(2, new String[] {
        "338", "0.0597","339", "0.4813","332", "0.0","333", "0.8747","330", "0.6","331", "0.2211","336", "1.0","337", "0.6044","334", "0.4255","335", "0.526","349", "0.3424","302", "0.2212","301", "0.4411","304", "0.3169","303", "0.2068","341", "0.4186","306", "0.048","305", "0.2021","342", "0.3342","343", "0.1039","308", "0.2966","344", "0.3208","307", "0.0889","345", "0.2384","346", "0.045","309", "0.0135","347", "0.0712","348", "0.555","340", "0.2","318", "0.509","319", "0.5573","316", "0.3804","317", "0.1787","314", "0.0719","315", "0.3464","312", "0.2495","313", "0.3719","310", "0.0072","311", "0.0608","327", "0.7286","328", "0.4381","329", "0.7579","323", "0.0583","324", "0.0","325", "0.0125","326", "0.6667","320", "0.1312","321", "0.4208","350", "0.4198","322", "0.1335"
    });
  }

  private static String[] baseline_token_AP = new String[] {
    "338", "0.0336","339", "0.4941","332", "0.0","333", "0.8719","330", "0.6","331", "0.2255","336", "0.5","337", "0.6051","334", "0.4261","335", "0.37","349", "0.3297","302", "0.2168","301", "0.4115","304", "0.2738","303", "0.2342","341", "0.4332","306", "0.0442","305", "0.1813","342", "0.0641","343", "0.1029","308", "0.2852","344", "0.3045","307", "0.1168","345", "0.3336","346", "0.0499","309", "0.0134","347", "0.0647","348", "0.5605","340", "0.1798","318", "0.4432","319", "0.5609","316", "0.3711","317", "0.0334","314", "0.0666","315", "0.1825","312", "0.2237","313", "0.2365","310", "0.0033","311", "0.0668","327", "0.225","328", "0.4386","329", "0.7745","323", "0.0775","324", "0.0","325", "0.0104","326", "0.0","320", "0.0707","321", "0.4164","350", "0.4197","322", "0.1389",
  };

  public EnFr_CLEF06() {
    super();
    qe = new QueryEngine();
  }

  @Test
  public void runRegressions() throws Exception {
    initialize();
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
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed" });
    FileSystem fs = FileSystem.getLocal(conf);

    // no need to repeat token-based case for other heuristics
    if (heuristic == 0) {
      qe.init(conf, fs);
      qe.runQueries(conf);
    }
    
    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".1best.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans1-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grammar

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".grammar.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".10best.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// interp

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".interp.xml",
        "--queries_path", "data/"+ PATH + "/cdec/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed" });

    qe.init(conf, fs);
    qe.runQueries(conf);
  }

  public static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

    Map<String, GroundTruth> g = Maps.newHashMap();

    g.put("en-" + LANGUAGE + ".token_10-0-100-100_0", new GroundTruth(Metric.AP, numTopics, baseline_token_AP, expectedTokenMAP));

    for (int heuristic=0; heuristic <= 2; heuristic++) {
      g.put("en-" + LANGUAGE + ".grammar_10-0-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, grammar_AP.get(heuristic), expectedMAPs.get(heuristic)[0]));
      g.put("en-" + LANGUAGE + ".1best_1-0-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, Onebest_AP.get(heuristic), expectedMAPs.get(heuristic)[1]));
      g.put("en-" + LANGUAGE + ".10best_10-100-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, Nbest_AP.get(heuristic), expectedMAPs.get(heuristic)[2]));
      g.put("en-" + LANGUAGE + ".interp_10-30-30-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, Interp_AP.get(heuristic), expectedMAPs.get(heuristic)[3]));
    }

    for (String model : models) {
      System.err.println("Verifying results of model \"" + model + "\"");

      GroundTruth groundTruth = g.get(model); 
      Map<String, Accumulator[]> result = results.get(model);
      groundTruth.verify(result, mapping, qrels);

      System.err.println("Done!");
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(EnFr_CLEF06.class);
  }

  public static void main(String[] args) {
    initialize();
    
    HMapStFW tenbestAPMap = array2Map(Nbest_AP.get(2));
    HMapStFW onebestAPMap = array2Map(Onebest_AP.get(2));
    HMapStFW grammarAPMap = array2Map(grammar_AP.get(2));
    HMapStFW tokenAPMap = array2Map(baseline_token_AP);
//    HMapStFW gridAPMap = array2Map(Interp_AP.get(2));
    System.out.println("10best: improved=" + countNumberOfImprovedTopics(tokenAPMap, tenbestAPMap) + ", negligible=" + countNumberOfNegligibleTopics(tokenAPMap, tenbestAPMap));
    System.out.println("Grammar: improved=" + countNumberOfImprovedTopics(tokenAPMap, grammarAPMap) + ", negligible=" + countNumberOfNegligibleTopics(tokenAPMap, grammarAPMap));
    System.out.println("1best: improved=" + countNumberOfImprovedTopics(tokenAPMap, onebestAPMap) + ", negligible=" + countNumberOfNegligibleTopics(tokenAPMap, onebestAPMap));
  }

  private static int countNumberOfImprovedTopics(HMapStFW tokenAPMap, HMapStFW gridAPMap) {
    int cnt = 0;
    for (String key : tokenAPMap.keySet()) {
      float difference = gridAPMap.get(key) - tokenAPMap.get(key); 
      if ( difference > 0.001 ) {
        cnt++;
      }
    }
    return cnt;
  }

  private static int countNumberOfNegligibleTopics(HMapStFW tokenAPMap, HMapStFW gridAPMap) {
    int cnt = 0;
    for (String key : tokenAPMap.keySet()) {
      float difference = gridAPMap.get(key) - tokenAPMap.get(key); 
      if ( difference > -0.001 && difference < 0.001 ) {
        cnt++;
      }
    }
    return cnt;
  }

  private static HMapStFW array2Map(String[] array) {
    HMapStFW map = new HMapStFW();
    for ( int i = 0; i < array.length; i += 2 ) {
      map.put(array[i], Float.parseFloat(array[i+1]));
    }
    return map;
  }

}
