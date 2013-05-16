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

public class EnAr_TREC02 {
  private static final Logger LOG = Logger.getLogger(EnAr_TREC02.class);
  private QueryEngine qe;
  private static String PATH = "en-ar.trec02";
  private static String LANGUAGE = "ar";
  private static Map<Integer,float[]> expectedMAPs = new HashMap<Integer,float[]>();{ 
    expectedMAPs.put(0, new float[]{0.2725f, 0.2320f, 0.2489f}); 
    expectedMAPs.put(1, new float[]{0.2656f, 0.2492f, 0.2544f}); 
    expectedMAPs.put(2, new float[]{0.2742f, 0.2456f, 0.2636f}); 
  };
  private static float expectedTokenMAP = 0.2714f;
  private static int numTopics = 50;

  private static Map<Integer, String[]> grammar_AP = new HashMap<Integer, String[]>();
  {
    grammar_AP.put(0, new String[] {
        "35", "0.1804","36", "0.0021","33", "0.0299","34", "0.0062","39", "0.3566","37", "0.376","38", "0.0071","43", "0.0","42", "0.4193","41", "0.3308","40", "0.5147","67", "0.6256","66", "0.153","69", "0.5646","68", "0.2853","26", "0.0063","27", "0.3447","28", "0.0178","29", "0.5264","30", "0.7025","32", "0.0823","31", "0.0404","70", "0.5069","71", "0.2641","72", "0.0284","73", "0.282","74", "0.0305","75", "0.1662","59", "0.7346","58", "0.1665","57", "0.0961","56", "0.4687","55", "0.6265","64", "6.0E-4","65", "0.399","62", "0.0507","63", "0.1007","60", "0.735","61", "0.0802","49", "0.7932","48", "0.7049","45", "0.0297","44", "0.1628","47", "0.3341","46", "0.3137","51", "0.0539","52", "0.149","53", "0.0036","54", "0.0363","50", "0.7372"
    });
    grammar_AP.put(1, new String[] {
        "35", "0.1037","36", "0.0022","33", "0.0229","34", "0.0059","39", "0.3325","37", "0.3722","38", "0.0073","43", "0.0","42", "0.4189","41", "0.249","40", "0.3957","67", "0.6286","66", "0.2452","69", "0.5362","68", "0.3104","26", "0.0056","27", "0.3447","28", "0.02","29", "0.5354","30", "0.6487","32", "0.0259","31", "0.0472","70", "0.5058","71", "0.2728","72", "0.027","73", "0.289","74", "0.0406","75", "0.1742","59", "0.7391","58", "0.0146","57", "0.1097","56", "0.42","55", "0.6252","64", "6.0E-4","65", "0.5014","62", "0.0514","63", "0.1048","60", "0.6849","61", "0.044","49", "0.8038","48", "0.6961","45", "0.0868","44", "0.1631","47", "0.3459","46", "0.3115","51", "0.0501","52", "0.1545","53", "0.0055","54", "0.044","50", "0.755"
    });
    grammar_AP.put(2, new String[] {
        "35", "0.1934","36", "0.0022","33", "0.0311","34", "0.0062","39", "0.3568","37", "0.3762","38", "0.0070","43", "0.0","42", "0.4141","41", "0.1347","40", "0.5304","67", "0.6308","66", "0.1942","69", "0.5631","68", "0.2799","26", "0.0056","27", "0.3447","28", "0.021","29", "0.5279","30", "0.719","32", "0.0908","31", "0.0403","70", "0.5056","71", "0.2694","72", "0.0295","73", "0.2819","74", "0.0334","75", "0.1663","59", "0.7367","58", "0.1385","57", "0.0961","56", "0.488","55", "0.6263","64", "6.0E-4","65", "0.5259","62", "0.0494","63", "0.1007","60", "0.719","61", "0.0456","49", "0.797","48", "0.7042","45", "0.0623","44", "0.1639","47", "0.334","46", "0.3219","51", "0.0486","52", "0.1539","53", "0.0038","54", "0.0448","50", "0.7936"
    });
  };

  private static Map<Integer, String[]> Nbest_AP = new HashMap<Integer, String[]>();
  {
    Nbest_AP.put(0, new String[] {
        "35", "0.1857","36", "0.0032","33", "0.023","34", "0.0036","39", "0.3364","37", "0.2864","38", "0.0066","43", "0.0","42", "0.413","41", "0.035","40", "0.4676","67", "0.5442","66", "0.2495","69", "0.5353","68", "0.1407","26", "0.0153","27", "0.2883","28", "0.0218","29", "0.5233","30", "0.609","32", "0.0832","31", "0.0594","70", "0.5315","71", "0.0889","72", "0.0061","73", "0.1576","74", "0.0133","75", "0.1388","59", "0.7296","58", "0.1361","57", "0.1723","56", "0.4276","55", "0.5829","64", "6.0E-4","65", "0.3166","62", "0.0522","63", "0.0886","60", "0.7308","61", "0.0829","49", "0.7505","48", "0.6716","45", "0.0495","44", "0.1796","47", "0.3371","46", "0.3172","51", "0.0888","52", "0.0832","53", "0.0025","54", "0.0379","50", "0.8417"
    });
    Nbest_AP.put(1, new String[] {
        "35", "0.1857","36", "0.0032","33", "0.0204","34", "0.0036","39", "0.3159","37", "0.2864","38", "0.0066","43", "0.0","42", "0.413","41", "0.0285","40", "0.2389","67", "0.5924","66", "0.3667","69", "0.5353","68", "0.1407","26", "0.0050","27", "0.2883","28", "0.0251","29", "0.5233","30", "0.6284","32", "0.0635","31", "0.0594","70", "0.5315","71", "0.0889","72", "0.0228","73", "0.1757","74", "0.029","75", "0.1388","59", "0.7296","58", "0.1361","57", "0.1723","56", "0.3616","55", "0.5829","64", "6.0E-4","65", "0.5289","62", "0.0522","63", "0.0886","60", "0.7308","61", "0.0829","49", "0.8409","48", "0.6733","45", "0.3643","44", "0.1796","47", "0.3371","46", "0.3172","51", "0.0889","52", "0.079","53", "0.0025","54", "0.0332","50", "0.6194"
    });
    Nbest_AP.put(2, new String[] {
        "35", "0.1857","36", "0.0032","33", "0.0284","34", "0.0036","39", "0.3364","37", "0.2864","38", "0.0066","43", "0.0","42", "0.413","41", "0.0403","40", "0.4806","67", "0.5975","66", "0.3614","69", "0.5353","68", "0.1407","26", "0.0158","27", "0.2883","28", "0.0213","29", "0.5233","30", "0.6131","32", "0.0832","31", "0.0594","70", "0.5315","71", "0.0889","72", "0.0232","73", "0.1589","74", "0.0118","75", "0.1388","59", "0.7296","58", "0.1361","57", "0.1723","56", "0.408","55", "0.5829","64", "6.0E-4","65", "0.5386","62", "0.0522","63", "0.0886","60", "0.7308","61", "0.0829","49", "0.8021","48", "0.6724","45", "0.3457","44", "0.1796","47", "0.3371","46", "0.3172","51", "0.0888","52", "0.0796","53", "0.0025","54", "0.0348","50", "0.8186"
    });
  }

  private static Map<Integer, String[]> Onebest_AP = new HashMap<Integer, String[]>();
  {
    Onebest_AP.put(0, new String[] {
        "35", "0.1409","36", "0.0032","33", "0.0401","34", "0.0036","39", "0.2904","37", "0.2199","38", "0.0052","43", "0.0","42", "0.4017","41", "0.0","40", "0.4349","67", "0.5407","66", "0.342","69", "0.5263","68", "0.1377","26", "0.0176","27", "0.2883","28", "0.0015","29", "0.5233","30", "0.6088","32", "0.0832","31", "0.0743","70", "0.474","71", "0.0889","72", "0.0064","73", "0.1318","74", "0.0221","75", "0.1259","59", "0.7296","58", "0.1272","57", "0.0050","56", "0.4276","55", "0.6427","64", "6.0E-4","65", "0.5105","62", "0.0541","63", "0.0757","60", "0.7304","61", "0.0706","49", "0.704","48", "0.6716","45", "0.1433","44", "0.1796","47", "0.3341","46", "0.3358","51", "0.0825","52", "0.0832","53", "0.0021","54", "0.0339","50", "0.121"
    });
    Onebest_AP.put(1, new String[] {
        "35", "0.1409","36", "0.0032","33", "0.0193","34", "0.0036","39", "0.2904","37", "0.2199","38", "0.0052","43", "0.0","42", "0.4017","41", "0.0256","40", "0.4349","67", "0.5823","66", "0.342","69", "0.5263","68", "0.1377","26", "0.0176","27", "0.2883","28", "0.0544","29", "0.5233","30", "0.6088","32", "0.0484","31", "0.0743","70", "0.474","71", "0.0889","72", "0.0064","73", "0.1318","74", "0.0221","75", "0.1259","59", "0.7296","58", "0.1272","57", "0.0050","56", "0.2484","55", "0.6427","64", "6.0E-4","65", "0.5631","62", "0.0541","63", "0.0757","60", "0.7304","61", "0.0706","49", "0.704","48", "0.6716","45", "0.5262","44", "0.1796","47", "0.3341","46", "0.3358","51", "0.0825","52", "0.0832","53", "0.0021","54", "0.0339","50", "0.6625"
    });
    Onebest_AP.put(2, new String[] {
        "35", "0.1409","36", "0.0032","33", "0.0252","34", "0.0036","39", "0.2904","37", "0.2199","38", "0.0052","43", "0.0","42", "0.4017","41", "0.0295","40", "0.4349","67", "0.6033","66", "0.342","69", "0.5263","68", "0.1377","26", "0.0176","27", "0.2883","28", "0.0395","29", "0.5233","30", "0.6088","32", "0.0484","31", "0.0743","70", "0.474","71", "0.0889","72", "0.0064","73", "0.1318","74", "0.0221","75", "0.1259","59", "0.7296","58", "0.1272","57", "0.0050","56", "0.1912","55", "0.6427","64", "6.0E-4","65", "0.4456","62", "0.0541","63", "0.0757","60", "0.7304","61", "0.0706","49", "0.704","48", "0.6716","45", "0.513","44", "0.1796","47", "0.3341","46", "0.3358","51", "0.0825","52", "0.0832","53", "0.0021","54", "0.0339","50", "0.652"
    });
  }

  private static String[] baseline_token_AP = new String[] {
    "35", "0.1661","36", "0.0022","33", "0.0198","34", "0.0059","39", "0.324","37", "0.3725","38", "0.0068","43", "0.0","42", "0.4007","41", "0.3236","40", "0.5037","67", "0.5584","66", "0.2819","69", "0.5405","68", "0.2965","26", "0.0037","27", "0.2963","28", "0.0021","29", "0.5275","30", "0.7243","32", "0.3655","31", "0.042","70", "0.5104","71", "0.2278","72", "0.0276","73", "0.248","74", "0.0159","75", "0.1714","59", "0.7359","58", "0.1171","57", "0.0971","56", "0.3585","55", "0.616","64", "6.0E-4","65", "0.5153","62", "0.0509","63", "0.0783","60", "0.7071","61", "0.0885","49", "0.7933","48", "0.7146","45", "0.0731","44", "0.171","47", "0.3368","46", "0.2876","51", "0.0186","52", "0.0617","53", "0.0038","54", "0.0409","50", "0.7403"
  };

  //  private static String[] Gridbest_AP = grammar_AP_one2none;

  public EnAr_TREC02() {
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

    /////// grammar

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".grammar.xml",
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
      g.put("en-" + LANGUAGE + ".grammar_10-0-0-100_" + heuristic, new GroundTruth(Metric.AP, numTopics, grammar_AP.get(heuristic), expectedMAPs.get(heuristic)[0]));
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
    return new JUnit4TestAdapter(EnAr_TREC02.class);
  }

  public static void main(String[] args) {
    //    HMapSFW gridAPMap = array2Map(Gridbest_AP);
    HMapSFW tenbestAPMap = array2Map(Nbest_AP.get(2));
    HMapSFW onebestAPMap = array2Map(Onebest_AP.get(1));
    HMapSFW grammarAPMap = array2Map(grammar_AP.get(0));
    HMapSFW tokenAPMap = array2Map(baseline_token_AP);
    //    System.out.println(countNumberOfImprovedTopics(tokenAPMap, gridAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, tenbestAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, onebestAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, grammarAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, tokenAPMap));
    //    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, gridAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, tenbestAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, onebestAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, grammarAPMap));
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
