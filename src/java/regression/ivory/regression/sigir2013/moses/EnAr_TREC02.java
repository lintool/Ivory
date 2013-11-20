package ivory.regression.sigir2013.moses;

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
import com.google.common.collect.Maps;
import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.map.HMapSFW;

public class EnAr_TREC02 {
  private QueryEngine qe;
  private static String PATH = "en-ar.trec02";
  private static String LANGUAGE = "ar";
  private static int numTopics = 50;

  private static float expectedTokenMAP = 0.2714f;
  private static Map<Integer,float[]> expectedMAPs = new HashMap<Integer,float[]>();
  
  private static Map<Integer, String[]> grammar_AP = new HashMap<Integer, String[]>();
  private static Map<Integer, String[]> Nbest_AP = new HashMap<Integer, String[]>();
  private static Map<Integer, String[]> Onebest_AP = new HashMap<Integer, String[]>();
  private static Map<Integer, String[]> Interp_AP = new HashMap<Integer, String[]>();

  public static void initialize() {
    expectedMAPs.put(0, new float[]{0.2725f, 0.2320f, 0.2489f, 0.2748f});   // "one2none" -> grammar,1best,10best,interp
    expectedMAPs.put(1, new float[]{0.2656f, 0.2492f, 0.2544f, 0.2735f});   // "one2one" -> grammar,1best,10best,interp
    expectedMAPs.put(2, new float[]{0.2742f, 0.2456f, 0.2636f, 0.2776f});   // "one2many" -> grammar,1best,10best,interp
    grammar_AP.put(0, new String[] {
        "35", "0.1804","36", "0.0021","33", "0.0299","34", "0.0062","39", "0.3566","37", "0.376","38", "0.0071","43", "0.0","42", "0.4193","41", "0.3308","40", "0.5147","67", "0.6256","66", "0.153","69", "0.5646","68", "0.2853","26", "0.0063","27", "0.3447","28", "0.0178","29", "0.5264","30", "0.7025","32", "0.0823","31", "0.0404","70", "0.5069","71", "0.2641","72", "0.0284","73", "0.282","74", "0.0305","75", "0.1662","59", "0.7346","58", "0.1665","57", "0.0961","56", "0.4687","55", "0.6265","64", "6.0E-4","65", "0.399","62", "0.0507","63", "0.1007","60", "0.735","61", "0.0802","49", "0.7932","48", "0.7049","45", "0.0297","44", "0.1628","47", "0.3341","46", "0.3137","51", "0.0539","52", "0.149","53", "0.0036","54", "0.0363","50", "0.7372"
    });
    grammar_AP.put(1, new String[] {
        "35", "0.1037","36", "0.0022","33", "0.0229","34", "0.0059","39", "0.3325","37", "0.3722","38", "0.0073","43", "0.0","42", "0.4189","41", "0.249","40", "0.3957","67", "0.6286","66", "0.2452","69", "0.5362","68", "0.3104","26", "0.0056","27", "0.3447","28", "0.02","29", "0.5354","30", "0.6487","32", "0.0259","31", "0.0472","70", "0.5058","71", "0.2728","72", "0.027","73", "0.289","74", "0.0406","75", "0.1742","59", "0.7391","58", "0.0146","57", "0.1097","56", "0.42","55", "0.6252","64", "6.0E-4","65", "0.5014","62", "0.0514","63", "0.1048","60", "0.6849","61", "0.044","49", "0.8038","48", "0.6961","45", "0.0868","44", "0.1631","47", "0.3459","46", "0.3115","51", "0.0501","52", "0.1545","53", "0.0055","54", "0.044","50", "0.755"
    });
    grammar_AP.put(2, new String[] {
        "35", "0.1934","36", "0.0022","33", "0.0311","34", "0.0062","39", "0.3568","37", "0.3762","38", "0.0070","43", "0.0","42", "0.4141","41", "0.1347","40", "0.5304","67", "0.6308","66", "0.1942","69", "0.5631","68", "0.2799","26", "0.0056","27", "0.3447","28", "0.021","29", "0.5279","30", "0.719","32", "0.0908","31", "0.0403","70", "0.5056","71", "0.2694","72", "0.0295","73", "0.2819","74", "0.0334","75", "0.1663","59", "0.7367","58", "0.1385","57", "0.0961","56", "0.488","55", "0.6263","64", "6.0E-4","65", "0.5259","62", "0.0494","63", "0.1007","60", "0.719","61", "0.0456","49", "0.797","48", "0.7042","45", "0.0623","44", "0.1639","47", "0.334","46", "0.3219","51", "0.0486","52", "0.1539","53", "0.0038","54", "0.0448","50", "0.7936"
    });
    Nbest_AP.put(0, new String[] {
        "35", "0.1857","36", "0.0032","33", "0.023","34", "0.0036","39", "0.3364","37", "0.2864","38", "0.0066","43", "0.0","42", "0.413","41", "0.035","40", "0.4676","67", "0.5442","66", "0.2495","69", "0.5353","68", "0.1407","26", "0.0153","27", "0.2883","28", "0.0218","29", "0.5233","30", "0.609","32", "0.0832","31", "0.0594","70", "0.5315","71", "0.0889","72", "0.0061","73", "0.1576","74", "0.0133","75", "0.1388","59", "0.7296","58", "0.1361","57", "0.1723","56", "0.4276","55", "0.5829","64", "6.0E-4","65", "0.3166","62", "0.0522","63", "0.0886","60", "0.7308","61", "0.0829","49", "0.7505","48", "0.6716","45", "0.0495","44", "0.1796","47", "0.3371","46", "0.3172","51", "0.0888","52", "0.0832","53", "0.0025","54", "0.0379","50", "0.8417"
    });
    Nbest_AP.put(1, new String[] {
        "35", "0.1857","36", "0.0032","33", "0.0204","34", "0.0036","39", "0.3159","37", "0.2864","38", "0.0066","43", "0.0","42", "0.413","41", "0.0285","40", "0.2389","67", "0.5924","66", "0.3667","69", "0.5353","68", "0.1407","26", "0.0050","27", "0.2883","28", "0.0251","29", "0.5233","30", "0.6284","32", "0.0635","31", "0.0594","70", "0.5315","71", "0.0889","72", "0.0228","73", "0.1757","74", "0.029","75", "0.1388","59", "0.7296","58", "0.1361","57", "0.1723","56", "0.3616","55", "0.5829","64", "6.0E-4","65", "0.5289","62", "0.0522","63", "0.0886","60", "0.7308","61", "0.0829","49", "0.8409","48", "0.6733","45", "0.3643","44", "0.1796","47", "0.3371","46", "0.3172","51", "0.0889","52", "0.079","53", "0.0025","54", "0.0332","50", "0.6194"
    });
    Nbest_AP.put(2, new String[] {
        "35", "0.1857","36", "0.0032","33", "0.0284","34", "0.0036","39", "0.3364","37", "0.2864","38", "0.0066","43", "0.0","42", "0.413","41", "0.0403","40", "0.4806","67", "0.5975","66", "0.3614","69", "0.5353","68", "0.1407","26", "0.0158","27", "0.2883","28", "0.0213","29", "0.5233","30", "0.6131","32", "0.0832","31", "0.0594","70", "0.5315","71", "0.0889","72", "0.0232","73", "0.1589","74", "0.0118","75", "0.1388","59", "0.7296","58", "0.1361","57", "0.1723","56", "0.408","55", "0.5829","64", "6.0E-4","65", "0.5386","62", "0.0522","63", "0.0886","60", "0.7308","61", "0.0829","49", "0.8021","48", "0.6724","45", "0.3457","44", "0.1796","47", "0.3371","46", "0.3172","51", "0.0888","52", "0.0796","53", "0.0025","54", "0.0348","50", "0.8186"
    });
    Onebest_AP.put(0, new String[] {
        "35", "0.1409","36", "0.0032","33", "0.0401","34", "0.0036","39", "0.2904","37", "0.2199","38", "0.0052","43", "0.0","42", "0.4017","41", "0.0","40", "0.4349","67", "0.5407","66", "0.342","69", "0.5263","68", "0.1377","26", "0.0176","27", "0.2883","28", "0.0015","29", "0.5233","30", "0.6088","32", "0.0832","31", "0.0743","70", "0.474","71", "0.0889","72", "0.0064","73", "0.1318","74", "0.0221","75", "0.1259","59", "0.7296","58", "0.1272","57", "0.0050","56", "0.4276","55", "0.6427","64", "6.0E-4","65", "0.5105","62", "0.0541","63", "0.0757","60", "0.7304","61", "0.0706","49", "0.704","48", "0.6716","45", "0.1433","44", "0.1796","47", "0.3341","46", "0.3358","51", "0.0825","52", "0.0832","53", "0.0021","54", "0.0339","50", "0.121"
    });
    Onebest_AP.put(1, new String[] {
        "35", "0.1409","36", "0.0032","33", "0.0193","34", "0.0036","39", "0.2904","37", "0.2199","38", "0.0052","43", "0.0","42", "0.4017","41", "0.0256","40", "0.4349","67", "0.5823","66", "0.342","69", "0.5263","68", "0.1377","26", "0.0176","27", "0.2883","28", "0.0544","29", "0.5233","30", "0.6088","32", "0.0484","31", "0.0743","70", "0.474","71", "0.0889","72", "0.0064","73", "0.1318","74", "0.0221","75", "0.1259","59", "0.7296","58", "0.1272","57", "0.0050","56", "0.2484","55", "0.6427","64", "6.0E-4","65", "0.5631","62", "0.0541","63", "0.0757","60", "0.7304","61", "0.0706","49", "0.704","48", "0.6716","45", "0.5262","44", "0.1796","47", "0.3341","46", "0.3358","51", "0.0825","52", "0.0832","53", "0.0021","54", "0.0339","50", "0.6625"
    });
    Onebest_AP.put(2, new String[] {
        "35", "0.1409","36", "0.0032","33", "0.0252","34", "0.0036","39", "0.2904","37", "0.2199","38", "0.0052","43", "0.0","42", "0.4017","41", "0.0295","40", "0.4349","67", "0.6033","66", "0.342","69", "0.5263","68", "0.1377","26", "0.0176","27", "0.2883","28", "0.0395","29", "0.5233","30", "0.6088","32", "0.0484","31", "0.0743","70", "0.474","71", "0.0889","72", "0.0064","73", "0.1318","74", "0.0221","75", "0.1259","59", "0.7296","58", "0.1272","57", "0.0050","56", "0.1912","55", "0.6427","64", "6.0E-4","65", "0.4456","62", "0.0541","63", "0.0757","60", "0.7304","61", "0.0706","49", "0.704","48", "0.6716","45", "0.513","44", "0.1796","47", "0.3341","46", "0.3358","51", "0.0825","52", "0.0832","53", "0.0021","54", "0.0339","50", "0.652"
    });
    Interp_AP.put(0, new String[] {
        "35", "0.1802","36", "0.0027","33", "0.0258","34", "0.0053","39", "0.3406","37", "0.3371","38", "0.0070","43", "0.0","42", "0.415","41", "0.3588","40", "0.5054","67", "0.6247","66", "0.2385","69", "0.5531","68", "0.2346","26", "0.0078","27", "0.311","28", "0.021","29", "0.5217","30", "0.6294","32", "0.2574","31", "0.0585","70", "0.532","71", "0.2315","72", "0.0266","73", "0.2417","74", "0.0253","75", "0.1621","59", "0.7341","58", "0.1366","57", "0.1264","56", "0.4036","55", "0.6272","64", "6.0E-4","65", "0.4858","62", "0.0506","63", "0.0848","60", "0.7417","61", "0.0891","49", "0.7783","48", "0.7015","45", "0.1264","44", "0.1696","47", "0.3355","46", "0.3179","51", "0.057","52", "0.1282","53", "0.0033","54", "0.0371","50", "0.7493"
    });
    Interp_AP.put(1, new String[] {
        "35", "0.1644","36", "0.0027","33", "0.0215","34", "0.0052","39", "0.3201","37", "0.3365","38", "0.0069","43", "0.0","42", "0.4157","41", "0.2666","40", "0.3948","67", "0.6229","66", "0.3132","69", "0.5314","68", "0.2389","26", "0.0071","27", "0.3111","28", "0.0221","29", "0.5288","30", "0.6816","32", "0.1236","31", "0.0602","70", "0.5315","71", "0.2376","72", "0.0257","73", "0.2471","74", "0.0324","75", "0.1614","59", "0.7345","58", "0.1099","57", "0.1269","56", "0.3877","55", "0.6295","64", "6.0E-4","65", "0.5192","62", "0.0511","63", "0.0832","60", "0.7306","61", "0.0899","49", "0.8235","48", "0.7013","45", "0.2919","44", "0.1701","47", "0.3379","46", "0.3166","51", "0.0575","52", "0.1312","53", "0.0035","54", "0.0393","50", "0.7298"
    });
    Interp_AP.put(2, new String[] {
        "35", "0.1799","36", "0.0027","33", "0.0278","34", "0.0052","39", "0.3408","37", "0.3385","38", "0.0070","43", "0.0","42", "0.4157","41", "0.1878","40", "0.5133","67", "0.6263","66", "0.2904","69", "0.5522","68", "0.2321","26", "0.0076","27", "0.311","28", "0.0194","29", "0.5242","30", "0.6404","32", "0.2063","31", "0.0585","70", "0.531","71", "0.2333","72", "0.0267","73", "0.2451","74", "0.0256","75", "0.1621","59", "0.7343","58", "0.135","57", "0.1264","56", "0.4115","55", "0.634","64", "6.0E-4","65", "0.5263","62", "0.0491","63", "0.0848","60", "0.7398","61", "0.0902","49", "0.7975","48", "0.702","45", "0.2794","44", "0.1701","47", "0.3348","46", "0.3215","51", "0.0571","52", "0.1295","53", "0.0033","54", "0.038","50", "0.8037"
    });
  }

  private static String[] baseline_token_AP = new String[] {
    "35", "0.1661","36", "0.0022","33", "0.0198","34", "0.0059","39", "0.324","37", "0.3725","38", "0.0068","43", "0.0","42", "0.4007","41", "0.3236","40", "0.5037","67", "0.5584","66", "0.2819","69", "0.5405","68", "0.2965","26", "0.0037","27", "0.2963","28", "0.0021","29", "0.5275","30", "0.7243","32", "0.3655","31", "0.042","70", "0.5104","71", "0.2278","72", "0.0276","73", "0.248","74", "0.0159","75", "0.1714","59", "0.7359","58", "0.1171","57", "0.0971","56", "0.3585","55", "0.616","64", "6.0E-4","65", "0.5153","62", "0.0509","63", "0.0783","60", "0.7071","61", "0.0885","49", "0.7933","48", "0.7146","45", "0.0731","44", "0.171","47", "0.3368","46", "0.2876","51", "0.0186","52", "0.0617","53", "0.0038","54", "0.0409","50", "0.7403"
  };

  public EnAr_TREC02() {
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
        new Qrels("data/" + PATH + "/qrels." + PATH + ".txt"));
  }

  public void runRegression(int heuristic) throws Exception {
    /////// baseline-token
    Configuration conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".token.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed", 
        "--unknown", "data/" + PATH + "/moses/10.unk"});
    FileSystem fs = FileSystem.getLocal(conf);
        
    // no need to repeat token-based case for other heuristics
    if (heuristic == 0) {
      qe.init(conf, fs);
      qe.runQueries(conf);
    }
    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".1best.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans1-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed",
        "--unknown", "data/" + PATH + "/moses/1.unk"});

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grammar

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".grammar.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed",
        "--unknown", "data/" + PATH + "/moses/10.unk"});

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/" + PATH + "/run_en-" + LANGUAGE + ".10best.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed",
        "--unknown", "data/" + PATH + "/moses/10.unk"});

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// interp

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/"+ PATH + "/run_en-" + LANGUAGE + ".interp.xml",
        "--queries_path", "data/" + PATH + "/moses/title_en-" + LANGUAGE + "-trans10-filtered.xml", "--one2many", heuristic + "", "--is_stemming", "--is_doc_stemmed",
        "--unknown", "data/" + PATH + "/moses/10.unk"});

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
    return new JUnit4TestAdapter(EnAr_TREC02.class);
  }

  public static void main(String[] args) {
    EnAr_TREC02.initialize();
    //    HMapSFW gridAPMap = array2Map(Interp_AP);
    HMapSFW tenbestAPMap = array2Map(Nbest_AP.get(2));
    HMapSFW onebestAPMap = array2Map(Onebest_AP.get(1));
    HMapSFW grammarAPMap = array2Map(grammar_AP.get(0));
    HMapSFW tokenAPMap = array2Map(baseline_token_AP);

    System.out.println("10best: improved=" + countNumberOfImprovedTopics(tokenAPMap, tenbestAPMap) + ", negligible=" + countNumberOfNegligibleTopics(tokenAPMap, tenbestAPMap));
    System.out.println("Grammar: improved=" + countNumberOfImprovedTopics(tokenAPMap, grammarAPMap) + ", negligible=" + countNumberOfNegligibleTopics(tokenAPMap, grammarAPMap));
    System.out.println("1best: improved=" + countNumberOfImprovedTopics(tokenAPMap, onebestAPMap) + ", negligible=" + countNumberOfNegligibleTopics(tokenAPMap, onebestAPMap));
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
