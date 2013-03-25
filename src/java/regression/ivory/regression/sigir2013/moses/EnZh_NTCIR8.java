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

public class EnZh_NTCIR8 {
  private static final Logger LOG = Logger.getLogger(EnZh_NTCIR8.class);
  private QueryEngine qe;
  private static String PATH = "en-zh.ntcir8";
  private static String LANGUAGE = "zh";
  private static Map<Integer,float[]> expectedMAPs = new HashMap<Integer,float[]>();{ 
    expectedMAPs.put(0, new float[]{0.1665f, 0.1461f, 0.1633f});   // "one2none" -> phrase,1best,10best
    expectedMAPs.put(1, new float[]{0.1514f, 0.1553f, 0.1634f});   // "one2one" -> phrase,1best,10best
    expectedMAPs.put(2, new float[]{0.1564f, 0.1497f, 0.1693f});   // "one2many" -> phrase,1best,10best
  };
  private static float expectedTokenMAP = 0.2714f;
  private static int numTopics = 73;

  private static Map<Integer, String[]> phrase_AP = new HashMap<Integer, String[]>();
  {
    phrase_AP.put(0, new String[] {
        "78", "0.2268","77", "0.2834","35", "0.0025","36", "0.0037","33", "0.3593","39", "0.024","38", "0.0","43", "0.0677","42", "0.2807","41", "0.0837","40", "0.0","82", "0.2387","83", "0.125","80", "0.0923","87", "0.2687","84", "0.3897","85", "0.1471","67", "0.1674","66", "0.0071","69", "0.0","68", "0.6847","23", "0.1406","26", "0.0156","28", "0.4795","29", "0.1019","2", "0.0687","30", "0.1548","6", "0.1378","32", "0.2224","5", "0.0016","70", "0.02","9", "0.1004","71", "0.4932","72", "0.3698","73", "0.0014","74", "0.059","75", "0.0023","76", "0.1204","59", "0.0367","58", "0.1107","57", "0.2242","19", "0.2737","56", "0.1247","18", "1.0E-4","15", "0.2154","16", "0.0622","12", "0.0545","64", "0.3435","65", "0.4672","62", "0.4946","63", "0.1854","99", "0.4383","61", "0.0313","100", "0.102","98", "0.4264","49", "0.0","97", "0.0088","48", "0.0020","96", "0.0383","95", "0.0067","94", "0.1035","45", "0.0671","93", "0.1538","44", "0.3571","92", "0.2312","47", "0.0","91", "0.6418","46", "0.0442","90", "0.474","51", "0.0","52", "0.1534","53", "0.0","54", "0.3372",
    });
    phrase_AP.put(1, new String[] {
        "78", "0.2521","77", "0.254","35", "0.0020","36", "0.0035","33", "0.3444","39", "0.0244","38", "0.0","43", "0.0844","42", "0.2372","41", "0.062","40", "1.0E-4","82", "0.2404","83", "0.1241","80", "0.0876","87", "0.2532","84", "0.4002","85", "0.1139","67", "0.1603","66", "0.0064","69", "0.0","68", "0.6811","23", "0.1321","26", "0.032","28", "0.4595","29", "0.0577","2", "0.136","30", "0.3204","6", "0.0343","32", "0.1952","5", "0.0015","70", "0.0147","9", "0.1255","71", "0.4915","72", "0.3808","73", "0.0015","74", "0.0349","75", "0.0026","76", "0.115","59", "0.0606","58", "0.1007","57", "0.2159","19", "0.1566","56", "0.1227","18", "3.0E-4","15", "0.0641","16", "0.0697","12", "0.0539","64", "0.3974","65", "0.4601","62", "0.4834","63", "0.0040","99", "0.4633","61", "0.044","100", "0.1025","98", "0.4092","49", "0.0","97", "0.0195","48", "0.0015","96", "0.0337","95", "0.0029","94", "0.0586","45", "0.0856","93", "0.1789","44", "0.0878","92", "0.2366","47", "0.0","91", "0.6598","46", "0.0465","90", "0.0719","51", "0.0","52", "0.1534","53", "0.0","54", "0.3376",
    });
    phrase_AP.put(2, new String[] {
        "78", "0.2861","77", "0.2821","35", "0.0026","36", "0.0036","33", "0.3453","39", "0.0244","38", "0.0","43", "0.0689","42", "0.2809","41", "0.077","40", "0.0","82", "0.2331","83", "0.1257","80", "0.0977","87", "0.2645","84", "0.3899","85", "0.0977","67", "0.177","66", "0.0081","69", "0.0","68", "0.6825","23", "0.145","26", "0.017","28", "0.4531","29", "0.0555","2", "0.0495","30", "0.053","6", "0.0324","32", "0.1536","5", "0.0017","70", "0.0285","9", "0.1818","71", "0.4932","72", "0.3629","73", "0.0014","74", "0.0587","75", "0.0023","76", "0.1711","59", "0.0374","58", "0.1118","57", "0.2113","19", "0.2736","56", "0.1285","18", "2.0E-4","15", "0.3116","16", "0.0735","12", "0.0542","64", "0.3578","65", "0.4685","62", "0.483","63", "0.0399","99", "0.431","61", "0.0306","100", "0.1024","98", "0.4019","49", "0.0","97", "0.0116","48", "0.0020","96", "0.036","95", "0.0056","94", "0.1051","45", "0.0969","93", "0.1563","44", "0.3376","92", "0.2338","47", "0.0","91", "0.6177","46", "0.045","90", "0.031","51", "0.0","52", "0.1534","53", "0.0","54", "0.3631",
    });
  };

  private static Map<Integer, String[]> Nbest_AP = new HashMap<Integer, String[]>();
  {
    Nbest_AP.put(0, new String[] {
        "78", "0.1939","77", "0.2593","35", "0.0019","36", "0.0022","33", "0.3505","39", "0.1125","38", "0.0","43", "0.0586","42", "0.1634","41", "0.225","40", "1.0E-4","82", "0.2835","83", "0.1208","80", "0.0453","87", "0.259","84", "0.3408","85", "0.0809","67", "0.1098","66", "0.0136","69", "0.0","68", "0.7284","23", "0.15","26", "0.0","28", "0.4829","29", "0.0564","2", "0.119","30", "0.5261","6", "0.0582","32", "0.3917","5", "0.0","70", "0.0052","9", "0.1028","71", "0.5278","72", "0.1549","73", "0.0294","74", "0.0718","75", "0.0037","76", "0.0352","59", "0.0193","58", "0.0843","57", "0.221","19", "0.238","56", "0.0893","18", "0.0301","15", "0.5073","16", "0.0451","12", "0.052","64", "0.4907","65", "0.4118","62", "0.4437","63", "0.0013","99", "0.349","61", "0.0171","100", "0.0067","98", "0.4335","49", "0.0","97", "0.0016","48", "2.0E-4","96", "0.0305","95", "0.0","94", "0.5249","45", "0.273","93", "0.0373","44", "0.3197","92", "0.2045","47", "0.0","91", "0.6822","46", "0.1406","90", "0.0","51", "0.0","52", "0.1534","53", "0.0","54", "0.0489",
    });
    Nbest_AP.put(1, new String[] {
        "78", "0.1939","77", "0.2476","35", "0.0019","36", "0.0022","33", "0.3488","39", "0.1125","38", "0.0","43", "0.0586","42", "0.1634","41", "0.225","40", "1.0E-4","82", "0.2835","83", "0.1208","80", "0.0453","87", "0.2592","84", "0.3408","85", "0.0809","67", "0.1098","66", "0.0136","69", "0.0","68", "0.7284","23", "0.15","26", "0.0","28", "0.4829","29", "0.0564","2", "0.119","30", "0.4551","6", "0.0254","32", "0.3917","5", "0.0","70", "0.0052","9", "0.1028","71", "0.5278","72", "0.1549","73", "0.0294","74", "0.0718","75", "0.0037","76", "0.0327","59", "0.0193","58", "0.0843","57", "0.221","19", "0.238","56", "0.0893","18", "0.0301","15", "0.5073","16", "0.0417","12", "0.052","64", "0.4907","65", "0.4118","62", "0.4437","63", "0.0035","99", "0.349","61", "0.0171","100", "0.0067","98", "0.4335","49", "0.0","97", "0.0016","48", "2.0E-4","96", "0.0305","95", "0.0","94", "0.5249","45", "0.273","93", "0.0373","44", "0.3197","92", "0.2045","47", "0.0","91", "0.6996","46", "0.1406","90", "0.1092","51", "0.0","52", "0.1534","53", "0.0","54", "0.0489",
    });
    Nbest_AP.put(2, new String[] {
        "78", "0.1939","77", "0.2562","35", "0.0019","36", "0.0022","33", "0.3497","39", "0.1125","38", "0.0","43", "0.0586","42", "0.1634","41", "0.225","40", "1.0E-4","82", "0.2835","83", "0.1208","80", "0.0453","87", "0.2578","84", "0.3408","85", "0.0809","67", "0.1098","66", "0.0136","69", "0.0","68", "0.7284","23", "0.15","26", "0.0","28", "0.4829","29", "0.0564","2", "0.119","30", "0.5326","6", "0.024","32", "0.3917","5", "0.0","70", "0.0052","9", "0.1028","71", "0.5278","72", "0.1549","73", "0.0294","74", "0.0718","75", "0.0037","76", "0.0429","59", "0.0193","58", "0.0843","57", "0.221","19", "0.238","56", "0.0893","18", "0.0301","15", "0.5073","16", "0.0451","12", "0.052","64", "0.4907","65", "0.4118","62", "0.4437","63", "0.0088","99", "0.349","61", "0.0171","100", "0.0067","98", "0.4335","49", "0.0","97", "0.0016","48", "2.0E-4","96", "0.0305","95", "0.0","94", "0.5249","45", "0.273","93", "0.0373","44", "0.3197","92", "0.2045","47", "0.0","91", "0.6488","46", "0.1406","90", "0.4868","51", "0.0","52", "0.1534","53", "0.0","54", "0.0489",
    });
  }

  private static Map<Integer, String[]> Onebest_AP = new HashMap<Integer, String[]>();
  {
    Onebest_AP.put(0, new String[] {
        "78", "0.194","77", "0.2578","35", "0.0016","36", "0.0016","33", "0.3129","39", "0.1222","38", "0.0","43", "0.0573","42", "0.1508","41", "0.2359","40", "1.0E-4","82", "0.2507","83", "0.1208","80", "0.0453","87", "0.0017","84", "0.2594","85", "0.1141","67", "0.1098","66", "0.0136","69", "0.0","68", "0.7284","23", "0.1517","26", "0.0","28", "0.5354","29", "0.011","2", "0.0685","30", "0.0742","6", "0.0138","32", "0.4093","5", "0.0","70", "0.0161","9", "0.0959","71", "0.4919","72", "0.1549","73", "3.0E-4","74", "0.072","75", "0.0050","76", "0.0223","59", "0.0211","58", "0.0843","57", "0.297","19", "0.2208","56", "0.0873","18", "0.0301","15", "0.5066","16", "0.1087","12", "0.0439","64", "0.6119","65", "0.3836","62", "0.4179","63", "8.0E-4","99", "0.2133","61", "0.0053","100", "0.0064","98", "0.3904","49", "0.0","97", "0.0010","48", "0.0","96", "0.0298","95", "0.0","94", "0.4194","45", "0.3499","93", "0.0946","44", "0.1842","92", "0.2045","47", "1.0E-4","91", "0.5492","46", "0.1402","90", "0.0","51", "0.0","52", "0.1534","53", "0.0","54", "0.0128",
    });
    Onebest_AP.put(1, new String[] {
        "78", "0.194","77", "0.2578","35", "0.0016","36", "0.0016","33", "0.3129","39", "0.1222","38", "0.0","43", "0.0573","42", "0.1508","41", "0.2359","40", "1.0E-4","82", "0.2507","83", "0.1208","80", "0.0453","87", "0.0017","84", "0.2594","85", "0.1141","67", "0.1098","66", "0.0136","69", "0.0","68", "0.7284","23", "0.1517","26", "0.0","28", "0.5354","29", "0.011","2", "0.0685","30", "0.2713","6", "0.0032","32", "0.4093","5", "0.0","70", "0.0161","9", "0.0959","71", "0.4919","72", "0.1549","73", "3.0E-4","74", "0.072","75", "0.0050","76", "0.0402","59", "0.0211","58", "0.0843","57", "0.297","19", "0.2208","56", "0.0873","18", "0.0301","15", "0.5066","16", "0.0515","12", "0.0439","64", "0.6119","65", "0.3836","62", "0.4179","63", "0.01","99", "0.2133","61", "0.0053","100", "0.0064","98", "0.3904","49", "0.0","97", "0.0010","48", "0.0","96", "0.0298","95", "0.0","94", "0.4194","45", "0.3499","93", "0.0946","44", "0.1842","92", "0.2045","47", "1.0E-4","91", "0.5584","46", "0.1402","90", "0.5008","51", "0.0","52", "0.1534","53", "0.0","54", "0.0128",
    });
    Onebest_AP.put(2, new String[] {
        "78", "0.194","77", "0.2578","35", "0.0016","36", "0.0016","33", "0.3129","39", "0.1222","38", "0.0","43", "0.0573","42", "0.1508","41", "0.2359","40", "1.0E-4","82", "0.2507","83", "0.1208","80", "0.0453","87", "0.0017","84", "0.2594","85", "0.1141","67", "0.1098","66", "0.0136","69", "0.0","68", "0.7284","23", "0.1517","26", "0.0","28", "0.5354","29", "0.011","2", "0.0685","30", "0.2713","6", "0.0032","32", "0.4093","5", "0.0","70", "0.0161","9", "0.0959","71", "0.4919","72", "0.1549","73", "3.0E-4","74", "0.072","75", "0.0050","76", "0.0571","59", "0.0211","58", "0.0843","57", "0.297","19", "0.2208","56", "0.0873","18", "0.0301","15", "0.5066","16", "0.0493","12", "0.0439","64", "0.6119","65", "0.3836","62", "0.4179","63", "0.0213","99", "0.2133","61", "0.0053","100", "0.0064","98", "0.3904","49", "0.0","97", "0.0010","48", "0.0","96", "0.0298","95", "0.0","94", "0.4194","45", "0.3499","93", "0.0946","44", "0.1842","92", "0.2045","47", "1.0E-4","91", "0.148","46", "0.1402","90", "0.4765","51", "0.0","52", "0.1534","53", "0.0","54", "0.0128",
    });
  }

  private static String[] baseline_token_AP = new String[] {
    "78", "0.2855","77", "0.1152","35", "0.0021","36", "0.0028","33", "0.354","39", "0.0542","38", "0.0","43", "0.0723","42", "0.2187","41", "0.0712","40", "0.0","82", "0.173","83", "0.1171","80", "0.0968","87", "0.2274","84", "0.3617","85", "0.06","67", "0.1404","66", "0.0118","69", "0.0","68", "0.6597","23", "0.1503","26", "0.0056","28", "0.4382","29", "0.1725","2", "0.1554","30", "0.1201","6", "0.0755","32", "0.1401","5", "0.0024","70", "0.0015","9", "0.1323","71", "0.4966","72", "0.3776","73", "7.0E-4","74", "0.0384","75", "0.0041","76", "0.3721","59", "0.0329","58", "0.0956","57", "0.1211","19", "0.2074","56", "0.1257","18", "0.0","15", "0.2479","16", "0.0552","12", "0.0476","64", "0.4514","65", "0.4463","62", "0.4401","63", "0.0063","99", "0.3902","61", "0.0106","100", "0.1015","98", "0.3637","49", "0.0","97", "0.0062","48", "2.0E-4","96", "0.0306","95", "7.0E-4","94", "0.1811","45", "0.1176","93", "0.1338","44", "0.026","92", "0.212","47", "0.0","91", "0.6703","46", "0.2169","90", "0.0233","51", "0.0","52", "0.1413","53", "0.1198","54", "0.1988",
  };

  //  private static String[] Gridbest_AP = phrase_AP_one2none;

  public EnZh_NTCIR8() {
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

    g.put("en-" + LANGUAGE + ".token_10-0-100-100_0", new GroundTruth(Metric.AP, 50, baseline_token_AP, expectedTokenMAP));

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
    return new JUnit4TestAdapter(EnZh_NTCIR8.class);
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
