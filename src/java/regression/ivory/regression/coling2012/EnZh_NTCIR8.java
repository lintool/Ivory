package ivory.regression.coling2012;

import ivory.core.eval.Qrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.retrieval.QueryEngine;
import ivory.sqe.retrieval.RunQueryEngine;
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

  private static String[] baseline_token_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78","0.2855","77","0.1152","35","0.0021","36","0.0028","33","0.354","39","0.0542","38","0.0","43","0.0723","42","0.2187","41","0.0712",
    "40","0.0","82","0.173","83","0.1171","80","0.0968","87","0.2274","84","0.3617","85","0.06","67","0.1404","66","0.0118","69","0.0",
    "68","0.6597","23","0.1503","26","0.0056","28","0.4382","29","0.1725","2","0.1554","30","0.1201","6","0.0755","32","0.1401","5","0.0024",
    "70","0.0015","9","0.1323","71","0.4966","72","0.3776","73","7.0E-4","74","0.0384","75","0.0041","76","0.3721","59","0.0329","58","0.0956",
    "57","0.1211","19","0.2074","56","0.1257","18","0.0","15","0.2479","16","0.0552","12","0.0476","64","0.4514","65","0.4463","62","0.4401",
    "63","0.0063","99","0.3902","61","0.0106","100","0.1015","98","0.3637","49","0.0","97","0.0062","48","2.0E-4","96","0.0306","95","7.0E-4",
    "94","0.1811","45","0.1176","93","0.1338","44","0.026","92","0.212","47","0.0","91","0.6703","46","0.2169","90","0.0233","51","0.0",
    "52","0.1413","53","0.1198","54","0.1988"};

  private static String[] phrase_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78","0.2691","77","0.3197","35","0.0017","36","0.0027","33","0.3596","39","0.0572","38","0.0","43","0.0773","42","0.245","41","0.0968",
    "40","1.0E-4","82","0.2863","83","0.1873","80","0.0878","87","0.2846","84","0.3215","85","0.075","67","0.1295","66","0.0221","69","0.0",
    "68","0.6583","23","0.1527","26","0.0191","28","0.5216","29","0.0995","2","0.125","30","0.1555","6","0.1253","5","0.0027","32","0.3643",
    "70","0.0182","9","0.1404","71","0.5154","72","0.3837","73","0.0053","74","0.0619","75","0.0037","76","0.3299","59","0.0407","58","0.0929",
    "57","0.1343","19","0.2881","56","0.2167","18","0.0535","15","0.2623","16","0.039","12","0.016","64","0.4597","65","0.4388","62","0.5182",
    "63","0.188","99","0.34","61","0.1207","100","0.1016","98","0.406","49","5.0E-4","97","0.0090","48","2.0E-4","96","0.0315","95","9.0E-4",
    "94","0.419","45","0.2865","93","0.1225","44","0.4111","92","0.2261","47","0.0","91","0.6121","46","0.2165","90","0.5066","51","0.0",
    "52","0.1534","53","0.0","54","0.4582"};

  private static String[] Nbest_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78","0.266","77","0.2643","35","0.0016","36","0.0029","33","0.3129","39","0.1182","38","0.0","43","0.0499","42","0.2263","41","0.2192",
    "40","1.0E-4","82","0.2306","83","0.1208","80","0.0339","87","0.2527","84","0.3947","85","0.0691","67","0.1098","66","0.0133","69","0.0",
    "68","0.6761","23","0.1517","26","0.1559","28","0.5351","29","0.0723","2","0.0708","30","0.4943","6","0.0039","5","3.0E-4","32","0.408",
    "70","0.0118","9","0.2702","71","0.4512","72","0.1548","73","0.0294","74","0.0718","75","0.0041","76","0.2049","59","0.0381","58","0.0843",
    "57","0.3188","19","0.2211","56","0.0744","18","0.0148","15","0.4675","16","0.0355","12","0.0434","64","0.5419","65","0.4075","62","0.462",
    "63","0.0030","99","0.05","61","0.0132","100","0.0064","98","0.4148","49","0.0","97","7.0E-4","48","0.0034","96","0.0303","95","0.0357",
    "94","0.3688","45","0.3488","93","0.0517","44","0.3071","92","0.2045","47","1.0E-4","91","0.6983","46","0.0736","90","0.5232","51","0.0",
    "52","0.1534","53","0.0","54","0.0126"};

  private static String[] Onebest_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78","0.2424","77","0.2578","35","0.0016","36","0.0035","33","0.3129","39","0.1006","38","0.0","43","0.0478","42","0.2263","41","0.2377",
    "40","1.0E-4","82","0.2306","83","0.1208","80","0.0339","87","0.0479","84","0.3973","85","0.0721","67","0.1098","66","0.0099","69","0.0",
    "68","0.7284","23","0.1517","26","0.1437","28","0.5354","29","0.0723","2","0.0658","30","0.441","6","0.0032","5","0.0","32","0.4093",
    "70","0.0119","9","0.2594","71","0.4512","72","0.1549","73","0.0294","74","0.0486","75","0.0041","76","0.0402","59","0.0381","58","0.0843",
    "57","0.3296","19","0.2208","56","0.0619","18","0.0148","15","0.4989","16","0.0528","12","0.0439","64","0.1109","65","0.4075","62","0.4603",
    "63","0.0080","99","0.0643","61","0.0132","100","0.0064","98","0.406","49","0.0","97","0.0010","48","0.0039","96","0.0298","95","0.026",
    "94","0.3688","45","0.3499","93","0.0505","44","0.3071","92","0.2045","47","1.0E-4","91","0.6822","46","0.0464","90","0.0249","51","0.0",
    "52","0.1534","53","0.0","54","0.0128"};

  private static String[] Gridbest_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78","0.3138","77","0.2976","35","0.0016","36","0.0030","33","0.3595","39","0.1146","38","0.0","43","0.0718","42","0.254","41","0.1289",
    "40","1.0E-4","82","0.2869","83","0.169","80","0.0843","87","0.2823","84","0.3724","85","0.0876","67","0.1276","66","0.0221","69","0.0",
    "68","0.667","23","0.1469","26","0.0493","28","0.5215","29","0.1042","2","0.2479","30","0.3111","6","0.0968","5","0.0017","32","0.3608",
    "70","0.0345","9","0.1637","71","0.5189","72","0.3919","73","0.0588","74","0.0705","75","0.0036","76","0.2858","59","0.0374","58","0.0966",
    "57","0.3193","19","0.2827","56","0.1832","18","0.0478","15","0.2447","16","0.0402","12","0.0388","64","0.47","65","0.4412","62","0.5023",
    "63","0.0115","99","0.3375","61","0.0951","100","0.0347","98","0.3874","49","3.0E-4","97","0.0021","48","2.0E-4","96","0.0337","95","0.0015",
    "94","0.4156","45","0.3067","93","0.1117","44","0.4117","92","0.2215","47","0.0","91","0.6543","46","0.2159","90","0.5054","51","0.0",
    "52","0.1534","53","0.1198","54","0.3676"};

  public EnZh_NTCIR8() {
    super();
    qe = new QueryEngine();
  }

  @Test
  public void runRegression() throws Exception {
    /////// baseline-token

    Configuration conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-zh.ntcir8/run_en-zh.token.xml",
        "--queries_path", "data/en-zh.ntcir8/queries.en-zh.ntcir8.xml" });
    FileSystem fs = FileSystem.getLocal(conf);

//    conf.setBoolean(Constants.Quiet, true);
    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] { 
        "--xml", "data/en-zh.ntcir8/run_en-zh.1best.xml",
        "--queries_path", "data/en-zh.ntcir8/queries.en-zh.k1.ntcir8.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// phrase

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-zh.ntcir8/run_en-zh.phrase.xml",
        "--queries_path", "data/en-zh.ntcir8/queries.en-zh.k10.ntcir8.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-zh.ntcir8/run_en-zh.10best.xml",
        "--queries_path", "data/en-zh.ntcir8/queries.en-zh.k10.ntcir8.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grid-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-zh.ntcir8/run_en-zh.gridbest.xml",
        "--queries_path", "data/en-zh.ntcir8/queries.en-zh.k10.ntcir8.xml" });   

    qe.init(conf, fs);
    qe.runQueries(conf);

    verifyAllResults(qe.getModels(), qe.getAllResults(), qe.getDocnoMapping(),
        new Qrels("data/en-zh.ntcir8/qrels.en-zh.ntcir8.txt"));
  }

  public static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

    Map<String, GroundTruth> g = Maps.newHashMap();

    g.put("en-zh.token_0-0-0-0", new GroundTruth(Metric.AP, 73, baseline_token_p005_c95_Zh_NTCIR8_AP, 0.1497f));
    g.put("en-zh.phrase_10-0-0-100", new GroundTruth(Metric.AP, 73, phrase_p005_c95_Zh_NTCIR8_AP, 0.1873f));
    g.put("en-zh.1best_1-100-0-100", new GroundTruth(Metric.AP, 73, Onebest_p005_c95_Zh_NTCIR8_AP, 0.1519f));
    g.put("en-zh.10best_10-100-0-100", new GroundTruth(Metric.AP, 73, Nbest_p005_c95_Zh_NTCIR8_AP, 0.1707f));
    g.put("en-zh.gridbest_10-20-10-100", new GroundTruth(Metric.AP, 73, Gridbest_p005_c95_Zh_NTCIR8_AP, 0.1932f));   // nbest=0.2, bitext=0.1 scfg=0.7

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
    HMapSFW gridAPMap = array2Map(Gridbest_p005_c95_Zh_NTCIR8_AP);
    HMapSFW tenbestAPMap = array2Map(Nbest_p005_c95_Zh_NTCIR8_AP);
    HMapSFW onebestAPMap = array2Map(Onebest_p005_c95_Zh_NTCIR8_AP);
    HMapSFW phraseAPMap = array2Map(phrase_p005_c95_Zh_NTCIR8_AP);
    HMapSFW tokenAPMap = array2Map(baseline_token_p005_c95_Zh_NTCIR8_AP);
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, gridAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, tenbestAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, onebestAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, phraseAPMap));
    System.out.println(countNumberOfImprovedTopics(tokenAPMap, tokenAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, gridAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, tenbestAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, onebestAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, phraseAPMap));
    System.out.println(countNumberOfNegligibleTopics(tokenAPMap, tokenAPMap));
  }

  private static int countNumberOfImprovedTopics(HMapSFW tokenAPMap, HMapSFW gridAPMap) {
    int cnt = 0;
    for (String key : tokenAPMap.keySet()) {
      if(gridAPMap.get(key) - tokenAPMap.get(key) > 0.001) {
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
