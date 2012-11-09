package ivory.regression.coling2012;

import ivory.core.eval.Qrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.retrieval.Constants;
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
    "78", "0.2952","77", "0.115","35", "0.0021","36", "0.0031","33", "0.3728","39", "0.0497","38", "0.0","43", "0.0777","42", 
    "0.218","41", "0.0665","40", "0.0","82", "0.1752","83", "0.1226","80", "0.0953","87", "0.2266","84", "0.3855","85", "0.0593",
    "67", "0.1383","66", "0.0115","69", "0.0","68", "0.6581","23", "0.1417","26", "0.0058","28", "0.4333","29", "0.1767",
    "2", "0.1429","30", "0.084","6", "0.0684","32", "0.1695","5", "0.0136","70", "0.0014","9", "0.1337","71", "0.4933",
    "72", "0.3719","73", "7.0E-4","74", "0.0352","75", "0.0043","76", "0.3662","59", "0.035","58", "0.0982","57", "0.1152",
    "19", "0.2902","56", "0.1272","18", "0.0","15", "0.2404","16", "0.0481","12", "0.0505","64", "0.5297","65", "0.4592",
    "62", "0.4428","63", "0.0057","99", "0.4136","61", "0.0095","100", "0.0682","98", "0.3741","49", "0.0","97", "0.0062",
    "48", "2.0E-4","96", "0.0303","95", "0.0012","94", "0.1845","45", "0.1175","93", "0.1094","44", "0.024","92", "0.1889",
    "47", "0.0","91", "0.6848","46", "0.2197","90", "0.0209","51", "0.0","52", "0.136","53", "0.1241","54", "0.1276"};

  private static String[] phrase_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78", "0.2726","77", "0.3091","35", "0.0020","36", "0.0030","33", "0.3788","39", "0.1207","38", "0.0","43", "0.0837","42", "0.241",
    "41", "0.0942","40", "1.0E-4","82", "0.2868","83", "0.1938","80", "0.0834","87", "0.2837","84", "0.3416","85", "0.0734","67", "0.1282",
    "66", "0.02","69", "0.0","68", "0.6542","23", "0.1494","26", "0.0199","28", "0.521","29", "0.1014","2", "0.1216","30", "0.1238",
    "6", "0.1283","5", "0.0049","32", "0.3683","70", "0.0177","9", "0.1413","71", "0.5145","72", "0.378","73", "0.0053","74", "0.0574",
    "75", "0.0039","76", "0.3346","59", "0.0418","58", "0.0936","57", "0.1442","19", "0.297","56", "0.2195","18", "0.0507","15", "0.2651",
    "16", "0.0385","12", "0.0189","64", "0.4995","65", "0.4503","62", "0.5225","63", "0.1869","99", "0.3244","61", "0.1281","100", "0.0682",
    "98", "0.3996","49", "5.0E-4","97", "0.0090","48", "2.0E-4","96", "0.0321","95", "9.0E-4","94", "0.4254","45", "0.2833","93", "0.1003",
    "44", "0.4088","92", "0.1969","47", "0.0","91", "0.6242","46", "0.2161","90", "0.5128","51", "0.0","52", "0.1371","53", "0.0","54", "0.3732"};

  private static String[] Nbest_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78", "0.2653","77", "0.2676","35", "0.0016","36", "0.0030","33", "0.3335","39", "0.1306","38", "0.0","43", "0.0517","42", "0.2267",
    "41", "0.2174","40", "1.0E-4","82", "0.2292","83", "0.1273","80", "0.0336","87", "0.2598","84", "0.4246","85", "0.069","67", "0.1067",
    "66", "0.0128","69", "0.0","68", "0.6738","23", "0.1509","26", "0.1635","28", "0.5382","29", "0.0718","2", "0.0749","30", "0.3952",
    "6", "0.0038","5", "0.0019","32", "0.4111","70", "0.0116","9", "0.2663","71", "0.4156","72", "0.1547","73", "0.0588","74", "0.066",
    "75", "0.0041","76", "0.2042","59", "0.0391","58", "0.0839","57", "0.3144","19", "0.284","56", "0.0781","18", "0.0144","15", "0.4729",
    "16", "0.0334","12", "0.0443","64", "0.5488","65", "0.4178","62", "0.465","63", "0.0029","99", "0.0502","61", "0.013","100", "0.0062",
    "98", "0.4183","49", "0.0","97", "7.0E-4","48", "0.0030","96", "0.0305","95", "0.0405","94", "0.3666","45", "0.3475","93", "0.0504",
    "44", "0.3072","92", "0.1763","47", "1.0E-4","91", "0.7122","46", "0.0662","90", "0.5156","51", "0.0","52", "0.1371","53", "0.0","54", "0.0073"};

  private static String[] Onebest_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78", "0.2522","77", "0.2647","35", "0.0016","36", "0.0034","33", "0.3335","39", "0.1641","38", "0.0","43", "0.0521","42", "0.2267","41", "0.2358",
    "40", "1.0E-4","82", "0.2292","83", "0.1273","80", "0.0336","87", "0.0516","84", "0.4259","85", "0.0718","67", "0.1067","66", "0.0099","69", "0.0",
    "68", "0.7268","23", "0.1509","26", "0.1514","28", "0.5388","29", "0.0718","2", "0.0674","30", "0.3794","6", "0.0030","5", "0.0","32", "0.4118",
    "70", "0.0116","9", "0.2543","71", "0.4156","72", "0.1546","73", "0.0588","74", "0.0418","75", "0.0041","76", "0.0401","59", "0.0391","58", "0.0839",
    "57", "0.3279","19", "0.284","56", "0.0642","18", "0.0144","15", "0.5014","16", "0.0515","12", "0.0448","64", "0.118","65", "0.4178","62", "0.4655",
    "63", "0.0082","99", "0.062","61", "0.013","100", "0.0062","98", "0.4094","49", "0.0","97", "0.0010","48", "0.0033","96", "0.0301","95", "0.0268",
    "94", "0.3666","45", "0.3497","93", "0.0514","44", "0.3072","92", "0.1763","47", "1.0E-4","91", "0.6976","46", "0.0439","90", "0.0245","51", "0.0",
    "52", "0.1371","53", "0.0","54", "0.0078"};

  private static String[] Gridbest_p005_c95_Zh_NTCIR8_AP = new String[] {
    "78", "0.3201","77", "0.2949","35", "0.0019","36", "0.0033","33", "0.3794","39", "0.1446","38", "0.0","43", "0.0786","42", "0.2543",
    "41", "0.1219","40", "1.0E-4","82", "0.2989","83", "0.1741","80", "0.0803","87", "0.2842","84", "0.3964","85", "0.0873","67", "0.1264",
    "66", "0.0196","69", "0.0","68", "0.6628","23", "0.1422","26", "0.0494","28", "0.522","29", "0.1068","2", "0.2179","30", "0.245",
    "6", "0.0954","5", "0.0040","32", "0.3635","70", "0.0257","9", "0.1613","71", "0.5155","72", "0.3839","73", "0.0588","74", "0.0634",
    "75", "0.0037","76", "0.283","59", "0.0407","58", "0.0974","57", "0.3143","19", "0.2929","56", "0.1956","18", "0.0464","15", "0.2337",
    "16", "0.0365","12", "0.0409","64", "0.515","65", "0.4538","62", "0.5067","63", "0.011","99", "0.346","61", "0.0965","100", "0.0347",
    "98", "0.3948","49", "3.0E-4","97", "0.0021","48", "2.0E-4","96", "0.0332","95", "0.0018","94", "0.4193","45", "0.3035","93", "0.0911",
    "44", "0.4117","92", "0.1897","47", "0.0","91", "0.6669","46", "0.2093","90", "0.4971","51", "0.0","52", "0.1371","53", "0.1241","54", "0.2697" };

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

    conf.setBoolean(Constants.Quiet, true);
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

    g.put("en-zh.token_0-0-0-0", new GroundTruth(Metric.AP, 73, baseline_token_p005_c95_Zh_NTCIR8_AP, 0.1507f));
    g.put("en-zh.phrase_10-0-0-100", new GroundTruth(Metric.AP, 73, phrase_p005_c95_Zh_NTCIR8_AP, 0.1867f));
    g.put("en-zh.1best_1-100-0-100", new GroundTruth(Metric.AP, 73, Onebest_p005_c95_Zh_NTCIR8_AP, 0.1535f));
    g.put("en-zh.10best_10-100-0-100", new GroundTruth(Metric.AP, 73, Nbest_p005_c95_Zh_NTCIR8_AP, 0.1709f));
    g.put("en-zh.gridbest_10-20-10-100", new GroundTruth(Metric.AP, 73, Gridbest_p005_c95_Zh_NTCIR8_AP, 0.1916f));   // nbest=0.2, bitext=0.1 scfg=0.7

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
