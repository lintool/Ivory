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

public class EnAr_TREC02 {
  private static final Logger LOG = Logger.getLogger(EnAr_TREC02.class);
  private QueryEngine qe;

  private static String[] baseline_token_p005_c95_Ar_TREC02_AP = new String[] {
    "35","0.1661","36","0.0022","33","0.0198","34","0.0059","39","0.324","37","0.3725","38","0.0068","43","0.0","42","0.4007",
    "41","0.3236","40","0.5037","67","0.5584","66","0.2819","69","0.5405","68","0.2965","26","0.0037","27","0.2963","28","0.0021",
    "29","0.5275","30","0.7243","32","0.3655","31","0.042","70","0.5104","71","0.2278","72","0.0276","73","0.248","74","0.0159",
    "75","0.1714","59","0.7359","58","0.1171","57","0.0971","56","0.3585","55","0.616","64","6.0E-4","65","0.5153","62","0.0509",
    "63","0.0783","60","0.7071","61","0.0885","49","0.7933","48","0.7146","45","0.0731","44","0.171","47","0.3368","46","0.2876",
    "51","0.0186","52","0.0617","53","0.0038","54","0.0409","50","0.7403"};

  private static String[] phrase_p005_c95_Ar_TREC02_AP = new String[] {
    "35","0.1855","36","0.0027","33","0.0238","34","0.0052","39","0.3555","37","0.3856","38","0.0094","43","0.0","42","0.4135",
    "41","0.422","40","0.5109","67","0.6491","66","0.368","69","0.5603","68","0.2751","26","0.0104","27","0.3312","28","0.0080",
    "29","0.5418","30","0.7043","32","0.8558","31","0.061","70","0.5446","71","0.2204","72","0.0289","73","0.2761","74","0.0789",
    "75","0.1316","59","0.7403","58","0.1486","57","0.1396","56","0.4541","55","0.6745","64","6.0E-4","65","0.5914","62","0.0516",
    "63","0.1069","60","0.7384","61","0.0842","49","0.7518","48","0.7092","45","0.3404","44","0.1845","47","0.341","46","0.3213",
    "51","0.064","52","0.102","53","0.0043","54","0.0485","50","0.7409"};

  private static String[] Nbest_p005_c95_Ar_TREC02_AP = new String[] {
    "35","0.1653","36","0.0032","33","0.0202","34","0.0036","39","0.3121","37","0.2503","38","0.0052","43","0.0214","42","0.4017",
    "41","0.0249","40","0.1236","67","0.5854","66","0.1563","69","0.5263","68","0.1657","26","0.0157","27","0.2883","28","0.0030",
    "29","0.5233","30","0.6088","32","0.0635","31","0.0743","70","0.474","71","0.0889","72","0.0013","73","0.2021","74","0.0",
    "75","0.1259","59","0.7296","58","0.0994","57","0.1145","56","0.3458","55","0.6379","64","6.0E-4","65","0.477","62","0.0724",
    "63","0.0757","60","0.7305","61","0.0706","49","0.704","48","0.6716","45","0.5262","44","0.1796","47","0.3353","46","0.3252",
    "51","0.0826","52","0.0832","53","0.0022","54","0.0345","50","0.6243"};

  private static String[] Onebest_p005_c95_Ar_TREC02_AP = new String[] {
    "35","0.1443","36","0.0032","33","0.023","34","0.0036","39","0.2904","37","0.2199","38","0.0052","43","0.0214","42","0.2195",
    "41","0.0649","40","0.4814","67","0.5823","66","0.342","69","0.5263","68","0.1358","26","0.0176","27","0.2883","28","0.0031",
    "29","0.5233","30","0.6088","32","0.0484","31","0.0743","70","0.474","71","0.0889","72","0.0010","73","0.1376","74","1.0E-4",
    "75","0.1259","59","0.7296","58","0.1009","57","0.1232","56","0.2688","55","0.6427","64","6.0E-4","65","0.5631","62","0.0541",
    "63","0.0757","60","0.7304","61","0.0706","49","0.704","48","0.6716","45","0.5262","44","0.1796","47","0.3341","46","0.3358",
    "51","0.0825","52","0.0832","53","0.0021","54","0.0339","50","0.6022"};

  private static String[] Gridbest_p005_c95_Ar_TREC02_AP = phrase_p005_c95_Ar_TREC02_AP;

  public EnAr_TREC02() {
    super();
    qe = new QueryEngine();
  }

  @Test
  public void runRegression() throws Exception {
    /////// baseline-token

    Configuration conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-ar.trec02/run_en-ar.token.xml",
        "--queries_path", "data/en-ar.trec02/queries.en-ar.trec02.xml" });
    FileSystem fs = FileSystem.getLocal(conf);

    conf.setBoolean(Constants.Quiet, true);
    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-ar.trec02/run_en-ar.1best.xml",
        "--queries_path", "data/en-ar.trec02/queries.en-ar.k1.trec02.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// phrase

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-ar.trec02/run_en-ar.phrase.xml",
        "--queries_path", "data/en-ar.trec02/queries.en-ar.k10.trec02.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-ar.trec02/run_en-ar.10best.xml",
        "--queries_path", "data/en-ar.trec02/queries.en-ar.k10.trec02.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grid-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-ar.trec02/run_en-ar.gridbest.xml",
        "--queries_path", "data/en-ar.trec02/queries.en-ar.k10.trec02.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    verifyAllResults(qe.getModels(), qe.getAllResults(), qe.getDocnoMapping(),
        new Qrels("data/en-ar.trec02/qrels.en-ar.trec02.txt"));
  }

  public static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

    Map<String, GroundTruth> g = Maps.newHashMap();

    g.put("en-ar.token_0-0-0-0", new GroundTruth(Metric.AP, 50, baseline_token_p005_c95_Ar_TREC02_AP, 0.2714f));
    g.put("en-ar.phrase_10-0-0-100", new GroundTruth(Metric.AP, 50, phrase_p005_c95_Ar_TREC02_AP, 0.3060f));
    g.put("en-ar.1best_1-100-0-100", new GroundTruth(Metric.AP, 50, Onebest_p005_c95_Ar_TREC02_AP, 0.2474f));
    g.put("en-ar.10best_10-100-0-100", new GroundTruth(Metric.AP, 50, Nbest_p005_c95_Ar_TREC02_AP, 0.2431f));
    g.put("en-ar.gridbest_10-0-0-100", new GroundTruth(Metric.AP, 50, Gridbest_p005_c95_Ar_TREC02_AP, 0.3060f));   // scfg=1.0

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
    HMapSFW gridAPMap = array2Map(Gridbest_p005_c95_Ar_TREC02_AP);
    HMapSFW tenbestAPMap = array2Map(Nbest_p005_c95_Ar_TREC02_AP);
    HMapSFW onebestAPMap = array2Map(Onebest_p005_c95_Ar_TREC02_AP);
    HMapSFW phraseAPMap = array2Map(phrase_p005_c95_Ar_TREC02_AP);
    HMapSFW tokenAPMap = array2Map(baseline_token_p005_c95_Ar_TREC02_AP);
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
