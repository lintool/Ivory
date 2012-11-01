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

public class EnFr_CLEF06 {
  private static final Logger LOG = Logger.getLogger(EnFr_CLEF06.class);
  private QueryEngine qe;

  private static String[] baseline_token_p005_c95_Fr_CLEF06_AP = new String[] {
    "338", "0.0336","339", "0.4941","332", "0.0","333", "0.8719","330", "0.6","331", "0.2254","336", "0.5","337", "0.6051",
    "334", "0.4261","335", "0.3699","349", "0.3295","302", "0.2168","301", "0.4115","304", "0.2737","303", "0.2342","306", "0.0442",
    "341", "0.4332","305", "0.1813","342", "0.063","308", "0.2852","343", "0.1029","307", "0.1168","344", "0.3045","345", "0.3336",
    "309", "0.0134","346", "0.0499","347", "0.0647","348", "0.5605","340", "0.1798","318", "0.4432","319", "0.5609","316", "0.3711",
    "317", "0.0334","314", "0.0666","315", "0.1825","312", "0.2237","313", "0.2365","310", "0.0033","311", "0.0668","327", "0.225",
    "328", "0.4386","329", "0.7745","323", "0.0775","324", "0.0","325", "0.0104","326", "0.0","320", "0.0707","350", "0.4197","321", 
    "0.4164","322", "0.1389"};

  private static String[] phrase_p005_c95_Fr_CLEF06_AP = new String[] {
    "338", "0.0901","339", "0.4803","332", "0.0","333", "0.864","330", "0.6","331", "0.2199","336", "0.125","337", "0.6117","334", "0.1691",
    "335", "0.5367","349", "0.3406","302", "0.247","301", "0.4428","304", "0.1994","303", "0.2011","341", "0.3399","306", "0.0473","305", "0.1864",
    "342", "0.3393","343", "0.1077","308", "0.338","344", "0.3513","307", "0.1258","345", "0.2542","346", "0.0199","309", "0.0135","347", "0.0707",
    "348", "0.5583","340", "0.1778","318", "0.5155","319", "0.4582","316", "0.3671","317", "0.0724","314", "0.0709","315", "0.3667","312", "0.2406",
    "313", "0.3657","310", "0.0048","311", "0.0736","327", "0.6314","328", "0.454","329", "0.779","323", "0.0563","324", "0.0","325", "0.0175",
    "326", "0.7436","320", "0.0359","321", "0.416","350", "0.4443","322", "0.1622"};

  private static String[] Nbest_p005_c95_Fr_CLEF06_AP = new String[] {
    "338", "0.1695","339", "0.4548","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "1.0","337", "0.6015","334", "0.4628",
    "335", "0.3373", "349", "0.3252","302", "0.1983","301", "0.4306","304", "0.3629","303", "0.1883","341", "0.0033","306", "0.0484",
    "305", "0.18","342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.077","345", "0.2182","346", "0.0235","309", "0.013",
    "347", "0.059","348", "0.5502","340", "0.1785","318", "0.5062","319", "0.5747","316", "0.3903","317", "0.109","314", "0.0572","315", "0.3695",
    "312", "0.2193","313", "0.3221","310", "0.0084","311", "0.0512","327", "0.485","328", "0.4048","329", "0.7542","323", "0.0225","324", "0.0",
    "325", "0.0068","326", "0.7667","320", "0.1571","321", "0.4218","350", "0.4028","322", "0.0963"};

  private static String[] Onebest_p005_c95_Fr_CLEF06_AP = new String[] {
    "338", "0.1263","339", "0.4548","332", "0.0","333", "0.8713","330", "0.6","331", "0.2192","336", "0.0476","337", "0.6015","334", "0.4628",
    "335", "0.3373","349", "0.3281","302", "0.1983","301", "0.4314","304", "0.361","303", "0.1883","341", "0.0029","306", "0.0484","305", "0.1801",
    "342", "0.3465","343", "0.103","308", "0.3853","344", "0.3619","307", "0.0525","345", "0.4165","346", "0.0233","309", "0.013","347", "0.059",
    "348", "0.549","340", "0.227","318", "0.5062","319", "0.5752","316", "0.3894","317", "0.1043","314", "0.0447","315", "0.3695","312", "0.2193",
    "313", "0.318","310", "0.0084","311", "0.0512","327", "0.4512","328", "0.4048","329", "0.7542","323", "0.0076","324", "0.0","325", "0.0068",
    "326", "0.7778","320", "0.2405","321", "0.4218","350", "0.4028","322", "0.0963"};

  private static String[] Gridbest_p005_c95_Fr_CLEF06_AP = new String[] {
    "338", "0.0751","339", "0.4793","332", "0.0","333", "0.8726","330", "0.6","331", "0.2212","336", "1.0","337", "0.6063","334", "0.3359",
    "335", "0.4889","349", "0.3429","302", "0.2194","301", "0.4427","304", "0.3067","303", "0.2038","341", "0.2185","306", "0.0478","305", "0.1892",
    "342", "0.3332","343", "0.1009","308", "0.356","344", "0.3147","307", "0.0879","345", "0.2893","346", "0.0396","309", "0.0137","347", "0.0707",
    "348", "0.5578","340", "0.176","318", "0.5072","319", "0.563","316", "0.3952","317", "0.1406","314", "0.0736","315", "0.3403","312", "0.2488",
    "313", "0.3669","310", "0.0064","311", "0.0624","327", "0.7365","328", "0.4378","329", "0.7579","323", "0.0493","324", "0.0","325", "0.0122",
    "326", "0.6667","320", "0.075","321", "0.4195","350", "0.4349","322", "0.1339"};

  public EnFr_CLEF06() {
    super();
    qe = new QueryEngine();
  }

  @Test
  public void runRegression() throws Exception {
    /////// baseline-token

    Configuration conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-fr.clef06/run_en-fr.token.xml",
        "--queries_path", "data/en-fr.clef06/queries.en-fr.clef06.xml" });
    FileSystem fs = FileSystem.getLocal(conf);

    conf.setBoolean(Constants.Quiet, true);
    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 1-best

    conf = RunQueryEngine.parseArgs(new String[] { 
        "--xml", "data/en-fr.clef06/run_en-fr.1best.xml",
        "--queries_path", "data/en-fr.clef06/queries.en-fr.k1.clef06.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// phrase

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-fr.clef06/run_en-fr.phrase.xml",
        "--queries_path", "data/en-fr.clef06/queries.en-fr.k10.clef06.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// 10-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-fr.clef06/run_en-fr.10best.xml",
        "--queries_path", "data/en-fr.clef06/queries.en-fr.k10.clef06.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    /////// grid-best

    conf = RunQueryEngine.parseArgs(new String[] {
        "--xml", "data/en-fr.clef06/run_en-fr.gridbest.xml",
        "--queries_path", "data/en-fr.clef06/queries.en-fr.k10.clef06.xml" });

    qe.init(conf, fs);
    qe.runQueries(conf);

    verifyAllResults(qe.getModels(), qe.getAllResults(), qe.getDocnoMapping(),
        new Qrels("data/en-fr.clef06/qrels.en-fr.clef06.txt"));
  }

  public static void verifyAllResults(Set<String> models,
      Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

    Map<String, GroundTruth> g = Maps.newHashMap();

    g.put("en-fr.token_0-0-0-0", new GroundTruth(Metric.AP, 50, baseline_token_p005_c95_Fr_CLEF06_AP, 0.2617f));
    g.put("en-fr.phrase_10-0-0-100", new GroundTruth(Metric.AP, 50, phrase_p005_c95_Fr_CLEF06_AP, 0.2867f));
    g.put("en-fr.1best_1-100-0-100", new GroundTruth(Metric.AP, 50, Onebest_p005_c95_Fr_CLEF06_AP, 0.2829f));
    g.put("en-fr.10best_10-100-0-100", new GroundTruth(Metric.AP, 50, Nbest_p005_c95_Fr_CLEF06_AP, 0.2979f));
    g.put("en-fr.gridbest_10-30-30-100", new GroundTruth(Metric.AP, 50, Gridbest_p005_c95_Fr_CLEF06_AP, 0.3084f));      // nbest=0.3, bitext=0.3 scfg=0.4

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
    HMapSFW gridAPMap = array2Map(Gridbest_p005_c95_Fr_CLEF06_AP);
    HMapSFW tenbestAPMap = array2Map(Nbest_p005_c95_Fr_CLEF06_AP);
    HMapSFW onebestAPMap = array2Map(Onebest_p005_c95_Fr_CLEF06_AP);
    HMapSFW phraseAPMap = array2Map(phrase_p005_c95_Fr_CLEF06_AP);
    HMapSFW tokenAPMap = array2Map(baseline_token_p005_c95_Fr_CLEF06_AP);
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
