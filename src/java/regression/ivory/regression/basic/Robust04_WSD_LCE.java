/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.regression.basic;

import static ivory.regression.RegressionUtils.loadScoresIntoMap;
import static org.junit.Assert.assertEquals;
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.BatchQueryRunner;

import java.util.Map;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class Robust04_WSD_LCE {
  private static final Logger LOG = Logger.getLogger(Robust04_WSD_LCE.class);

  private static String[] sDir_WSD_LCE_RawAP = new String[] {
          "601", "0.6388", "602", "0.2729", "603", "0.2653", "604", "0.8518", "605", "0.0879",
          "606", "0.6670", "607", "0.4088", "608", "0.1252", "609", "0.3729", "610", "0.0177",
          "611", "0.3111", "612", "0.6127", "613", "0.2555", "614", "0.4461", "615", "0.0408",
          "616", "0.7738", "617", "0.2982", "618", "0.1374", "619", "0.5610", "620", "0.0281",
          "621", "0.4697", "622", "0.1817", "623", "0.3294", "624", "0.3294", "625", "0.0586",
          "626", "0.6481", "627", "0.0306", "628", "0.3160", "629", "0.1655", "630", "0.7167",
          "631", "0.0988", "632", "0.4584", "633", "0.6625", "634", "0.7903", "635", "0.7607",
          "636", "0.3350", "637", "0.5973", "638", "0.0861", "639", "0.3799", "640", "0.4242",
          "641", "0.4877", "642", "0.1845", "643", "0.6097", "644", "0.1810", "645", "0.6971",
          "646", "0.3502", "647", "0.4369", "648", "0.6291", "649", "0.7811", "650", "0.2081",
          "651", "0.0593", "652", "0.5488", "653", "0.5842", "654", "0.7982", "655", "0.0059",
          "656", "0.5600", "657", "0.5261", "658", "0.2582", "659", "0.4089", "660", "0.7228",
          "661", "0.6856", "662", "0.7108", "663", "0.6868", "664", "0.9583", "665", "0.2276",
          "666", "0.0179", "667", "0.5005", "668", "0.3739", "669", "0.0854", "670", "0.3245",
          "671", "0.3550", "673", "0.1769", "674", "0.0544", "675", "0.2584", "676", "0.3192",
          "677", "0.9381", "678", "0.2429", "679", "0.9762", "680", "0.2995", "681", "0.5998",
          "682", "0.3803", "683", "0.1891", "684", "0.1039", "685", "0.2616", "686", "0.3829",
          "687", "0.4442", "688", "0.1683", "689", "0.0160", "690", "0.0064", "691", "0.3228",
          "692", "0.5250", "693", "0.3853", "694", "0.4690", "695", "0.4106", "696", "0.2934",
          "697", "0.1565", "698", "0.4551", "699", "0.5363", "700", "0.6709" };

  private static String[] sDir_WSD_LCE_RawP10 = new String[] {
          "601", "0.3000", "602", "0.2000", "603", "0.3000", "604", "0.6000", "605", "0.2000",
          "606", "0.5000", "607", "0.5000", "608", "0.0000", "609", "0.6000", "610", "0.0000",
          "611", "0.5000", "612", "0.6000", "613", "0.6000", "614", "0.4000", "615", "0.0000",
          "616", "0.9000", "617", "0.6000", "618", "0.1000", "619", "0.7000", "620", "0.1000",
          "621", "0.8000", "622", "0.4000", "623", "0.8000", "624", "0.4000", "625", "0.1000",
          "626", "0.6000", "627", "0.1000", "628", "0.6000", "629", "0.3000", "630", "0.3000",
          "631", "0.0000", "632", "1.0000", "633", "1.0000", "634", "0.8000", "635", "0.8000",
          "636", "0.4000", "637", "0.7000", "638", "0.2000", "639", "0.7000", "640", "0.8000",
          "641", "0.7000", "642", "0.3000", "643", "0.4000", "644", "0.3000", "645", "0.9000",
          "646", "0.4000", "647", "0.7000", "648", "1.0000", "649", "1.0000", "650", "0.3000",
          "651", "0.0000", "652", "1.0000", "653", "0.6000", "654", "0.9000", "655", "0.0000",
          "656", "0.7000", "657", "0.7000", "658", "0.4000", "659", "0.5000", "660", "1.0000",
          "661", "0.8000", "662", "0.9000", "663", "0.6000", "664", "0.7000", "665", "0.4000",
          "666", "0.0000", "667", "0.9000", "668", "0.7000", "669", "0.1000", "670", "0.5000",
          "671", "0.0000", "673", "0.4000", "674", "0.0000", "675", "0.4000", "676", "0.2000",
          "677", "0.8000", "678", "0.4000", "679", "0.6000", "680", "0.2000", "681", "0.7000",
          "682", "0.8000", "683", "0.4000", "684", "0.1000", "685", "0.2000", "686", "0.6000",
          "687", "0.8000", "688", "0.6000", "689", "0.0000", "690", "0.0000", "691", "0.5000",
          "692", "0.8000", "693", "0.6000", "694", "0.4000", "695", "0.9000", "696", "0.7000",
          "697", "0.4000", "698", "0.3000", "699", "0.7000", "700", "0.9000" };

  private static Qrels sQrels;
  private static DocnoMapping sMapping;

  @Test
  public void runRegression() throws Exception {
    Map<String, Map<String, Float>> AllModelsAPScores = Maps.newHashMap();
    AllModelsAPScores.put("robust04-dir-wsd-lce", loadScoresIntoMap(sDir_WSD_LCE_RawAP));

    Map<String, Map<String, Float>> AllModelsP10Scores = Maps.newHashMap();
    AllModelsP10Scores.put("robust04-dir-wsd-lce", loadScoresIntoMap(sDir_WSD_LCE_RawP10));

    sQrels = new Qrels("data/trec/qrels.robust04.noCRFR.txt");

    String[] params = new String[] {
       "data/trec/run.robust04.wsd.lce.xml",
       "data/trec/queries.robust04.xml" };

    FileSystem fs = FileSystem.getLocal(new Configuration());

    BatchQueryRunner qr = new BatchQueryRunner(params, fs);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    sMapping = qr.getDocnoMapping();

    for (String model : qr.getModels()) {
      LOG.info("Verifying results of model \"" + model + "\"");
      Map<String, Accumulator[]> results = qr.getResults(model);
      verifyResults(model, results, AllModelsAPScores.get(model), AllModelsP10Scores.get(model));
      LOG.info("Done!");
    }
  }

  private static void verifyResults(String model, Map<String, Accumulator[]> results,
      Map<String, Float> apScores, Map<String, Float> p10Scores) {
    float apSum = 0, p10Sum = 0;
    for (String qid : results.keySet()) {
      float ap = (float) RankedListEvaluator.computeAP(results.get(qid), sMapping,
          sQrels.getReldocsForQid(qid));

      float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), sMapping,
          sQrels.getReldocsForQid(qid));

      apSum += ap;
      p10Sum += p10;

      if ((apScores.get(qid)) != null && ap != 0) {

        LOG.info("verifying qid " + qid + " for model " + model);
        assertEquals(apScores.get(qid), ap, 10e-5);
        assertEquals(p10Scores.get(qid), p10, 10e-5);
      }
    }

    // one topic didn't contain qrels, so trec_eval only picked up 99 topics
    float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 99f);
    float P10Avg = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / 99f);

    if (model.equals("robust04-dir-wsd-lce")) {
      assertEquals(0.3941, MAP, 10e-5);
      assertEquals(0.4980, P10Avg, 10e-5);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Robust04_WSD_LCE.class);
  }
}
