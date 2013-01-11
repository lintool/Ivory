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

import ivory.core.eval.Qrels;
import ivory.regression.GroundTruth;
import ivory.regression.GroundTruth.Metric;
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

public class Web09catB_NonPositional_Baselines {
  private static final Logger LOG = Logger.getLogger(Web09catB_NonPositional_Baselines.class);

  private static String[] bm25_rawAP = new String[] {
      "1",  "0.5212", "2",  "0.5116", "3",  "0.0007", "4",  "0.0597", "5",  "0.0779",
      "6",  "0.1209", "7",  "0.1247", "8",  "0.0242", "9",  "0.1250", "10", "0.0174",
      "11", "0.4282", "12", "0.1549", "13", "0.0600", "14", "0.0806", "15", "0.3438",
      "16", "0.3469", "17", "0.1294", "18", "0.1609", "19", "0.0000", "20", "0.0000",
      "21", "0.4137", "22", "0.4806", "23", "0.0080", "24", "0.1639", "25", "0.2739",
      "26", "0.0527", "27", "0.1993", "28", "0.2890", "29", "0.0119", "30", "0.2322",
      "31", "0.4254", "32", "0.0734", "33", "0.4791", "34", "0.0246", "35", "0.4572",
      "36", "0.1041", "37", "0.0624", "38", "0.1022", "39", "0.1413", "40", "0.1838",
      "41", "0.2528", "42", "0.0180", "43", "0.4975", "44", "0.0502", "45", "0.2768",
      "46", "0.7204", "47", "0.5047", "48", "0.1471", "49", "0.2433", "50", "0.0779" };

  private static String[] bm25_rawP10 = new String[] {
      "1",  "0.8000", "2",  "1.0000", "3",  "0.0000", "4",  "0.2000", "5",  "0.1000",
      "6",  "0.1000", "7",  "0.4000", "8",  "0.0000", "9",  "0.4000", "10", "0.1000",
      "11", "0.7000", "12", "0.9000", "13", "0.1000", "14", "0.0000", "15", "0.6000",
      "16", "0.8000", "17", "0.2000", "18", "0.3000", "19", "0.0000", "20", "0.0000",
      "21", "0.7000", "22", "1.0000", "23", "0.0000", "24", "0.5000", "25", "0.2000",
      "26", "0.3000", "27", "0.1000", "28", "0.5000", "29", "0.0000", "30", "0.6000",
      "31", "0.4000", "32", "0.5000", "33", "0.7000", "34", "0.0000", "35", "0.7000",
      "36", "0.5000", "37", "0.1000", "38", "0.3000", "39", "0.3000", "40", "0.4000",
      "41", "0.6000", "42", "0.0000", "43", "0.6000", "44", "0.4000", "45", "0.5000",
      "46", "0.8000", "47", "0.6000", "48", "0.1000", "49", "0.4000", "50", "0.1000" };

  private static String[] ql_rawAP = new String[] {
      "1",  "0.3663", "2",  "0.4251", "3",  "0.0007", "4",  "0.0585", "5",  "0.0261",
      "6",  "0.1093", "7",  "0.1250", "8",  "0.0205", "9",  "0.1551", "10", "0.0603",
      "11", "0.4410", "12", "0.1688", "13", "0.0595", "14", "0.0693", "15", "0.3321",
      "16", "0.3222", "17", "0.1071", "18", "0.2644", "19", "0.0000", "20", "0.0000",
      "21", "0.4158", "22", "0.4482", "23", "0.0060", "24", "0.1606", "25", "0.2705",
      "26", "0.1891", "27", "0.1981", "28", "0.2793", "29", "0.0679", "30", "0.2060",
      "31", "0.4260", "32", "0.0696", "33", "0.4708", "34", "0.0245", "35", "0.4336",
      "36", "0.1028", "37", "0.0500", "38", "0.0874", "39", "0.1265", "40", "0.1879",
      "41", "0.1171", "42", "0.0096", "43", "0.3539", "44", "0.0431", "45", "0.2405",
      "46", "0.7038", "47", "0.4459", "48", "0.1267", "49", "0.2187", "50", "0.0656" };

  private static String[] ql_rawP10 = new String[] {
      "1",  "0.5000", "2",  "0.9000", "3",  "0.0000", "4",  "0.1000", "5",  "0.1000",
      "6",  "0.1000", "7",  "0.5000", "8",  "0.0000", "9",  "0.5000", "10", "0.3000",
      "11", "0.7000", "12", "0.9000", "13", "0.1000", "14", "0.0000", "15", "0.5000",
      "16", "0.5000", "17", "0.2000", "18", "0.4000", "19", "0.0000", "20", "0.0000",
      "21", "0.7000", "22", "1.0000", "23", "0.0000", "24", "0.5000", "25", "0.2000",
      "26", "0.3000", "27", "0.0000", "28", "0.5000", "29", "0.1000", "30", "0.3000",
      "31", "0.4000", "32", "0.5000", "33", "0.5000", "34", "0.1000", "35", "0.5000",
      "36", "0.6000", "37", "0.1000", "38", "0.3000", "39", "0.5000", "40", "0.4000",
      "41", "0.3000", "42", "0.0000", "43", "0.2000", "44", "0.4000", "45", "0.3000",
      "46", "0.8000", "47", "0.6000", "48", "0.1000", "49", "0.4000", "50", "0.0000" };

  @Test
  public void runRegression() throws Exception {
    Map<String, GroundTruth> g = Maps.newHashMap();
    g.put("bm25-base", new GroundTruth(Metric.AP, 50, bm25_rawAP, 0.2051f));
    g.put("ql-base", new GroundTruth(Metric.AP, 50, ql_rawAP, 0.1931f));

    Map<String, GroundTruth> h = Maps.newHashMap();
    h.put("bm25-base", new GroundTruth(Metric.P10, 50, bm25_rawP10, 0.3720f));
    h.put("ql-base", new GroundTruth(Metric.P10, 50, ql_rawP10, 0.3380f));

    Qrels qrels = new Qrels("data/clue/qrels.web09catB.txt");

    String[] params = new String[] {
        "data/clue/run.web09catB.nonpositional.baselines.xml",
        "data/clue/queries.web09.xml" };

    FileSystem fs = FileSystem.getLocal(new Configuration());

    BatchQueryRunner qr = new BatchQueryRunner(params, fs);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    DocnoMapping mapping = qr.getDocnoMapping();

    for (String model : qr.getModels()) {
      LOG.info("Verifying results of model \"" + model + "\"");

      Map<String, Accumulator[]> results = qr.getResults(model);
      g.get(model).verify(results, mapping, qrels);
      h.get(model).verify(results, mapping, qrels);

      LOG.info("Done!");
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Web09catB_NonPositional_Baselines.class);
  }
}