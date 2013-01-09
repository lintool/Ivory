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
import java.util.Set;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class Genomics05_Basic {
  private static final Logger LOG = Logger.getLogger(Genomics05_Basic.class);

  private static final ImmutableMap<String, Float> sDirBaseRawAP = new ImmutableMap.Builder<String, Float>()
      .put("100", 0.2060f).put("101", 0.0844f).put("102", 0.0127f).put("103", 0.1192f).put("104", 0.1280f)
      .put("105", 0.1947f).put("106", 0.0232f).put("107", 0.5105f).put("108", 0.0988f).put("109", 0.7033f)
      .put("110", 0.0053f).put("111", 0.3166f).put("112", 0.3427f).put("113", 0.5873f).put("114", 0.4534f)
      .put("115", 0.0005f).put("116", 0.0069f).put("117", 0.5059f).put("118", 0.1432f).put("119", 0.7091f)
      .put("120", 0.5367f).put("121", 0.7389f).put("122", 0.1332f).put("123", 0.0128f).put("124", 0.1628f)
      .put("125", 0.0003f).put("126", 0.2265f).put("127", 0.0248f).put("128", 0.1591f).put("129", 0.0883f)
      .put("130", 0.6528f).put("131", 0.7245f).put("132", 0.1072f).put("133", 0.0193f).put("134", 0.2130f)
      .put("135", 0.0000f).put("136", 0.0014f).put("137", 0.0560f).put("138", 0.2025f).put("139", 0.5158f)
      .put("140", 0.4106f).put("141", 0.3442f).put("142", 0.5229f).put("143", 0.0020f).put("144", 0.1000f)
      .put("145", 0.3619f).put("146", 0.6649f).put("147", 0.0112f).put("148", 0.0395f).put("149", 0.0370f)
      .build();

  private static final ImmutableMap<String, Float> sDirBaseRawP10 = new ImmutableMap.Builder<String, Float>()
      .put("100", 0.5000f).put("101", 0.2000f).put("102", 0.0000f).put("103", 0.2000f).put("104", 0.1000f)
      .put("105", 0.8000f).put("106", 0.3000f).put("107", 1.0000f).put("108", 0.5000f).put("109", 0.9000f)
      .put("110", 0.0000f).put("111", 0.6000f).put("112", 0.5000f).put("113", 0.6000f).put("114", 0.9000f)
      .put("115", 0.0000f).put("116", 0.0000f).put("117", 0.9000f).put("118", 0.5000f).put("119", 1.0000f)
      .put("120", 1.0000f).put("121", 0.9000f).put("122", 0.5000f).put("123", 0.0000f).put("124", 0.6000f)
      .put("125", 0.0000f).put("126", 0.5000f).put("127", 0.0000f).put("128", 0.6000f).put("129", 0.1000f)
      .put("130", 1.0000f).put("131", 0.9000f).put("132", 0.2000f).put("133", 0.0000f).put("134", 0.3000f)
      .put("135", 0.0000f).put("136", 0.0000f).put("137", 0.2000f).put("138", 0.2000f).put("139", 0.9000f)
      .put("140", 0.5000f).put("141", 0.6000f).put("142", 0.8000f).put("143", 0.0000f).put("144", 0.1000f)
      .put("145", 0.7000f).put("146", 0.9000f).put("147", 0.0000f).put("148", 0.0000f).put("149", 0.2000f)
      .build();

  private static final ImmutableMap<String, Float> sBm25BaseRawAP = new ImmutableMap.Builder<String, Float>()
      .put("100", 0.1532f).put("101", 0.1097f).put("102", 0.0092f).put("103", 0.0700f).put("104", 0.0739f)
      .put("105", 0.1705f).put("106", 0.0328f).put("107", 0.4942f).put("108", 0.1332f).put("109", 0.7034f)
      .put("110", 0.0228f).put("111", 0.3283f).put("112", 0.3506f).put("113", 0.6508f).put("114", 0.4667f)
      .put("115", 0.0005f).put("116", 0.0278f).put("117", 0.4671f).put("118", 0.3668f).put("119", 0.7016f)
      .put("120", 0.5815f).put("121", 0.6925f).put("122", 0.1555f).put("123", 0.0205f).put("124", 0.1187f)
      .put("125", 0.0000f).put("126", 0.1128f).put("127", 0.0143f).put("128", 0.1410f).put("129", 0.1058f)
      .put("130", 0.6072f).put("131", 0.6856f).put("132", 0.1052f).put("133", 0.0259f).put("134", 0.2198f)
      .put("135", 0.0000f).put("136", 0.0015f).put("137", 0.0414f).put("138", 0.1972f).put("139", 0.4548f)
      .put("140", 0.4500f).put("141", 0.3095f).put("142", 0.5366f).put("143", 0.0014f).put("144", 0.5000f)
      .put("145", 0.4002f).put("146", 0.6623f).put("147", 0.0134f).put("148", 0.0613f).put("149", 0.0321f)
      .build();

  private static final ImmutableMap<String, Float> sBm25BaseRawP10 = new ImmutableMap.Builder<String, Float>()
      .put("100", 0.4000f).put("101", 0.2000f).put("102", 0.0000f).put("103", 0.2000f).put("104", 0.1000f)
      .put("105", 0.6000f).put("106", 0.3000f).put("107", 1.0000f).put("108", 0.6000f).put("109", 0.9000f)
      .put("110", 0.1000f).put("111", 0.7000f).put("112", 0.4000f).put("113", 0.6000f).put("114", 1.0000f)
      .put("115", 0.0000f).put("116", 0.0000f).put("117", 1.0000f).put("118", 0.8000f).put("119", 1.0000f)
      .put("120", 1.0000f).put("121", 0.8000f).put("122", 0.6000f).put("123", 0.0000f).put("124", 0.4000f)
      .put("125", 0.0000f).put("126", 0.4000f).put("127", 0.0000f).put("128", 0.5000f).put("129", 0.3000f)
      .put("130", 0.9000f).put("131", 0.9000f).put("132", 0.3000f).put("133", 0.1000f).put("134", 0.3000f)
      .put("135", 0.0000f).put("136", 0.0000f).put("137", 0.2000f).put("138", 0.2000f).put("139", 0.7000f)
      .put("140", 0.6000f).put("141", 0.5000f).put("142", 0.8000f).put("143", 0.0000f).put("144", 0.1000f)
      .put("145", 0.7000f).put("146", 1.0000f).put("147", 0.0000f).put("148", 0.0000f).put("149", 0.1000f)
      .build();

  @Test
  public void runRegression() throws Exception {
    String[] params = new String[] {
            "data/medline/run.genomics05.xml",
            "data/medline/queries.genomics05.xml" };

    FileSystem fs = FileSystem.getLocal(new Configuration());

    BatchQueryRunner qr = new BatchQueryRunner(params, fs);

    long start = System.currentTimeMillis();
    qr.runQueries();
    long end = System.currentTimeMillis();

    LOG.info("Total query time: " + (end - start) + "ms");

    verifyAllResults(qr.getModels(), qr.getAllResults(), qr.getDocnoMapping(),
           new Qrels("data/medline/qrels.genomics05.txt"));
  }

  public static void verifyAllResults(Set<String> models,
          Map<String, Map<String, Accumulator[]>> results, DocnoMapping mapping, Qrels qrels) {

    Map<String, GroundTruth> g = Maps.newHashMap();
    // One topic didn't contain qrels, so trec_eval only picked up 49.
    g.put("genomics05-dir-base", new GroundTruth(Metric.AP, 49, sDirBaseRawAP, 0.2494f));
    g.put("genomics05-bm25-base", new GroundTruth(Metric.AP, 49, sBm25BaseRawAP, 0.2568f));

    Map<String, GroundTruth> h = Maps.newHashMap();
    // One topic didn't contain qrels, so trec_eval only picked up 49.
    h.put("genomics05-dir-base", new GroundTruth(Metric.P10, 49, sDirBaseRawP10, 0.4327f));
    h.put("genomics05-bm25-base", new GroundTruth(Metric.P10, 49, sBm25BaseRawP10, 0.4347f));

    for (String model : models) {
      LOG.info("Verifying results of model \"" + model + "\"");

      Map<String, Accumulator[]> r = results.get(model);
      g.get(model).verify(r, mapping, qrels);
      h.get(model).verify(r, mapping, qrels);

      LOG.info("Done!");
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Genomics05_Basic.class);
  }
}
