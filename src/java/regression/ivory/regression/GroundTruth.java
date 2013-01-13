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

package ivory.regression;

import static org.junit.Assert.assertEquals;
import ivory.core.eval.GradedQrels;
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;

import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class GroundTruth {
  private static final Logger LOG = Logger.getLogger(GroundTruth.class);

  public static enum Metric {
    AP, P10, NDCG20
  }

  private final String model;
  private final Metric metric;
  private final int numTopics;
  private final Map<String, Float> scores;
  private final float overallScore;

  public GroundTruth(Metric m, int numTopics, String[] arr, float s) {
    this(null, m, numTopics, arr, s);
  }

  public GroundTruth(Metric m, int numTopics, Map<String, Float> scores, float s) {
    this(null, m, numTopics, scores, s);
  }

  public GroundTruth(String model, Metric m, int numTopics, String[] arr, float s) {
    this.model = model;
    this.metric = Preconditions.checkNotNull(m);
    this.numTopics = numTopics;
    this.overallScore = s;

    Preconditions.checkNotNull(arr);
    this.scores = Maps.newHashMap();

    for (int i = 0; i < arr.length; i += 2) {
      scores.put(arr[i], Float.parseFloat(arr[i + 1]));
    }
  }

  public GroundTruth(String model, Metric m, int numTopics, Map<String, Float> scores, float s) {
    this.model = model;
    this.metric = Preconditions.checkNotNull(m);
    this.numTopics = numTopics;
    this.overallScore = s;
    this.scores = Preconditions.checkNotNull(scores);
  }

  public Metric getMetric() {
    return metric;
  }

  public String getModel() {
    return model;
  }

  public void verify(Map<String, Accumulator[]> results, DocnoMapping mapping, Qrels qrels) {
    if (metric.equals(Metric.AP)) {
      verifyAP(results, mapping, qrels);
    } else if (metric.equals(Metric.P10)) {
      verifyP10(results, mapping, qrels);
    } else if (metric.equals(Metric.NDCG20)) {
      verifyNDCG20(results, mapping, (GradedQrels) qrels);
    } else {
      throw new RuntimeException("Unknown metric: Don't know how to verify!");
    }
  }

  private void verifyNDCG20(Map<String, Accumulator[]> results, DocnoMapping mapping,
      GradedQrels qrels) {
    float ndcgSum = 0;

    for (String qid : results.keySet()) {
      float ndcg = (float) RankedListEvaluator.computeNDCG(20, results.get(qid), mapping,
          qrels.getReldocsForQid(qid, true));

      ndcgSum += ndcg;

      String s = model == null ? "" : "model " + model + ": ";
      LOG.info(s + "verifying ndcg for qid " + qid);

      assertEquals(scores.get(qid), ndcg, 10e-4);
    }

    float NDCG = (float) RankedListEvaluator.roundTo4SigFigs(ndcgSum / (float) numTopics);

    assertEquals(overallScore, NDCG, 10e-4);

  }

  private void verifyAP(Map<String, Accumulator[]> results, DocnoMapping mapping, Qrels qrels) {
    float apSum = 0;
    for (String qid : results.keySet()) {
      float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      apSum += ap;

      String s = model == null ? "" : "model " + model + ": ";
      LOG.info(s + "verifying average precision for qid " + qid);

      assertEquals(scores.get(qid), ap, 10e-4);
    }

    float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / (float) numTopics);

    assertEquals(overallScore, MAP, 10e-4);
  }

  private void verifyP10(Map<String, Accumulator[]> results, DocnoMapping mapping, Qrels qrels) {
    float p10Sum = 0;
    for (String qid : results.keySet()) {
      float p10 = (float) RankedListEvaluator.computePN(10, results.get(qid), mapping,
          qrels.getReldocsForQid(qid));

      p10Sum += p10;

      String s = model == null ? "" : "model " + model + ": ";
      LOG.info(s + "verifying average precision for qid " + qid);

      assertEquals(scores.get(qid), p10, 10e-4);
    }

    float P10 = (float) RankedListEvaluator.roundTo4SigFigs(p10Sum / (float) numTopics);

    assertEquals(overallScore, P10, 10e-4);
  }
}
