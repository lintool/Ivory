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
import ivory.core.eval.Qrels;
import ivory.core.eval.RankedListEvaluator;
import ivory.smrf.retrieval.Accumulator;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.DocnoMapping;

public class RegressionUtils {
	private static final Logger sLogger = Logger.getLogger(RegressionUtils.class);

	public static Map<String, Float> loadScoresIntoMap(String[] arr) {
		Map<String, Float> scores = new HashMap<String, Float>();
		for (int i = 0; i < arr.length; i += 2) {
			scores.put(arr[i], Float.parseFloat(arr[i + 1]));
		}

		return scores;
	}

	public static void verifyAP(String model, Map<String, Accumulator[]> results,
			Map<String, Float> apScores, float goldMAP, DocnoMapping mapping, Qrels qrels) {
		float apSum = 0;
		for (String qid : results.keySet()) {
			float ap = (float) RankedListEvaluator.computeAP(results.get(qid), mapping, qrels
					.getReldocsForQid(qid));

			apSum += ap;

			sLogger.info("verifying average precision for qid " + qid + " for model " + model);

			assertEquals(apScores.get(qid), ap, 10e-6);
		}

		float MAP = (float) RankedListEvaluator.roundTo4SigFigs(apSum / 50.0f);

		assertEquals(goldMAP, MAP, 10e-5);
	}
}
