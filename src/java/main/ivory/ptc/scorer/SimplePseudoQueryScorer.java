/**
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package ivory.ptc.scorer;

import ivory.ptc.data.PseudoJudgments;

/**
 * A simple scorer that aggregates the confidence scores of the given
 * pseudo judgments.
 *
 * @author Nima Asadi
 */
public class SimplePseudoQueryScorer implements PseudoQueryScorer {
  @Override
	public float getScore(String query, PseudoJudgments judgments) {
		if (judgments.size() != 0) {
			return judgments.sumWeights() / judgments.size();
		}
		return 0;
	}
}
