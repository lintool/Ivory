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

package ivory.ptc.judgments.extractor;

import ivory.ptc.data.AnchorTextTarget;
import ivory.ptc.data.PseudoJudgments;
import edu.umd.cloud9.io.array.ArrayListWritable;


/**
 * A pseudo judgment extractor interface.
 *
 * @author Nima Asadi
 */
public interface PseudoJudgmentExtractor {

  /**
   * Sets the parameters of the judgment extractor.
   *
   * @param params Array of string representation of the parameters.
   */
	public void setParameters(String[] params);

  /**
   * Extracts pseudo judgments from the list of given target documents,
   * according to the extraction criteria.
   *
   * @param anchorTextTargets Set of target documents from which pseudo
   * judgments are extracted. The input list of documents must be sorted
   * based on their relevence scores.
   * @return set of pseudo judgments extracted according to the extraction
   * criteria.
   */
	public PseudoJudgments getPseudoJudgments(ArrayListWritable<AnchorTextTarget> anchorTextTargets);
}
