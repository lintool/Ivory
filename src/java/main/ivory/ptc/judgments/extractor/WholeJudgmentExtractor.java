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
import tl.lin.data.array.ArrayListWritable;


/**
 * Returns all the input documents as pseudo judgments. No parameter is necessary.
 *
 * @author Nima Asadi
 */
public class WholeJudgmentExtractor implements PseudoJudgmentExtractor {
  @Override
	public PseudoJudgments getPseudoJudgments(ArrayListWritable<AnchorTextTarget> anchorTextTargets) {
		PseudoJudgments judgments = new PseudoJudgments();
		for(AnchorTextTarget a :  anchorTextTargets) {
			judgments.add(a.getTarget(), a.getWeight());
		}	
		return judgments;
	}

  @Override
	public void setParameters(String[] params) {
	}
}
