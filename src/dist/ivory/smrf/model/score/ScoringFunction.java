/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

package ivory.smrf.model.score;

import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public abstract class ScoringFunction {

	public abstract void configure(Node domNode);

	/**
	 * @param termEvidence
	 * @param globalEvidence
	 */
	public abstract void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence);

	/**
	 * @param tf
	 * @param docLen
	 */
	public abstract double getScore(double tf, int docLen);

	public double getMaxScore() {
		return Double.POSITIVE_INFINITY;
	}

	@SuppressWarnings("unchecked")
	public static ScoringFunction create(String functionType, Node functionNode) throws Exception {
		if (functionNode == null) {
			throw new Exception("Unable to generate a ScoringFunction from a null node!");
		}

		try {
			Class<? extends ScoringFunction> clz = (Class<? extends ScoringFunction>) Class
					.forName(functionType);
			ScoringFunction f = clz.newInstance();

			f.configure(functionNode);

			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error: Unable to instantiate scoring function!");
		}
	}

}
