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
import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * Computes score based on language modeling using Jelinek-Mercer smoothing.
 *
 * @author Don Metzler
 *
 */
public class JelinekMercerScoringFunction extends ScoringFunction {
	private float lambda = 0.7f;        // Smoothing parameter.
	private float backgroundProb;       // Background probability.
	private float backgroundLogProb;    // Log of background probability (efficiency trick).
	private float maxScore;	            // Maximum possible score.
	private boolean isOOV = false;      // Is this term OOV?

	@Override
	public void configure(Node domNode) {
		lambda = XMLTools.getAttributeValue(domNode, "lambda", 0.7f);
	}

	@Override
	public float getScore(int tf, int docLen) {
		if (isOOV) {
			return 0.0f;
		}
		return tf == 0.0 ? backgroundLogProb : (float) Math.log((1.0f - lambda) * (tf / docLen) + lambda * backgroundProb);
	}

	@Override
	public String toString() {
		return "<scoringfunction type=\"JelinekMercer\" lambda=\"" + lambda + "\" />\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		isOOV = termEvidence.getCf() == 0 ? true : false;
		if (!isOOV) {
			backgroundProb = (float) termEvidence.getCf() / (float) globalEvidence.collectionLength;
			backgroundLogProb = (float) Math.log(lambda * backgroundProb);
			maxScore = (float) Math.log((1.0f - lambda) + lambda * backgroundProb);
		} else {
			maxScore = 0.0f;
		}
	}

	@Override
	public float getMinScore() {
		return Float.NEGATIVE_INFINITY;
	}
	
	@Override
	public float getMaxScore() {
		return maxScore;
	}
}
