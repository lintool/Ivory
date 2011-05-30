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
 * Computes score based on language modeling using Dirichlet smoothing.
 *
 * @author Don Metzler
 *
 */
public class DirichletScoringFunction extends ScoringFunction {
	public float mu = 2500.0f;     // Smoothing parameter.
	public float backgroundProb; 	// Background probability.
	public boolean isOOV = false;  // Is this term OOV?

	@Override
	public void configure(Node domNode) {
		mu = XMLTools.getAttributeValue(domNode, "mu", 2500.0f);
	}

	@Override
	public float getScore(int tf, int docLen) {
		return isOOV ? 0.0f : (float) Math.log(((float) tf + mu * backgroundProb) / (docLen + mu));
	}

	@Override
	public String toString() {
		return "<scoringfunction type=\"Dirichlet\" mu=\"" + mu + "\" />\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		isOOV = termEvidence.getCf() == 0 ? true : false;
		backgroundProb = (float) termEvidence.getCf() / (float) globalEvidence.collectionLength;
	}

	@Override 
	public float getMinScore() {
		return Float.NEGATIVE_INFINITY;
	}
	
	@Override
	public float getMaxScore() {
		// TODO: make a tighter upper bound for this score
		return 0.0f;
	}

	public float getMu(){
		return mu;
	}
}
