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
 * @author Don Metzler
 * 
 */
public class DirichletScoringFunction extends ScoringFunction {

	// Smoothing parameter.
	private float mMu = 2500.0f;

	// Background probability.
	private float mBackgroundProb;

	// is this term OOV?
	private boolean mOOV = false;

	@Override
	public void configure(Node domNode) {
		mMu = XMLTools.getAttributeValue(domNode, "mu", 2500.0f);
	}

	@Override
	public float getScore(int tf, int docLen) {
		if (mOOV) {
			return 0.0f;
		}

		return (float) Math.log(((float) tf + mMu * mBackgroundProb) / (docLen + mMu));
	}

	@Override
	public String toString() {
		return "<scoringfunction type=\"Dirichlet\" mu=\"" + mMu + "\" />\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		mOOV = termEvidence.cf == 0 ? true : false;
		mBackgroundProb = (float) termEvidence.cf / (float) globalEvidence.collectionLength;
	}

	@Override
	public float getMaxScore() {
		// TODO: make a tighter upper bound for this score
		return 0.0f;
	}
}
