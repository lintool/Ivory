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
public class JelinekMercerScoringFunction extends ScoringFunction {

	// Smoothing parameter.
	private float mLambda = 0.7f;

	// Background probability.
	private float mBackgroundProb;

	// Log of background probability (efficiency trick).
	private float mLogBackgroundProb;

	// Maximum possible score.
	private float mMaxScore;

	// Is this term OOV?
	private boolean mOOV = false;

	@Override
	public void configure(Node domNode) {
		mLambda = XMLTools.getAttributeValue(domNode, "lambda", 0.7f);
	}

	@Override
	public float getScore(int tf, int docLen) {
		if (mOOV) {
			return 0.0f;
		}
		if (tf == 0.0) {
			return mLogBackgroundProb;
		}
		return (float) Math.log((1.0f - mLambda) * (tf / docLen) + mLambda * mBackgroundProb);
	}

	@Override
	public String toString() {
		return "<scoringfunction type=\"JelinekMercer\" lambda=\"" + mLambda + "\" />\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		mOOV = termEvidence.cf == 0 ? true : false;
		if (!mOOV) {
			mBackgroundProb = (float) termEvidence.cf / (float) globalEvidence.collectionLength;
			mLogBackgroundProb = (float) Math.log(mLambda * mBackgroundProb);
			mMaxScore = (float) Math.log((1.0f - mLambda) + mLambda * mBackgroundProb);
		} else {
			mMaxScore = 0.0f;
		}
	}

	@Override
	public float getMaxScore() {
		return mMaxScore;
	}
}
