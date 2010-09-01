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

	// smoothing parameter
	private double mLambda = 0.7;

	// background probability
	private double mBackgroundProb;

	// log of background probability (efficiency trick)
	private double mLogBackgroundProb;

	// maximum possible score
	private double mMaxScore;

	// is this term OOV?
	private boolean mOOV = false;

	@Override
	public void configure(Node domNode) {
		mLambda = XMLTools.getAttributeValue(domNode, "lambda", 0.5);
	}

	@Override
	public double getScore(double tf, int docLen) {
		if (mOOV) {
			return 0.0;
		}
		if (tf == 0.0) {
			return mLogBackgroundProb;
		}
		return Math.log((1.0 - mLambda) * (tf / docLen) + mLambda * mBackgroundProb);
	}

	@Override
	public String toString() {
		return "<scoringfunction type=\"JelinekMercer\" lambda=\"" + mLambda + "\" />\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		mOOV = termEvidence.cf == 0 ? true : false;
		if (!mOOV) {
			mBackgroundProb = (double) termEvidence.cf / (double) globalEvidence.collectionLength;
			mLogBackgroundProb = Math.log(mLambda * mBackgroundProb);
			mMaxScore = Math.log((1.0 - mLambda) + mLambda * mBackgroundProb);
		} else {
			mMaxScore = 0.0;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ivory.smrf.model.score.ScoringFunction#getMaxScore()
	 */
	public double getMaxScore() {
		return mMaxScore;
	}

}
