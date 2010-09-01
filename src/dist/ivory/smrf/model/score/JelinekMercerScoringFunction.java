/*
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

package ivory.smrf.model.score;

import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;


/**
 * @author Don Metzler
 *
 */
public class JelinekMercerScoringFunction extends ScoringFunction {

	/**
	 * Smoothing parameter 
	 */
	private double mLambda = 0.7;
	
	/**
	 * background probability
	 */
	private double mBackgroundProb;
	
	/**
	 * log of background probability (efficiency trick)
	 */
	private double mLogBackgroundProb;
	
	/**
	 * maximum possible score
	 */
	private double mMaxScore;
	
	/**
	 * is this term OOV?
	 */
	private boolean mOOV = false;
	
	/**
	 * @param lambda
	 */
	public JelinekMercerScoringFunction(double lambda) {
		mLambda = lambda;
	}
	
	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.scoringfunction.ScoringFunction#getScore(double, int, int, double, long, long)
	 */
	@Override
	public double getScore(double tf, int docLen) {
		if(mOOV) {
			return 0.0;
		}
		if(tf == 0.0) {
			return mLogBackgroundProb;
		}
		return Math.log( ( 1.0 - mLambda ) * ( tf / docLen ) + mLambda * mBackgroundProb );
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "<scoringfunction type=\"JelinekMercer\" lambda=\"" + mLambda + "\" />\n";
	}

	/* (non-Javadoc)
	 * @see ivory.smrf.model.score.ScoringFunction#initialize(ivory.smrf.model.GlobalTermEvidence, ivory.smrf.model.GlobalEvidence)
	 */
	@Override
	public void initialize(GlobalTermEvidence termEvidence,
			GlobalEvidence globalEvidence) {
		mOOV = termEvidence.cf == 0 ? true : false;
		if(!mOOV) {
			mBackgroundProb = (double)termEvidence.cf / (double)globalEvidence.collectionLength;
			mLogBackgroundProb = Math.log(mLambda * mBackgroundProb);
			mMaxScore = Math.log( ( 1.0 - mLambda ) + mLambda * mBackgroundProb );
		}
		else {
			mMaxScore = 0.0;
		}
	}
	
	/* (non-Javadoc)
	 * @see ivory.smrf.model.score.ScoringFunction#getMaxScore()
	 */
	public double getMaxScore() {
		return mMaxScore;
	}

}
