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
public class DirichletScoringFunction extends ScoringFunction {

	/**
	 * Smoothing parameter 
	 */
	private double mMu = 2500.0;
	
	/**
	 * background probability
	 */
	private double mBackgroundProb;

	/**
	 * is this term OOV?
	 */
	private boolean mOOV = false;
	
	/**
	 * @param mu
	 */
	public DirichletScoringFunction(double mu) {
		mMu = mu;
	}
	
	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.scoringfunction.ScoringFunction#getScore(double, int, int, double, long, long)
	 */
	@Override
	public double getScore(double tf, int docLen) {
		if( mOOV ) {
			return 0.0;
		}
		return Math.log( ( tf + mMu * mBackgroundProb ) / ( docLen + mMu ) );
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "<scoringfunction type=\"Dirichlet\" mu=\"" + mMu + "\" />\n";
	}

	/* (non-Javadoc)
	 * @see ivory.smrf.model.score.ScoringFunction#initialize(ivory.smrf.model.GlobalTermEvidence, ivory.smrf.model.GlobalEvidence)
	 */
	@Override
	public void initialize(GlobalTermEvidence termEvidence,
			GlobalEvidence globalEvidence) {
		mOOV = termEvidence.cf == 0 ? true : false;
		mBackgroundProb = (double)termEvidence.cf / (double)globalEvidence.collectionLength;
	}
	
	/* (non-Javadoc)
	 * @see ivory.smrf.model.score.ScoringFunction#getMaxScore()
	 */
	public double getMaxScore() {
		// TODO: make a tighter upper bound for this score
		return 0.0;
	}
}
