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
public class BM25ScoringFunction extends ScoringFunction {

	/**
	 * bm25 parameter 
	 */
	private double mK1;
	
	/**
	 * bm25 parameter
	 */
	private double mB;
	
	/**
	 * avg. document length
	 */
	private double mAvgDocLen;
	
	/**
	 * type of idf to use
	 */
	private String mIdfType;
	
	/**
	 * idf
	 */
	private double mIDF;
	
	/**
	 * @param k1
	 * @param b
	 * @param idfType
	 */
	public BM25ScoringFunction( double k1, double b, String idfType ) {
		// bm25 parameters
		mK1 = k1;
		mB = b;
		mIdfType = idfType;
	}
	
	/**
	 * @param k1 bm25 parameter
	 * @param b bm25 parameter
	 */
	public BM25ScoringFunction( double k1, double b ) {
		this(k1, b, "okapi");
	}
	
	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.scoringfunction.ScoringFunction#getScore(double, int, int, double, long, long)
	 */
	@Override
	public double getScore(double tf, int docLen) {
		double bm25TF = ( ( mK1 + 1.0 ) * tf ) / ( mK1 * ( ( 1.0 - mB ) + mB * docLen / mAvgDocLen ) + tf );		
		return bm25TF * mIDF;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "<scoringfunction>BM25</scoringfunction>\n";
	}

	/* (non-Javadoc)
	 * @see ivory.smrf.model.score.ScoringFunction#initialize(ivory.smrf.model.GlobalTermEvidence, ivory.smrf.model.GlobalEvidence)
	 */
	@Override
	public void initialize(GlobalTermEvidence termEvidence,
			GlobalEvidence globalEvidence) {
		// avg. document length
		mAvgDocLen = (double)globalEvidence.collectionLength / (double)globalEvidence.numDocs;
		
		// idf
		if( "none".equals( mIdfType ) ) {
			mIDF = 1;
		}
		else if( "classic".equals( mIdfType ) ) {
			mIDF = Math.log( (double)globalEvidence.numDocs / (double)termEvidence.df );
		}
		else if( "okapi-positive".equals( mIdfType ) ) {
			mIDF = Math.log( ( (double)globalEvidence.numDocs + 0.5 ) / ( (double)termEvidence.df + 0.5 ) ); 
		}
		else { // defaults to "okapi" IDF
			mIDF = Math.log( ( (double)globalEvidence.numDocs - (double)termEvidence.df + 0.5 ) / ( (double)termEvidence.df + 0.5 ) );
		}
	}
	
	/* (non-Javadoc)
	 * @see ivory.smrf.model.score.ScoringFunction#getMaxScore()
	 */
	public double getMaxScore() {
		return ( mK1 + 1.0 ) * mIDF;
	}
}
