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
public class BM25ScoringFunction extends ScoringFunction {

	private double mK1;
	private double mB;
	private double mAvgDocLen;
	private String mIdfType;
	private double mIDF;

	@Override
	public void configure(Node domNode) {
		mK1 = XMLTools.getAttributeValue(domNode, "k1", 1.2);
		mB = XMLTools.getAttributeValue(domNode, "b", 0.75);
		mIdfType = XMLTools.getAttributeValue(domNode, "idf", "okapi");
	}

	@Override
	public double getScore(double tf, int docLen) {
		double bm25TF = ((mK1 + 1.0) * tf) / (mK1 * ((1.0 - mB) + mB * docLen / mAvgDocLen) + tf);
		return bm25TF * mIDF;
	}

	public String toString() {
		return "<scoringfunction>BM25</scoringfunction>\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		// avg. document length
		mAvgDocLen = (double) globalEvidence.collectionLength / (double) globalEvidence.numDocs;

		// idf
		if ("none".equals(mIdfType)) {
			mIDF = 1;
		} else if ("classic".equals(mIdfType)) {
			mIDF = Math.log((double) globalEvidence.numDocs / (double) termEvidence.df);
		} else if ("okapi-positive".equals(mIdfType)) {
			mIDF = Math.log(((double) globalEvidence.numDocs + 0.5)
					/ ((double) termEvidence.df + 0.5));
		} else { // defaults to "okapi" IDF
			mIDF = Math.log(((double) globalEvidence.numDocs - (double) termEvidence.df + 0.5)
					/ ((double) termEvidence.df + 0.5));
		}
	}

	public double getMaxScore() {
		return (mK1 + 1.0) * mIDF;
	}
}
