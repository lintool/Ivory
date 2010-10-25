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

	private float mK1;
	private float mB;
	private float mAvgDocLen;
	private String mIdfType;
	private float mIdf;

	@Override
	public void configure(Node domNode) {
		mK1 = XMLTools.getAttributeValue(domNode, "k1", 1.2f);
		mB = XMLTools.getAttributeValue(domNode, "b", 0.75f);
		mIdfType = XMLTools.getAttributeValue(domNode, "idf", "okapi");
	}

	@Override
	public float getScore(int tf, int docLen) {
		float bm25TF = ((mK1 + 1.0f) * tf) / (mK1 * ((1.0f - mB) + mB * docLen / mAvgDocLen) + tf);
		return bm25TF * mIdf;
	}

	@Override
	public String toString() {
		return "<scoringfunction>BM25</scoringfunction>\n";
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		mAvgDocLen = (float) globalEvidence.collectionLength / (float) globalEvidence.numDocs;

		if ("none".equals(mIdfType)) {
			mIdf = 1;
		} else if ("classic".equals(mIdfType)) {
			mIdf = (float) Math.log((float) globalEvidence.numDocs / (float) termEvidence.df);
		} else if ("okapi-positive".equals(mIdfType)) {
			mIdf = (float) Math.log(((float) globalEvidence.numDocs + 0.5f)
					/ ((float) termEvidence.df + 0.5f));
		} else {
			// Defaults to "Okapi" idf.
			mIdf = (float) Math.log(((float) globalEvidence.numDocs - (float) termEvidence.df + 0.5f)
					/ ((float) termEvidence.df + 0.5f));
		}
	}

	@Override
	public float getMaxScore() {
		return (mK1 + 1.0f) * mIdf;
	}
}
