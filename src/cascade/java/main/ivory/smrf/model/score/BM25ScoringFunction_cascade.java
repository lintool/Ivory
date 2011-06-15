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
import ivory.util.RetrievalEnvironment;

import org.w3c.dom.Node;

/**
 * @author Lidan Wang
 * 
 */
public class BM25ScoringFunction_cascade extends BM25ScoringFunction {

        public static float K1;
        public static float B;
        public static float avg_docLen;

	@Override
	public void configure(Node domNode) {
		super.configure(domNode);
		K1 = k1;
                B = b; 
	}

	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {

		super.initialize(termEvidence, globalEvidence); 
		avg_docLen = avgDocLen;
	}

        @Override
        public float getScore(int tf, int docLen) {
                float bm25TF = 0;
 
                if (RetrievalEnvironment.mIsNewModel){
                        bm25TF = ((K1 + 1.0f) * tf) / (K1 * ((1.0f - B) + B * docLen / avg_docLen) + tf);
                }
                else{
                        bm25TF = ((k1 + 1.0f) * tf) / (k1 * ((1.0f - b) + b * docLen / avgDocLen) + tf);
                }
 
                return bm25TF * idf;
        }

}
