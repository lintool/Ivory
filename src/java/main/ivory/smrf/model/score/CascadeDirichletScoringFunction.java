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
public class CascadeDirichletScoringFunction extends DirichletScoringFunction {

        private static float MU;

	
	@Override
	public synchronized void configure(Node domNode) {
		super.configure(domNode);
		MU = mu;
	}


        @Override
        public float getScore(int tf, int docLen) {
//          if ( MU != mu ) {
//            throw new RuntimeException("MU=" + MU + ", mu=" + mu + ", " + RetrievalEnvironment.mIsNewModel);
//          }
                if (isOOV) {
                        return 0.0f;
                }

                //Since cascade is trained this way (term and term proximity features have same mu)
                if (RetrievalEnvironment.mIsNewModel){
                        return (float) Math.log(((float) tf + MU * backgroundProb) / (docLen + MU));
                }
                else{
                        return (float) Math.log(((float) tf + mu * backgroundProb) / (docLen + mu));
                }
        }

//	@Override
//	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
//		super.initialize(termEvidence, globalEvidence);
//		collectionLength = (float) globalEvidence.collectionLength;
//	}
}
