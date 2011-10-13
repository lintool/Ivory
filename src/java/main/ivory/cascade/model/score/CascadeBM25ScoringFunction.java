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

package ivory.cascade.model.score;

import ivory.core.RetrievalEnvironment;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.score.BM25ScoringFunction;

import org.w3c.dom.Node;


/**
 * @author Lidan Wang
 */
public class CascadeBM25ScoringFunction extends BM25ScoringFunction {
  public static float staticK1;
  public static float staticB;

  @Override
  public void configure(Node domNode) {
    super.configure(domNode);
    staticK1 = k1;
    staticB = b;
  }

  @Override
  public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
    super.initialize(termEvidence, globalEvidence);
  }

  @Override
  public float getScore(int tf, int docLen) {
    float bm25TF = 0;

    if (RetrievalEnvironment.mIsNewModel) {
      bm25TF = ((staticK1 + 1.0f) * tf) / (staticK1 * ((1.0f - staticB) + staticB * docLen / avgDocLen) + tf);
      return bm25TF * idf;
    }

    return super.getScore(tf, docLen);
  }
}
