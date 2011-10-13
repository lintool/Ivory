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

import ivory.core.util.XMLTools;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;

import org.w3c.dom.Node;

/**
 * Computes score based on F2EXP.
 *
 * @author Don Metzler
 */
public class F2EXPScoringFunction extends ScoringFunction {
  private float s;
  private float k;
  private float avgDocLen;
  private float idf;

  @Override
  public void configure(Node domNode) {
    s = XMLTools.getAttributeValue(domNode, "s", 0.5f);
    k = XMLTools.getAttributeValue(domNode, "k", 1.0f);
  }

  @Override
  public float getScore(int tf, int docLen) {
    return (tf / (tf + s + s * avgDocLen)) * idf;
  }

  @Override
  public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
    avgDocLen = (float) globalEvidence.collectionLength / (float) globalEvidence.numDocs;
    idf = (float) Math.pow((globalEvidence.numDocs + 1.0f) / (float) termEvidence.getDf(), k);
  }
}
