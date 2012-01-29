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

/**
 * Scoring function based on stream length normalized term frequency only.
 *
 * @author Nima Asadi
 */
public class StreamLengthNormalizedTFScoringFunction extends ScoringFunction {
  @Override
  public float getScore(int tf, int docLen) {
    if(docLen == 0) {
      return 0.0f;
    }
    return ((float) tf) / docLen;
  }

  @Override
  public float getMinScore() {
    return 0.0f;
  }
}
