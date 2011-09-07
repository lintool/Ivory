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
 * Computes score based on binary presence/absence of term.
 *
 * @author Don Metzler
 */
public class BinaryTFScoringFunction extends ScoringFunction {
  @Override
  public float getScore(int tf, int docLen) {
    return tf > 0.0 ? 1.0f : 0.0f;
  }

  @Override
  public float getMinScore() {
    return 0.0f;
  }

  @Override
  public float getMaxScore() {
    return 1.0f;
  }
}
