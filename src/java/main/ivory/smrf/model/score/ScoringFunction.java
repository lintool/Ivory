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

import ivory.core.ConfigurationException;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * Abstract base class of all scoring functions.
 *
 * @author Don Metzler
 */
public abstract class ScoringFunction {
  protected GlobalTermEvidence termEvidence;
  protected GlobalEvidence globalEvidence;

  /**
   * Configures this scoring function.
   *
   * @param domNode DOM node containing configuration data
   */
  public void configure(Node domNode) {}

  /**
   * Initializes this scoring function with global evidence.
   *
   * @param termEvidence term evidence
   * @param globalEvidence global evidence
   */
  public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
    this.termEvidence = termEvidence;
    this.globalEvidence = globalEvidence;
  }

  /**
   * Returns the global evidence associated with this scoring function.
   *
   * @return global evidence associated with this scoring function
   */
  public GlobalEvidence getGlobalEvidence() {
    return globalEvidence;
  }

  /**
   * Returns the global term evidence associated with this scoring function.
   *
   * @return global term evidence associated with this scoing function
   */
  public GlobalTermEvidence getGlobalTermEvidence() {
    return termEvidence;
  }

  /**
   * Computes score.
   */
  public abstract float getScore(int tf, int docLen);

  /**
   * Returns the minimum possible score.
   *
   * @return minimum possible score
   */
  public float getMinScore() {
    return Float.NEGATIVE_INFINITY;
  }

  /**
   * Returns the maximum possible score.
   *
   * @return maximum possible score
   */
  public float getMaxScore() {
    return Float.POSITIVE_INFINITY;
  }

  /**
   * Creates a scoring function.
   */
  @SuppressWarnings("unchecked")
  public static ScoringFunction create(String functionType, Node functionNode)
      throws ConfigurationException {
    Preconditions.checkNotNull(functionType);
    Preconditions.checkNotNull(functionNode);

    try {
      Class<? extends ScoringFunction> clz = 
        (Class<? extends ScoringFunction>) Class.forName(functionType);
      ScoringFunction f = clz.newInstance();
      f.configure(functionNode);

      return f;
    } catch (Exception e) {
      e.printStackTrace();
      throw new ConfigurationException(
          "Unable to instantiate scoring function \"" + functionType + "\"!", e);
    }
  }
}
