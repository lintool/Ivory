/**
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

package ivory.ptc.judgments.weighting;

import edu.umd.cloud9.webgraph.data.AnchorText;

import org.apache.hadoop.fs.FileSystem;

/**
 * A weighting scheme interface that computes the confidence scores for
 * a relevance judgment.
 *
 * @author Nima Asadi
 */
public interface WeightingScheme {
  /**
   * Initializes the current weighting scheme with the given
   * set of parameters.
   *
   * @param fs FileSystem object.
   * @param params Array of the string representation of the
   * parameters needed to initialize the weighting scheme.
   */
	public void initialize(FileSystem fs, String... params);

  /**
   * Computes a relevence score of the given anchor text (pseudo quert)
   * and a target document (pseudo judgment)
   *
   * @param targetDocument Document id for the target document.
   * @param anchorText Anchor text object.
   */
	public float getWeight(int targetDocument, AnchorText anchorText);
}
