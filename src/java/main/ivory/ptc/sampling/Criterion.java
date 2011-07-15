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

package ivory.ptc.sampling;

import org.apache.hadoop.fs.FileSystem;

import ivory.ptc.data.PseudoJudgments;
import ivory.ptc.data.PseudoQuery;

/**
 * Criterion for sampling pseudo judgments.
 *
 * @author Nima Asadi
 */
public interface Criterion {
  /**
   * Initializes the criterion with the necessary parameters.
   *
   * @param fs FileSystem.
   * @param params Parameters needed to initialize the criterion.
   */
	public void initialize(FileSystem fs, String... params);

  /**
   * Checks whether the given query and set of judgments meet the
   * defined criterion.
   *
   * @param query Pseudo query.
   * @param judgments Set of pseudo judgments.
   * @return whether or not the configuration meets the criterion.
   */
	public boolean meets(PseudoQuery query, PseudoJudgments judgments);
}
