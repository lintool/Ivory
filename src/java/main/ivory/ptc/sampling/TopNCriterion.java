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
 * Returns top N pseudo queries and judgments passed to this
 * criterion.
 *
 * Required Parameters:
 *  - Integer N
 *
 * @author Nima Asadi
 */
public class TopNCriterion implements Criterion {
	private int n;

  @Override
	public void initialize(FileSystem fs, String... params) {
		if (params.length != 1) {
			throw new RuntimeException(toString() + ": Missing N.");
		}
		n = Integer.parseInt(params[0]);
	}

  @Override
	public boolean meets(PseudoQuery query, PseudoJudgments judgments) {
		if (n-- > 0) {
			return true;
		}
		return false;
	}

  @Override
	public String toString() {
		return "TopNCritrion";
	}
}
