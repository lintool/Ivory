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

import ivory.ptc.data.PseudoJudgments;
import ivory.ptc.data.PseudoQuery;
import edu.umd.cloud9.util.map.HMapII;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



/**
 * Sampling criterion based on the length of the query.
 *
 * Required parameters:
 * - Counts table: &lt;length of query&gt;\t&lt;number of instances to sample&gt;
 * - Integer: minimum number of target documents anchor text must have
 * - Integer: maximum number of target documents anchor text must have
 *
 * @author Nima Asadi
 */
public class LengthCountCriterion implements Criterion {
	private HMapII counts;
	private int minNumberTargets;
	private int maxNumberTargets;

	@SuppressWarnings("deprecation")
	@Override
	public void initialize(FileSystem fs, String... params) {
		if (params.length != 3) {
			throw new RuntimeException(toString() + ": Missing parameter(s).\n"
			    + "1-Path to the Counts Table: A text file consisting of one "
					    + "<length-of-anchor-text>\t<number-of-instances-to-sample> record per line.)\n"
					        + "2-Integer: Minimum number of target documents\n"
					            + "3-Integer: Maximum number of target documents");
		}
		counts = new HMapII();
		minNumberTargets = Integer.parseInt(params[1]);
		maxNumberTargets = Integer.parseInt(params[2]);

		try {
			FSDataInputStream in = fs.open(new Path(params[0]));
			String next;
			String[] records;
			while((next = in.readLine()) != null) {
				records = next.split("\t");
				counts.put(Integer.parseInt(records[0].trim()),
				    Integer.parseInt(records[1].trim()));
			}
			in.close();
		} catch (Exception e) {
			throw new RuntimeException("Initialization Failed: Error reading the counts table!", e);
		}
	}

  @Override
	public boolean meets(PseudoQuery query, PseudoJudgments judgments) {
		if (judgments.size() < minNumberTargets || judgments.size() > maxNumberTargets) {
			return false;
		}
		int index = query.getQuery().split("\\s+").length;
		if (!counts.containsKey(index)) {
			return false;
		}
		int count = counts.get(index);
		if (count > 0) {
			counts.put(index, count - 1);
			return true;
		}
		return false;
	}

  @Override
	public String toString() {
		return "LengthCountCriterion";
	}
}
