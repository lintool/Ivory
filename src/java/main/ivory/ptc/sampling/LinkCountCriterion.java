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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tl.lin.data.map.HMapII;



/**
 * Sampling criterion based on the number of target documents.
 *
 * Required parameters:
 * - Counts table: &lt;number of target documents&gt; \t &lt;number of instances to sample&gt;
 *
 * @author Nima Asadi
 */
public class LinkCountCriterion implements Criterion {
	private HMapII counts;

	@SuppressWarnings("deprecation")
	@Override
	public void initialize(FileSystem fs, String... params) {
		if (params.length != 1) {
			throw new RuntimeException(toString() + ": Missing counts table (path to a text file consisting of one "
			    + "<number-of-target-documents>\t<number-of-instances-to-sample> record per line.)");
		}
		counts = new HMapII();

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
		int index = judgments.size();
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
		return "LinkCountCriterion";
	}
}
