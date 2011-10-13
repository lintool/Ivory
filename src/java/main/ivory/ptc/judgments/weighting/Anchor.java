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

import ivory.core.data.stat.SpamPercentileScore;
import edu.umd.cloud9.webgraph.data.AnchorText;

import org.apache.hadoop.fs.FileSystem;



/**
 * Weighting scheme originally introduced in
 * http://portal.acm.org/citation.cfm?id=1571981
 *
 * Parameters required: Percentile spam scores (can be
 * obtained from http://plg.uwaterloo.ca/~gvcormac/clueweb09spam/)
 * @author Nima Asadi
 */
public class Anchor implements WeightingScheme {
	private SpamPercentileScore scoresSpam;

  @Override
	public float getWeight(int targetDocument, AnchorText anchorText) {
		return anchorText.getWeight() * (scoresSpam.getRawScore(targetDocument) / 100.0f);
	}

  @Override
	public void initialize(FileSystem fs, String... params) {
		if (params.length != 1) {
			throw new RuntimeException("Missing score table. " + toString()
			    + " requires one parameter as follows: <SpamScore>");
		}
		scoresSpam = new SpamPercentileScore();
		try {
			scoresSpam.initialize(params[0], fs);
		} catch (Exception e) {
			throw new RuntimeException("Error reading scores!", e);
		}
	}

  @Override
	public String toString() {
		return "Anchor";
	}
}
