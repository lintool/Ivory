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

import ivory.data.DocScoreTable4BF;
import ivory.data.SpamPercentileScore;

/**
 * PageRank weighting scheme:
 *
 *    weight(targetDocument) = logScale_spamminess(targetDocument)
 *                                + logScale_PageRank(targetDocument)
 *
 * Parameters required:
 * - Percentile spam scores (can be obtained from
 *   http://plg.uwaterloo.ca/~gvcormac/clueweb09spam/)
 * - PageRank scores
 *
 * @author Nima Asadi
 */
public class PageRank implements WeightingScheme {
	private SpamPercentileScore scoresSpam;
	private DocScoreTable4BF scoresPageRank;

  @Override
	public float getWeight(int targetDocument, AnchorText anchorText) {
		return scoresSpam.getScore(targetDocument) + scoresPageRank.getScore(targetDocument);
	}

  @Override
	public void initialize(FileSystem fs, String... params) {
		if (params.length != 2) {
			throw new RuntimeException("Missing score table. " + toString()
			    + " requires two parameters as follows: <SpamScores, PageRankScores>.");
		}
		scoresSpam = new SpamPercentileScore();
		scoresPageRank = new DocScoreTable4BF();

		try {
			scoresSpam.initialize(params[0], fs);
			scoresPageRank.initialize(params[1], fs);
		} catch (Exception e) {
			throw new RuntimeException("Error reading scores!", e);
		}
	}

  @Override
	public String toString() {
		return "PageRank";
	}
}
