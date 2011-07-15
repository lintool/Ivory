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

package ivory.ptc.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Data structure that represents a pseudo query. Along with the text of the
 * pseudo query, this object provides a field that measures the quality of the
 * pseudo query as a quality score.
 *
 * @author Nima Asadi
 */
public class PseudoQuery implements WritableComparable<PseudoQuery> {
	private float score;
	private String query;

  /**
   * Constructs a pseudo query with no text and a quality score of zero.
   */
	public PseudoQuery() {
	}

  /**
   * Initializes a pseudo query object with the given setting.
   *
   * @param query Text of the pseudo query.
   * @param score Quality score of the pseudo query.
   */
	public PseudoQuery(String query, float score) {
		set(query, score);
	}

  @Override
	public void readFields(DataInput in) throws IOException {
		score = in.readFloat();
		query = in.readUTF();
	}

  @Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(score);
		out.writeUTF(query);
	}

  /**
   * Retrieves the quality score.
   *
   * @return quality score.
   */
	public float getScore() {
		return score;
	}

  /**
   * Retrieves the text of the pseudo query.
   *
   * @return text of the query.
   */
	public String getQuery() {
		return query;
	}

  /**
   * Sets the text and quality score to the given
   * text and quality score.
   *
   * @param query Text of the pseudo query.
   * @param score Quality score of the pseudo query.
   */
	public void set(String query, float score) {
		this.score = score;
		this.query = query;
	}

  @Override
	public boolean equals(Object obj) {
		PseudoQuery pair = (PseudoQuery) obj;
		return score == pair.getScore() && query.equalsIgnoreCase(pair.getQuery());
	}

  @Override
	public int compareTo(PseudoQuery other) {
		if (this.score > other.score) {
			return -1;
		} else if (this.score < other.score) {
			return 1;
		}
		return query.compareTo(other.query);
	}

  @Override
	public int hashCode() {
		return (int) score + query.hashCode();
	}

  @Override
	public String toString() {
		return "(" + query + ", " + score + ")";
	}
}
