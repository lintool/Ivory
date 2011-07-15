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

import edu.umd.cloud9.util.array.ArrayListOfFloats;
import edu.umd.cloud9.util.array.ArrayListOfInts;

import org.apache.hadoop.io.Writable;

/**
 * Data structure that represents a set of pseudo relevence judgments.
 * This structure holds a list of documents and their corresponding weights.
 * The weights persumably indicate the relevance of a document to a pseudo
 * query object.
 *
 * @author Nima Asadi
 */
public class PseudoJudgments implements Writable {
  // Parallel arrays to handle documents and their associated weights
	private final ArrayListOfInts docnos;
	private final ArrayListOfFloats weights;

  /**
   * Constructs a pseudo judgment with an empty list of documents
   */
	public PseudoJudgments() {
		docnos = new ArrayListOfInts();
		weights = new ArrayListOfFloats();
	}

  @Override
	public void readFields(DataInput in) throws IOException {
		int n = in.readInt();

		docnos.clear();
		for(int i = 0; i < n; i++) {
			docnos.add(in.readInt());
		}

		weights.clear();
		for(int i = 0; i < n; i++) {
			weights.add(in.readFloat());
		}
	}

  @Override
	public void write(DataOutput out) throws IOException {
		if (docnos.size() != weights.size()) {
			throw new RuntimeException("Error writing qrels: " + docnos.size() + ", " + weights.size());
		}
		if (docnos.size() == 0) {
			throw new RuntimeException("Error writing qrels: empty list!");
		}
		// Writing the number of the pseudo judgments
		out.writeInt(docnos.size());

		for(int i = 0; i < docnos.size(); i++) {
			out.writeInt(docnos.get(i));
		}

		for(int i = 0; i < weights.size(); i++){
			out.writeFloat(weights.get(i));
		}
	}

  @Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[");
		for(int i = 0; i < docnos.size(); i++) {
			buffer.append("<" + docnos.get(i) + ", " + weights.get(i) + ">");
		}
		buffer.append("]");
		return buffer.toString();
	}

  /**
   * Resets the current pseudo judgment.
   */
	public void clear() {
		docnos.clear();
		weights.clear();
	}

  /**
   * Retrieves the number of pseudo judgments.
   *
   * @return number of pseudo judgments.
   */
	public int size() {
		if(docnos.size() != weights.size()) {
			throw new RuntimeException("Error in qrels: " + docnos.size() + ", " + weights.size());
		}
		return docnos.size();
	}

  /**
   * @return sum of the weights associated with the pseudo judgments.
   */
	public float sumWeights() {
		float sum = 0;
		for(int i = 0; i < weights.size(); i++) {
			sum += weights.get(i);
		}
		return sum;
	}

  /**
   * Retrieves the document id at the given index.
   *
   * @param i Index
   * @return document id at the given index
   */
	public int getDocno(int i) {
		return docnos.get(i);
	}

  /**
   * Retrieves the weight for the document at the given index.
   *
   * @param i Index
   * @return weight of the document at the given index
   */
	public float getWeight(int i) {
		return weights.get(i);
	}

  /**
   * Adds a pseudo relevence judgment to the current object.
   *
   * @param docno Document id.
   * @param weight Relevence score.
   */
	public void add(int docno, float weight) {
		docnos.add(docno);
		weights.add(weight);
	}
}
