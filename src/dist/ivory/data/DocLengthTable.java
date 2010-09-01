/*
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

package ivory.data;


/**
 * <p>
 * Object that keeps track of the length of each document in the collection.
 * Document lengths are measured in number of terms.
 * </p>
 * 
 * <p>
 * Document length data is stored in a serialized data file, in the following
 * format: the data file consists of one long stream of integers. The first
 * integer in the stream specifies the number of documents in the collection (<i>n</i>).
 * Thereafter, the input stream contains exactly <i>n</i> integers, one for
 * every document in the collection. Since the documents are numbered
 * sequentially, each one of these integers corresponds to the length of
 * documents 1 ... <i>n</i> in the collection. Note that documents are numbered
 * starting from one because it is impossible to express zero in many
 * compression schemes (e.g., Golomb encoding).
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public interface DocLengthTable {

	/**
	 * Returns the length of a document.
	 */
	public int getDocLength(int docno);

	public int getDocnoOffset();

	public float getAvgDocLength();

	/**
	 * Returns number of documents in the collection.
	 */
	public int getDocCount();

}
