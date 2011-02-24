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
 * Interface representing an object that keeps track of the length of each
 * document in the collection. Document lengths are measured in number of terms.
 * The number of documents <i>n</i> is provided by {@link #getDocCount()}, and
 * the documents are consecutively numbered, starting from <i>d</i> + 1, where
 * <i>d</i> is the by value provided by {@link #getDocnoOffset()}.
 * </p>
 * 
 * <p>
 * The notion of docno offset is necessary for large document collections that
 * are partitioned, where docnos need to be consecutively numbered across
 * partitions. For example, the first English segment of ClueWeb09 contains
 * 50,220,423 documents, has a docno offset of 0, and contains documents
 * numbered from 1 to 50,220,423. The second segment has a docno offset of
 * 50,220,423 and begins with docno 50,220,424. By convention, docnos are
 * numbered starting at one because it is impossible to code zero using certain
 * schemes (e.g., gamma codes).
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

	/**
	 * Returns the first docno in this collection. All documents are number
	 * consecutively from this value.
	 */
	public int getDocnoOffset();

	/**
	 * Returns the average document length.
	 */
	public float getAvgDocLength();

	/**
	 * Returns number of documents in the collection.
	 */
	public int getDocCount();
}
