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
 * Interface to object that keeps track of the collection frequency of each term
 * in the collection. Concrete classes may vary in terms of implementation,
 * e.g., hashes (faster lookup, but less memory efficient) or arrays (slower
 * binary search lookup, but more memory efficient).
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public interface CfTable {

	/**
	 * Returns the collection frequency of a term.
	 */
	public long getCf(int term);

	/**
	 * Returns the total number of terms in this collection.
	 */
	public long getCollectionSize();

	/**
	 * Returns the number of unique terms in the collection.
	 */
	public int getVocabularySize();

	/**
	 * Returns the collection frequency of the term with the highest collection
	 * frequency.
	 */
	public long getMaxCf();

	/**
	 * Returns the term with the highest collection frequency.
	 */
	public int getMaxCfTerm();

	/**
	 * Returns the number of terms that only appear once in the collection.
	 */
	public int getCountOfTermWithCfOne();
}
