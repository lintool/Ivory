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

package ivory.core.data.index;

import ivory.core.index.TermPositions;

import java.io.IOException;


import org.apache.hadoop.io.Writable;


/**
 * <p>
 * Interface representing a postings list, which contains a list of individual
 * postings. The ordering of the postings depends on the concrete
 * implementation. Postings lists may or may not store positional information.
 * </p>
 * 
 * <p>
 * Note that there are subtle but important differences between:
 * </p>
 * 
 * <ul>
 * 
 * <li>{@link #size()}, which returns the number of postings that have been
 * added to this list (i.e., intermediate state).</li>
 * 
 * <li>{@link #getNumberOfPostings()} and {@link #setNumberOfPostings(int)},
 * which return and set the number of postings that will ultimately be added to
 * this list (i.e., final state). For many encoding schemes, this is required to
 * be known in advance (i.e., before any postings are added to the list).</li>
 * 
 * <li> {@link #getDf()}, which returns the document frequency of the term
 * associated with this postings list. In a large collection with many document
 * partition, this returns the global document frequency (whereas the previous
 * two methods deal with <i>this</i> specific partition). In a collection with
 * only a single partition, this is exactly the same value as
 * {@link #getNumberOfPostings()}</li>
 * 
 * </ul>
 * 
 * @author Jimmy Lin
 */
public interface PostingsList extends Writable {

	/**
	 * Adds a posting to this postings list.
	 * 
	 * @param docno
	 *            docno of the posting
	 * @param score
	 *            score of the posting
	 * @param pos
	 *            positional information (optional)
	 */
	public void add(int docno, short score, TermPositions pos);

	/**
	 * Returns the number of postings that have been added to this postings
	 * list. Note that under most circumstances this method will return a
	 * different value than {@link #getNumberOfPostings()}, which returns the
	 * number of postings that will eventually be added (required for certain
	 * encoding schemes).
	 */
	public int size();

	/**
	 * Clears this postings list.
	 */
	public void clear();

	/**
	 * Returns the {@link PostingsReader} associated with this postings list.
	 */
	public PostingsReader getPostingsReader();

	/**
	 * Sets the number of documents in this collection. This value is necessary
	 * for certain encoding schemes (e.g., Golomb encoding). This method is
	 * typically called <i>before</i> postings are adding to the postings list.
	 */
	public void setCollectionDocumentCount(int docs);

	/**
	 * Returns the number of documents in this collection.
	 */
	public int getCollectionDocumentCount();

	/**
	 * Sets the number of postings that will be added to this list. For certain
	 * encoding schemes (e.g., Golomb encoding), this value is required before
	 * any postings are added.
	 */
	public void setNumberOfPostings(int n);

	/**
	 * Returns the number of postings that will be added to this list. That is,
	 * this method returns the value set by {@link #setNumberOfPostings(int)}.
	 * Note that under most circumstances this method will return a different
	 * value than {@link #size()}, which returns the number of postings that
	 * have already been added.
	 */
	public int getNumberOfPostings();

	/**
	 * Returns the document frequency of the term associated with this postings
	 * list. Note this is may be different from {@link #getNumberOfPostings()}
	 * because of document partitioning for large collections. In these cases,
	 * this method should return the global document frequency, whereas
	 * {@link #getNumberOfPostings()} should return the document frequency <i>in
	 * this partition</i>. In a collection with only a single partition, this
	 * is exactly the same value as {@link #getNumberOfPostings()}. This method
	 * is not meaningful for impact-sorted indexes.
	 */
	public int getDf();

	public void setDf(int df);

	/**
	 * Returns the collection frequency of the term associated with this
	 * postings list. This method is not meaningful for impact-sorted indexes.
	 */
	public long getCf();

	public void setCf(long cf);

	/**
	 * Returns the raw byte array representation of this postings list.
	 */
	public byte[] getRawBytes();
	
	public byte[] serialize() throws IOException;
}
