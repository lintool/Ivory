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

import ivory.index.TermPositions;

/**
 * <p>
 * Interface for a postings reader.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public interface PostingsReader {
	/**
	 * Reads the next posting, consuming it from the stream of postings.
	 * 
	 * @param posting
	 *            object for holding the posting
	 * @return <code>true</code> if posting successfully read,
	 *         <code>false</code> otherwise
	 */
	public boolean nextPosting(Posting posting);

	/**
	 * Checks to see if there are any more postings to be read.
	 * 
	 * @return <code>true</code> if there are any more postings to be read,
	 *         <code>false</code> otherwise
	 */
	public boolean hasMorePostings();

	/**
	 * Returns the number of postings in this postings list. Note that this
	 * value <i>may not</i> be the same as the document frequency of the term
	 * (e.g., if this postings list contains only a partition of the entire
	 * collection).
	 */
	public int getNumberOfPostings();

	/**
	 * Resets this object to start reading from the first posting.
	 */
	public void reset();
	
	/**
	 * Returns an array of term positions corresponding to the current posting.
	 * This is an optional operation valid only for postings that store
	 * positional information.
	 */
	public int[] getPositions();

	/**
	 * Loads a <code>TermPositions</code> object corresponding to the current posting.
	 * This is an optional operation valid only for postings that store
	 * positional information.
	 * 
	 * @return <code>true</code> if there are any more postings to be read,
	 *         <code>false</code> otherwise
	 */
	public boolean getPositions(TermPositions tp);
	
	/**
	 * Returns the score of the next posting, without consuming it from the
	 * stream of postings. This is an optional operation.
	 */
	public short peekNextScore();

	/**
	 * Returns the docno of the next posting, without consuming it from the
	 * stream of postings. This is an optional operation.
	 */
	public int peekNextDocno();

	/**
	 * @return returns the score corresponding to the current posting
	 */
	public short getScore();

	/**
         * @return the window size of term proximity features
	 */
	public int getWindowSize();
	
	/**
	 * @return returns the docno corresponding to the current posting
	 */
	public int getDocno();
	
	/**
	 * Returns the <code>PostingsList</code> associated with this reader.
	 */
	public PostingsList getPostingsList();
	
	public byte[] getBytePositions();

	
}
