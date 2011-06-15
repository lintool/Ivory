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

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Interface representing a document vector where the terms are represented by
 * integer term ids.
 * 
 * @author Tamer Elsayed
 * 
 */
public interface IntDocVector extends Writable {

	/**
	 * Returns the reader for this <code>IntDocVector</code>.
	 */
	public abstract IntDocVector.Reader getReader() throws IOException;

	/**
	 * Interface representing a reader for a {@link IntDocVector}, providing access
	 * to terms, their frequencies, and positions.
	 * 
	 * @author Tamer Elsayed
	 * 
	 */
	public interface Reader {

	  /**
	   * Returns the next term.
	   */
	  public int nextTerm();

	  /**
	   * Returns <code>true</code> if there are more terms to read.
	   */
	  public boolean hasMoreTerms();

	  /**
	   * Returns the total number of terms.
	   */
	  public int getNumberOfTerms();

	  /**
	   * Resets the reader.
	   */
	  public void reset();

	  /**
	   * Returns the position offsets of the current term as an array.
	   */
	  public int[] getPositions();

	  /**
	   * Returns the position offsets of the current term as a
	   * {@link TermPositions} object.
	   */
	  public boolean getPositions(TermPositions tp);

	  /**
	   * Returns the term frequency of the current term.
	   */
	  public short getTf();
	}

}