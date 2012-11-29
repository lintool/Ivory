/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

package ivory.core.data.document;

import ivory.core.data.index.TermPositions;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Interface representing a document vector where the terms are represented by
 * integer term ids.
 *
 * @author Tamer Elsayed
 */
public interface IntDocVector extends Writable {

  /**
   * Returns the reader for this {@code IntDocVector}.
   *
   * @return reader for this {@code IntDocVector}
   * @throws IOException
   */
  IntDocVector.Reader getReader() throws IOException;

  /**
   * Interface representing a reader for a {@link IntDocVector}, providing access
   * to terms, their frequencies, and positions.
   *
   * @author Tamer Elsayed
   */
  public interface Reader {
    /**
     * Returns the next term.
     *
     * @return termid of the next term
     */
    int nextTerm();

    /**
     * Returns {@code true} if there are more terms to read.
     *
     * @return {@code true} if there are more terms to read
     */
    boolean hasMoreTerms();

    /**
     * Returns the total number of terms in this document.
     *
     * @return total number of terms in this document
     */
    int getNumberOfTerms();

    /**
     * Resets the reader to the beginning of the document.
     */
    void reset();

    /**
     * Returns the position offsets of the current term as an array.
     *
     * @return array containing position offsets of the current term
     */
    int[] getPositions();

    /**
     * Returns the position offsets of the current term as a {@link TermPositions} object.
     *
     * @return {@code TermPositions} representing offsets of the current term.
     */
    boolean getPositions(TermPositions tp);

    /**
     * Returns the term frequency of the current term.
     *
     * @return term frequency of the current term
     */
    short getTf();
  }
}
