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

package ivory.core.data.stat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public interface DocScoreTable {
  /**
   * Initializes this object.
   */
  void initialize(String file, FileSystem fs) throws IOException;

  /**
   * Returns the score of a document.
   */
  float getScore(int docno);

  /**
   * Returns the first docno in this collection. All documents are number
   * consecutively from this value.
   */
  int getDocnoOffset();

  /**
   * Returns number of documents in the collection.
   */
  int getDocCount();
}
