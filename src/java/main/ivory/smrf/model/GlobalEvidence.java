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

package ivory.smrf.model;

/**
 * Object encapsulating collection-level global evidence for ranking purposes.
 *
 * @author Don Metzler
 */
public class GlobalEvidence {
  public long numDocs;          // Number of documents in the collection.
  public long collectionLength; // Collection length.
  public int queryLength;       // Query length.

  /**
   * Creates a {@code GlobalEvidence} object.
   *
   * @param ndocs number of documents in the collection
   * @param collen collection length
   * @param querylen query length
   */
  public GlobalEvidence(long ndocs, long collen, int querylen) {
    this.numDocs = ndocs;
    this.collectionLength = collen;
    this.queryLength = querylen;
  }
}
