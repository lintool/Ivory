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

package ivory.core.data.dictionary;

/**
 * A dictionary provides a bidirectional mapping terms (Strings) and term ids (integers). The
 * semantics of the mapping is left unspecified, but the iteration order is always in
 * increasing term id. 
 *
 * @author Jimmy Lin
 */
public interface Dictionary extends Iterable<String> {
  /**
   * Returns the term associated with this term id.
   * @param id term id
   * @return term associated with this term id
   */
  String getTerm(int id);

  /**
   * Returns the id associated with this term.
   * @param term term
   * @return id associated with this term
   */
  int getId(String term);

  /**
   * Returns the size of this dictionary. 
   * @return number of terms in this dictionary
   */
  int size();
}
