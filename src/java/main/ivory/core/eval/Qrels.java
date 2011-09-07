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

package ivory.core.eval;

import ivory.core.util.DelimitedValuesFileReader;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

/**
 * <p>
 * Representation of relevance judgments. In TREC parlance, qrels are judgments made by humans as to
 * whether a document is relevant to an information need (i.e., topic). Typically, qrels are created
 * by a process known as "pooling" in large-scale system evaluations such as those at TREC.
 * </p>
 * 
 * @author Jimmy Lin
 */
public class Qrels {
  private SortedMap<String, Map<String, Boolean>> data = Maps.newTreeMap();

  private float topics = 0;

  /**
   * Creates a {@code Qrels} object from a file
   *
   * @param file file containing qrels
   */
  public Qrels(String file) {
    DelimitedValuesFileReader iter = new DelimitedValuesFileReader(file, " ");

    String[] arr;
    while ((arr = iter.nextValues()) != null) {
      String qno = arr[0];
      String docno = arr[2];
      boolean rel = arr[3].equals("0") ? false : true;

      if (data.containsKey(qno)) {
        data.get(qno).put(docno, rel);
      } else {
        Map<String, Boolean> t = new HashMap<String, Boolean>();
        t.put(docno, rel);
        data.put(qno, t);
      }
    }
  }

  /**
   * Determines if a document is relevant for a topic.
   *
   * @param qid topic id
   * @param docid id of the document to test
   * @return {@code true} if the document is relevant
   */
  public boolean isRelevant(String qid, String docid) {
    if (!data.containsKey(qid))
      return false;

    if (!data.get(qid).containsKey(docid))
      return false;

    return data.get(qid).get(docid);
  }

  /**
   * Returns the set of relevant documents for a topic.
   *
   * @param qid topic id
   * @return the set of relevant documents
   */
  public Set<String> getReldocsForQid(String qid) {
    Set<String> set = new TreeSet<String>();

    if (!data.containsKey(qid)) {
      return set;
    }

    topics++;

    for (Entry<String, Boolean> e : data.get(qid).entrySet()) {
      if (e.getValue()) {
        set.add(e.getKey());
      }
    }

    return set;
  }

  /**
   * Returns a set containing the topic ids.
   *
   * @return a set containing the topic ids
   */
  public Set<String> getQids() {
    return data.keySet();
  }

  /**
   * Used with RunQueryHDFSTrainWSD class
   * 
   * @return number of topics needed by RunQueryHDFSTrainWSD to compute effectiveness scores
   */
  public float helperHDFSTrainWSDTopics() {
    return topics;
  }
}
