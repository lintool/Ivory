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

import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.core.util.XMLTools;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A Markov Random Field.
 *
 * @author Don Metzler
 */
public class MarkovRandomField {
  private final List<Clique> cliques = Lists.newArrayList();
  private final RetrievalEnvironment env;
  private final GlobalEvidence globalEvidence;
  private final String[] queryTerms;

  /**
   * Creates a {@code MarkovRandomField} object.
   *
   * @param queryTerms query terms
   * @param env retrieval environment (for computing global evidence)
   */
  public MarkovRandomField(String[] queryTerms, RetrievalEnvironment env) {
    this.queryTerms = Preconditions.checkNotNull(queryTerms);
    this.env = Preconditions.checkNotNull(env);
    this.globalEvidence = new GlobalEvidence(env.getDocumentCount(), env.getCollectionSize(),
        queryTerms.length);
  }

  /**
   * Initializes this MRF.
   */
  public void initialize() throws ConfigurationException {
    env.clearPostingsReaderCache();
    for (Clique c : cliques) {
      c.initialize(globalEvidence);
    }
  }

  /**
   * Returns the nodes associated with this MRF.
   *
   * @return list of nodes associated with this MRF.
   */
  public List<GraphNode> getNodes() {
    List<GraphNode> nodes = Lists.newArrayList();
    for (Clique clique : cliques) {
      List<GraphNode> cliqueNodes = clique.getNodes();
      for (GraphNode node : cliqueNodes) {
        if (!nodes.contains(node)) {
          nodes.add(node);
        }
      }
    }
    return nodes;
  }

  /**
   * Adds a clique to this MRF.
   *
   * @param c clique to add
   */
  public void addClique(Clique c) {
    cliques.add(c);
  }

  /**
   * Removes all cliques from this MRF.
   */
  public void removeAllCliques() {
    cliques.clear();
  }

  /**
   * Returns the cliques in this MRF.
   *
   * @return all cliques in this MRF
   */
  public List<Clique> getCliques() {
    return cliques;
  }

  /**
   * Returns the next candidate for scoring.
   *
   * @return docno of the next candidate
   */
  public int getNextCandidate() {
    int nextCandidate = Integer.MAX_VALUE;

    for (Clique clique : cliques) {
      int candidate = clique.getNextCandidate();
      if (candidate < nextCandidate) {
        nextCandidate = candidate;
      }
    }

    return nextCandidate;
  }

  /**
   * Returns the {@code GlobalEvidence} associated with this MRF.
   *
   * @return {@code GlobalEvidence} associated with this MRF
   */
  public GlobalEvidence getGlobalEvidence() {
    return globalEvidence;
  }

  /**
   * Returns the query terms.
   *
   * @return query terms
   */
  public String[] getQueryTerms() {
    return queryTerms;
  }

  /**
   * Returns a human-readable representation of this MRF.
   *
   * @return human-readable representation of this MRF
   */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Returns a human-readable representation of this MRF.
   *
   * @param verbose verbose output
   */
  public String toString(boolean verbose) {
    StringBuilder sb = new StringBuilder("<mrf>\n");

    for (Iterator<Clique> cliqueIter = cliques.iterator(); cliqueIter.hasNext();) {
      sb.append(cliqueIter.next().toString(verbose));
    }
    return XMLTools.format(sb.append("</mrf>").toString());
  }

  /**
   * Returns the retrieval environment associated with this MRF.
   *
   * @return the retrieval environment associated with this MRF
   */
  public RetrievalEnvironment getRetrievalEnvironment() {
    return env;
  }
}
