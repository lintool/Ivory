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

import ivory.exception.ConfigurationException;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;
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
	private List<Clique> cliques = new ArrayList<Clique>();
	private final RetrievalEnvironment env;
	private final GlobalEvidence globalEvidence;
	private final String[] queryTerms;
	
	/**
	 * Creates a <code>MarkovRandomField</code> object.
	 *
	 * @param queryTerms query terms
	 * @param env        retrieval environment (for computing global evidence)
	 */
	public MarkovRandomField(String[] queryTerms, RetrievalEnvironment env) {
		this.queryTerms = Preconditions.checkNotNull(queryTerms);
		this.env = Preconditions.checkNotNull(env);
		this.globalEvidence = new GlobalEvidence(env.getDocumentCount(), env.getCollectionSize(), queryTerms.length);
	}

	/**
	 * Initializes this MRF.
	 */
	public void initialize() throws ConfigurationException {
		env.clearPostingsReaderCache();
		for(Clique c : cliques) {
			c.initialize(globalEvidence);
		}
	}
	
	/**
	 * Returns the nodes associated with this MRF.
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
	 */
	public void addClique(Clique c) {
		cliques.add(c);
	}

	/**             
         * Removes all cliques from this MRF.
         */
	public void removeAllCliques(){
                cliques = new ArrayList<Clique>();
        }


	/**
	 * Returns the cliques associated with this MRF.
	 */
	public List<Clique> getCliques() {
		return cliques;
	}

	/**
	 * Returns the next candidate for scoring.
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
	 * Returns the <code>GlobalEvidence</code> associated with this MRF.
	 */
	public GlobalEvidence getGlobalEvidence() {
		return globalEvidence;
	}

	/**
	 * Returns the query terms.
	 */
	public String[] getQueryTerms() {
		return queryTerms;
	}

	/**
	 * Returns a human-readable representation of this MRF.
	 */
	@Override
	public String toString() {
		return toString(false);
	}

	/**
	 * Returns a human-readable representation of this MRF.
	 * @param verbose verbose output
	 */
	public String toString(boolean verbose) {
		StringBuilder sb = new StringBuilder("<mrf>\n");

		for (Iterator<Clique> cliqueIter = cliques.iterator(); cliqueIter.hasNext();) {
			sb.append(cliqueIter.next().toString(verbose));
		}
		return XMLTools.format(sb.append("</mrf>").toString());
	}
}
