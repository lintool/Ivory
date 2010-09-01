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

import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * Object representing a Markov Random Field.
 * </p>
 * 
 * @author Don Metzler
 */
public class MarkovRandomField {

	/**
	 * Retrieval environment
	 */
	protected RetrievalEnvironment mEnv = null;

	/**
	 * Global evidence associated with this MRF.
	 */
	protected GlobalEvidence mGlobalEvidence = null;

	/**
	 * List of cliques associated with this MRF.
	 */
	protected List<Clique> mCliques = null;

	/**
	 * Query terms associated with this MRF.
	 */
	protected String[] mQueryTerms = null;
	
	/**
	 * <p>
	 * Creates a <code>MarkovRandomField</code> object.
	 * </p>
	 * 
	 * @param queryTerms
	 *            query terms
	 * @param env
	 *            retrieval environment (for global evidence)
	 */
	public MarkovRandomField(String[] queryTerms, RetrievalEnvironment env) {
		mEnv = env;
		
		mQueryTerms = queryTerms;

		// initialize cliques
		mCliques = new ArrayList<Clique>();

		// set global evidence
		mGlobalEvidence = new GlobalEvidence(env.documentCount(), env.termCount(),
				queryTerms.length);
	}

	public void initialize() throws Exception {
		mEnv.clearPostingsReaderCache();
		for(Clique c : mCliques) {
			c.initialize(mGlobalEvidence);
		}
	}
	
	/**
	 * Returns the log of the unnormalized joint probability of the current
	 * graph configuration.
	 */
	public double getLogUnormalizedProb() throws Exception {
		double prob = 0.0;

		for (Clique c : mCliques) {
			prob += c.getPotential();
		}

		return prob;
	}

	/**
	 * Returns the nodes associated with this MRF.
	 */
	public List<GraphNode> getNodes() {
		ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
		for (Clique clique : mCliques) {
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
	 * @param c
	 */
	public void addClique(Clique c) {
		mCliques.add(c);
	}

	/**
	 * @return Returns the cliques associated with this MRF.
	 */
	public List<Clique> getCliques() {
		return mCliques;
	}

	public int getNextCandidate() {
		int nextCandidate = Integer.MAX_VALUE;

		for (Clique clique : mCliques) {
			int candidate = clique.getNextCandidate();
			if (candidate < nextCandidate) {
				nextCandidate = candidate;
			}
		}

		return nextCandidate;
	}

	/**
	 * Sets the <code>GlobalEvidence</code> associated with this MRF.
	 */
	public void setGlobalEvidence(GlobalEvidence evidence) {
		mGlobalEvidence = evidence;
	}

	/**
	 * Returns the <code>GlobalEvidence</code> associated with this MRF.
	 */
	public GlobalEvidence getGlobalEvidence() {
		return mGlobalEvidence;
	}

	public String[] getQueryTerms() {
		return mQueryTerms;
	}

	@Override
	public String toString() {
		return toString(false);
	}

	public String toString(boolean verbose) {
		StringBuilder sb = new StringBuilder("<mrf>\n");

		for (Iterator<Clique> cliqueIter = mCliques.iterator(); cliqueIter.hasNext();) {
			sb.append(cliqueIter.next().toString(verbose));
		}
		return XMLTools.format(sb.append("</mrf>").toString());
	}


}
