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

package ivory.smrf.model;

import ivory.util.RetrievalEnvironment;
import ivory.util.TextTools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class MarkovRandomField {

	/**
	 * global evidence
	 */
	protected GlobalEvidence mGlobalEvidence = null;
	
	/**
	 * 	the cliques associated with this MRF 
	 */
	protected List<Clique> mCliques = null;
	
	/**
	 * @param queryText
	 * @param env
	 */
	public MarkovRandomField(String queryText, RetrievalEnvironment env) {
		// initialize cliques
		mCliques = new ArrayList<Clique>();
		
		// set global evidence
		mGlobalEvidence = new GlobalEvidence(env.documentCount(), env.termCount(), TextTools.countTokens(queryText));
	}

	/**
	 * @return Returns the log of the unormalized joint
	 *         probability of the current graph configuration.
	 * @throws SMRFException 
	 * @throws Exception
	 */
	public double getLogUnormalizedProb() throws Exception {
		double prob = 0.0;
		
		for (Clique c : mCliques) {
			prob += c.getPotential();
		}

		return prob;
	}
		
	/**
	 * @return Returns the nodes associated with this MRF.
	 */
	// TODO: this is rather inefficient, but should be okay for small graphs
	public List<Node> getNodes() {
		ArrayList<Node> nodes = new ArrayList<Node>();
		for (Clique clique : mCliques) {
			List<Node> cliqueNodes = clique.getNodes();
			for (Node node : cliqueNodes) {
				if( !nodes.contains( node ) ) {
					nodes.add( node );
				}
			}
		}
		return nodes;
	}
	
	/**
	 * @param c
	 */
	public void addClique(Clique c) {
		mCliques.add( c );
	}
	
	/**
	 * @return Returns the cliques associated with this MRF.
	 */
	public List<Clique> getCliques() {
		return mCliques;
	}

	public int getNextCandidate() {
		int nextCandidate = Integer.MAX_VALUE;
		
		for(Clique clique : mCliques) {
			int candidate = clique.getNextCandidate();
			if( candidate < nextCandidate ) {
				nextCandidate = candidate;
			}
		}
		
		return nextCandidate;
	}
	
	/**
	 * @param evidence
	 */
	public void setGlobalEvidence(GlobalEvidence evidence) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * @return global evidence hash
	 */
	public GlobalEvidence getGlobalEvidence() {
		return mGlobalEvidence;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String ret = new String();
		ret = "<mrf>\n";
		for( Iterator<Clique> cliqueIter = mCliques.iterator(); cliqueIter.hasNext(); ) {
			ret += cliqueIter.next();
		}		
		return ret + "</mrf>\n";
	}
}
