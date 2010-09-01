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

package ivory.smrf.retrieval;

import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.MaxScoreCliqueSorter;
import ivory.smrf.model.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Don Metzler
 *
 */
public class MRFDocumentRanker {

	/**
	 * markov random field that we are using to generate the ranking
	 */
	private MarkovRandomField mMRF = null;
	
	/**
	 * 'document' nodes associated with the MRF 
	 */
	private List<DocumentNode> mDocNodes = null;
		
	/**
	 * maximum number of results to return 
	 */
	private int mNumResults = 1000;

	/**
	 * thread pool, shared across queries
	 */
	// TODO: make the number of threads configurable!
	private static ExecutorService sThreadPool = Executors.newFixedThreadPool(4);
	
	/**
	 * @param mrf
	 * @param numResults
	 */
	public MRFDocumentRanker( MarkovRandomField mrf, int numResults ) {
		mMRF = mrf;
		mNumResults = numResults;
		mDocNodes = _getDocNodes();		
	}
	
	/**
	 * @throws Exception
	 */
	public Accumulator [] rank() throws Exception {
		// use a priority queue for producing the ranking
		PriorityQueue<Accumulator> sortedAccumulators = new PriorityQueue<Accumulator>();

		// cliques associated with the MRF
		List<Clique> cliques = mMRF.getCliques();
	
		// maximum possible score that this MRF can achieve
		double mrfMaxScore = 0.0;
		
		// TODO: add step that optimizes MRF (removes unneccesary cliques, etc.)
		// _optimize( _mrf );
		
		// initialize cliques in parallel
		Map<Clique,Future<?>> cliqueFutures = new HashMap<Clique,Future<?>>();
		for(Clique c : cliques) {
			Future<?> future = sThreadPool.submit(new CliqueInitializeTask( c, mMRF.getGlobalEvidence() ) );
			cliqueFutures.put(c, future);
		}
		
		// wait for clique initialization to complete
		for( Clique c : cliques ) {
			cliqueFutures.get(c).get();
			mrfMaxScore += c.getMaxScore();
		}

		// sort cliques according to their max scores
		Collections.sort( cliques, new MaxScoreCliqueSorter());
		
		// score that must be achieved to enter result set
		double scoreThreshold = Double.NEGATIVE_INFINITY;
		
		int docid;
		while( ( docid = mMRF.getNextCandidate() ) < Integer.MAX_VALUE ) {
			// get accumulator for this document
			Accumulator a = new Accumulator(docid, 0.0);

			// score this clique for each document in the document set
			for (DocumentNode documentNode : mDocNodes) {
				documentNode.setDocumentID(docid);
			}

			// document-at-a-time scoring
			double docMaxScore = mrfMaxScore;
			boolean skipped = false;
			for(int i = 0; i < cliques.size(); i++) {
				// current clique that we're scoring
				Clique c = cliques.get(i);
				
				// if there's no way that this document can enter the result set then exit
				if(a.score + docMaxScore <= scoreThreshold) {
					// advance postings readers (but don't score)
					for(int j = i; j < cliques.size(); j++) {
						cliques.get(j).setNextCandidate(docid+1);
					}
					skipped = true;
					break;
				}
				
				// document independent cliques do not affect the ranking
				if(!c.isDocDependent()) {
					continue;
				}
				
				// update document score
				a.score += c.getPotential();
				
				// update the max score for the rest of the cliques
				docMaxScore -= c.getMaxScore();
			}
			
			// keep track of _numResults best accumulators
			if( !skipped && a.score > scoreThreshold ) {
				sortedAccumulators.add( a );
				if( sortedAccumulators.size() == mNumResults + 1) {
					sortedAccumulators.poll();
					scoreThreshold = sortedAccumulators.peek().score;
				}
			}				
		}
		
		// grab the accumulators off the stack, in (reverse) order
		Accumulator [] results = new Accumulator[ Math.min( mNumResults, sortedAccumulators.size() ) ];
		for( int i = 0; i < results.length; i++ ) {
			results[results.length - 1 - i] = sortedAccumulators.poll();
		}

		return results;
	}
	
	/**
	 * @return returns the markov random field associated with this ranker
	 */
	public MarkovRandomField getMRF() {
		return mMRF;
	}
	
	/**
	 * @param numResults number of results to return
	 */
	public void setNumResults( int numResults ) {
		mNumResults = numResults;
	}
	
	private List<DocumentNode> _getDocNodes() {
		ArrayList<DocumentNode> docNodes = new ArrayList<DocumentNode>();
		
		// check which of the nodes are DocumentNodes
		List<Node> nodes = mMRF.getNodes();
		for (Node node : nodes) {
			if( node instanceof DocumentNode ) {
				docNodes.add( (DocumentNode)node );
			}
		}
		return docNodes;
	}
}
