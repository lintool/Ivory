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
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.MaxScoreCliqueSorter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Don Metzler
 * 
 */
public class MRFDocumentRanker {

	private static final Logger sLogger = Logger.getLogger(MRFDocumentRanker.class);

	static {
		sLogger.setLevel(Level.WARN);
	}

	/**
	 * markov random field that we are using to generate the ranking
	 */
	private MarkovRandomField mMRF = null;

	/**
	 * if defined, only documents within this set will be scored
	 */
	private int [] mDocSet = null;
	
	/**
	 * 'document' nodes associated with the MRF
	 */
	private List<DocumentNode> mDocNodes = null;

	/**
	 * maximum number of results to return
	 */
	private int mNumResults = 1000;

	/**
	 * @param mrf
	 * @param numResults
	 */
	public MRFDocumentRanker(MarkovRandomField mrf, int numResults) {
		mMRF = mrf;
		mDocSet = null;
		mNumResults = numResults;
		mDocNodes = getDocNodes();
	}

	/**
	 * @param mrf
	 * @param docSet
	 * @param numResults
	 */
	public MRFDocumentRanker(MarkovRandomField mrf, int[] docSet, int numResults) {
		mMRF = mrf;
		mDocSet = docSet;
		mNumResults = numResults;
		mDocNodes = getDocNodes();
	}

	/**
	 * @throws Exception
	 */
	public Accumulator[] rank() throws Exception {
		// use a priority queue for producing the ranking
		PriorityQueue<Accumulator> sortedAccumulators = new PriorityQueue<Accumulator>();

		// cliques associated with the MRF
		List<Clique> cliques = mMRF.getCliques();

		// initialize the MRF
		mMRF.initialize();
		
		// maximum possible score that this MRF can achieve
		double mrfMaxScore = 0.0;
		for (Clique c : cliques) {
			mrfMaxScore += c.getMaxScore();
		}

		// sort cliques according to their max scores
		Collections.sort(cliques, new MaxScoreCliqueSorter());

		sLogger.info(mMRF.toString(true));

		// score that must be achieved to enter result set
		double scoreThreshold = Double.NEGATIVE_INFINITY;

		System.out.println("In MRFDocumentRanker, set of cliques to be used is :");
                for (Clique c : cliques){
                        System.out.println(c.getCliqueWeight()+" "+c);
                }

		// offset into document set we're currently at (if applicable)
		int docsetOffset = 0;
		
		int docid = 0;
		if(mDocSet != null) {
			if(docsetOffset < mDocSet.length) {
				docid = mDocSet[docsetOffset++];
			}
			else {
				docid = Integer.MAX_VALUE;
			}
		}
		else {
			docid = mMRF.getNextCandidate();
		}

		while (docid < Integer.MAX_VALUE) {
			// get accumulator for this document
			Accumulator a = new Accumulator(docid, 0.0);

			// score this clique for each document in the document set
			for (DocumentNode documentNode : mDocNodes) {
				documentNode.setDocno(docid);
			}

			// document-at-a-time scoring
			double docMaxScore = mrfMaxScore;
			boolean skipped = false;
			for (int i = 0; i < cliques.size(); i++) {
				// current clique that we're scoring
				Clique c = cliques.get(i);

				// if there's no way that this document can enter the result set
				// then exit
				if (a.score + docMaxScore <= scoreThreshold) {
					// advance postings readers (but don't score)
					for (int j = i; j < cliques.size(); j++) {
						cliques.get(j).setNextCandidate(docid + 1);
					}
					skipped = true;
					break;
				}

				// document independent cliques do not affect the ranking
				if (!c.isDocDependent()) {
					continue;
				}

				// update document score
				// a.score += c.getPotential();

				// value of feature function only
                                double cliqueScore = c.getPotential2();

				a.score += cliqueScore * c.getCliqueWeight();

				// update the max score for the rest of the cliques
				docMaxScore -= c.getMaxScore();
			}

			// keep track of _numResults best accumulators
			if (!skipped && a.score > scoreThreshold) {
				sortedAccumulators.add(a);
				if (sortedAccumulators.size() == mNumResults + 1) {
					sortedAccumulators.poll();
					scoreThreshold = sortedAccumulators.peek().score;
				}
			}

			if(mDocSet != null) {
				if(docsetOffset < mDocSet.length) {
					docid = mDocSet[docsetOffset++];
				}
				else {
					docid = Integer.MAX_VALUE;
				}
			}
			else {
				docid = mMRF.getNextCandidate();
			}
		}

		// grab the accumulators off the stack, in (reverse) order
		Accumulator[] results = new Accumulator[Math.min(mNumResults, sortedAccumulators.size())];
		for (int i = 0; i < results.length; i++) {
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
	 * @param numResults
	 *            number of results to return
	 */
	public void setNumResults(int numResults) {
		mNumResults = numResults;
	}

	private List<DocumentNode> getDocNodes() {
		ArrayList<DocumentNode> docNodes = new ArrayList<DocumentNode>();

		// check which of the nodes are DocumentNodes
		List<GraphNode> nodes = mMRF.getNodes();
		for (GraphNode node : nodes) {
			if (node instanceof DocumentNode) {
				docNodes.add((DocumentNode) node);
			}
		}
		return docNodes;
	}
}
