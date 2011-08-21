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

import ivory.core.exception.ConfigurationException;
import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.MarkovRandomField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	 * Pool of accumulators.
	 */
	private Accumulator[] mAccumulators = null;

	/**
	 * Sorted list of accumulators.
	 */
	private final PriorityQueue<Accumulator> mSortedAccumulators = new PriorityQueue<Accumulator>();

	/**
	 * Comparator used to sort cliques by their max score.
	 */
	private final Comparator<Clique> mMaxScoreComparator = new Clique.MaxScoreComparator();

	/**
	 * Markov Random Field that we are using to generate the ranking.
	 */
	private MarkovRandomField mMRF = null;

	/**
	 * If defined, only documents within this set will be scored.
	 */
	private int[] mDocSet = null;

	/**
	 * MRF document nodes.
	 */
	private List<DocumentNode> mDocNodes = null;

	/**
	 * Maximum number of results to return.
	 */
	private int mNumResults;

	public MRFDocumentRanker(MarkovRandomField mrf, int numResults) {
		this(mrf, null, numResults);
	}

	public MRFDocumentRanker(MarkovRandomField mrf, int[] docSet, int numResults) {
		mMRF = mrf;
		mDocSet = docSet;
		mNumResults = numResults;
		mDocNodes = getDocNodes();

		// Create single pool of reusable accumulators.
		mAccumulators = new Accumulator[numResults + 1];
		for (int i = 0; i < numResults + 1; i++) {
			mAccumulators[i] = new Accumulator(0, 0.0f);
		}
	}

	public Accumulator[] rank() {
		// Clear priority queue.
		mSortedAccumulators.clear();

		// Cliques associated with the MRF.
		List<Clique> cliques = mMRF.getCliques();

		// Current accumulator.
		Accumulator a = mAccumulators[0];

		// Initialize the MRF.
		try {
			mMRF.initialize();
		} catch (ConfigurationException e) {
			sLogger.error("Error initializing MRF. Aborting ranking!");
			return null;
		}

		// Maximum possible score that this MRF can achieve.
		float mrfMaxScore = 0.0f;
		for (Clique c : cliques) {
			mrfMaxScore += c.getMaxScore();
		}

		// Sort cliques according to their max scores.
		Collections.sort(cliques, mMaxScoreComparator);

		// Score that must be achieved to enter result set.
		double scoreThreshold = Double.NEGATIVE_INFINITY;

		// Offset into document set we're currently at (if applicable).
		int docsetOffset = 0;
		
		int docno = 0;
		if (mDocSet != null) {
			docno = docsetOffset < mDocSet.length ? mDocSet[docsetOffset++] : Integer.MAX_VALUE;
		} else {
			docno = mMRF.getNextCandidate();
		}

		while (docno < Integer.MAX_VALUE) {
			float score = 0.0f;

			for (DocumentNode documentNode : mDocNodes) {
				documentNode.setDocno(docno);
			}

			// Document-at-a-time scoring.
			float docMaxScore = mrfMaxScore;
			boolean skipped = false;
			for (int i = 0; i < cliques.size(); i++) {
				// Current clique that we're scoring.
				Clique c = cliques.get(i);

				// If there's no way that this document can enter the result set
				// then exit.
				if (score + docMaxScore <= scoreThreshold) {
					// Advance postings readers (but don't score).
					for (int j = i; j < cliques.size(); j++) {
						cliques.get(j).setNextCandidate(docno + 1);
					}
					skipped = true;
					break;
				}

				// Document independent cliques do not affect the ranking.
				if (!c.isDocDependent()) {
					continue;
				}

				// Update document score.
				score += c.getWeight() * c.getPotential();

				// Update the max score for the rest of the cliques.
				docMaxScore -= c.getMaxScore();
			}

			// Keep track of mNumResults best accumulators.
			if (!skipped && score > scoreThreshold) {
				a.docno = docno;
				a.score = score;
				mSortedAccumulators.add(a);

				if (mSortedAccumulators.size() == mNumResults + 1) {
					a = mSortedAccumulators.poll();
					scoreThreshold = mSortedAccumulators.peek().score;
				} else {
					a = mAccumulators[mSortedAccumulators.size()];
				}
			}

			if (mDocSet != null) {
				docno = docsetOffset < mDocSet.length ? mDocSet[docsetOffset++] : Integer.MAX_VALUE;
			} else {
				docno = mMRF.getNextCandidate();
			}
		}

		// Grab the accumulators off the stack, in (reverse) order.
		Accumulator[] results = new Accumulator[Math.min(mNumResults, mSortedAccumulators.size())];
		for (int i = 0; i < results.length; i++) {
			results[results.length - 1 - i] = mSortedAccumulators.poll();
		}

		return results;
	}

	/**
	 * Returns the Markov Random Field associated with this ranker.
	 */
	public MarkovRandomField getMRF() {
		return mMRF;
	}

	/**
	 * Sets the number of results to return.
	 */
	public void setNumResults(int numResults) {
		mNumResults = numResults;
	}

	private List<DocumentNode> getDocNodes() {
		ArrayList<DocumentNode> docNodes = new ArrayList<DocumentNode>();

		// Check which of the nodes are DocumentNodes.
		List<GraphNode> nodes = mMRF.getNodes();
		for (GraphNode node : nodes) {
			if (node.getType() == GraphNode.Type.DOCUMENT) {
				docNodes.add((DocumentNode) node);
			}
		}
		return docNodes;
	}
}
