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

package ivory.smrf.model.expander;

import ivory.data.IntDocVector;
import ivory.exception.ConfigurationException;
import ivory.exception.RetrievalException;
import ivory.smrf.model.Clique;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.VocabFrequencyPair;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.MRFDocumentRanker;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 * 
 */
public class NGramLatentConceptExpander extends MRFExpander {

	/**
	 * gram sizes associated with each expansionmodel
	 */
	private List<Integer> mGramSizes = null;

	/**
	 * number of documents to expand with
	 */
	private List<Integer> mFbDocList = null;

	/**
	 * number of concepts to expand with
	 */
	private List<Integer> mFbTermList = null;

	/**
	 * builders used to build MRFs from expansion concepts
	 */
	private List<MRFBuilder> mBuilders = null;

	/**
	 * @param env
	 * @param gramList
	 * @param builderList
	 * @param fbDocsList
	 * @param fbTermsList
	 */
	public NGramLatentConceptExpander(RetrievalEnvironment env, List<Integer> gramList,
			List<MRFBuilder> builderList, List<Integer> fbDocsList, List<Integer> fbTermsList) {
		mEnv = Preconditions.checkNotNull(env);
		mGramSizes = Preconditions.checkNotNull(gramList);
		mBuilders = Preconditions.checkNotNull(builderList);
		mFbDocList = Preconditions.checkNotNull(fbDocsList);
		mFbTermList = Preconditions.checkNotNull(fbTermsList);
	}

	@Override
	public MarkovRandomField getExpandedMRF(MarkovRandomField mrf, Accumulator[] results)
			throws ConfigurationException {
		// begin constructing the expanded MRF
		MarkovRandomField expandedMRF = new MarkovRandomField(mrf.getQueryTerms(), mEnv);

		// add cliques corresponding to original MRF
		List<Clique> cliques = mrf.getCliques();
		for (Clique clique : cliques) {
			expandedMRF.addClique(clique);
		}

		// find the best concepts for each of the expansion models
		for (int modelNum = 0; modelNum < mBuilders.size(); modelNum++) {
			// get the information about this expansion model
			int curGramSize = mGramSizes.get(modelNum);
			MRFBuilder curBuilder = mBuilders.get(modelNum);
			int curFbDocs = mFbDocList.get(modelNum);
			int curFbTerms = mFbTermList.get(modelNum);

			// gather Accumulators we're actually going to use for feedback
			// purposes
			Accumulator[] fbResults = new Accumulator[Math.min(results.length, curFbDocs)];
			for (int i = 0; i < Math.min(results.length, curFbDocs); i++) {
				fbResults[i] = results[i];
			}

			// sort the Accumulators by docid
			Arrays.sort(fbResults, new Accumulator.DocnoComparator());

			// get docids that correspond to the accumulators
			int[] docSet = Accumulator.accumulatorsToDocnos(fbResults);

			// get document vectors for results
			IntDocVector[] docVecs = mEnv.documentVectors(docSet);

			// extract vocabulary from results
			VocabFrequencyPair[] vocab = null;
			try {
				vocab = getVocabulary(docVecs, curGramSize);
			} catch (IOException e) {
				throw new RuntimeException("Error: Unable to fetch the vocabulary!");
			}

			// priority queue for the concepts associated with this builder
			PriorityQueue<Accumulator> sortedConcepts = new PriorityQueue<Accumulator>();

			// score each concept
			for (int conceptID = 0; conceptID < vocab.length; conceptID++) {
				// only consider _maxCandidates
				if (mMaxCandidates > 0 && conceptID >= mMaxCandidates) {
					break;
				}

				// the current concept
				String concept = vocab[conceptID].getKey();

				String[] concepts = concept.split(" ");
				MarkovRandomField conceptMRF = curBuilder.buildMRF(concepts);

				MRFDocumentRanker ranker = new MRFDocumentRanker(conceptMRF, docSet, docSet.length);
				Accumulator[] conceptResults = ranker.rank();
				Arrays.sort(conceptResults, new Accumulator.DocnoComparator());

				float score = 0.0f;
				for (int i = 0; i < conceptResults.length; i++) {
					if (fbResults[i].docno != conceptResults[i].docno) {
						throw new RetrievalException("Error: Mismatch occured in getExpandedMRF!");
					}
					score += Math.exp(fbResults[i].score + conceptResults[i].score);
				}

				int size = sortedConcepts.size();
				if (size < curFbTerms || sortedConcepts.peek().score < score) {
					if (size == curFbTerms) {
						sortedConcepts.poll(); // remove worst concept
					}
					sortedConcepts.add(new Accumulator(conceptID, score));
				}
				// System.out.println( vocab[ conceptID ].getKey() + "\t" +
				// vocab[ conceptID ].getValue() + "\t" + score );
			}

			// compute the weights of the expanded terms
			int numTerms = Math.min(curFbTerms, sortedConcepts.size());
			float totalWt = 0.0f;
			Accumulator[] bestConcepts = new Accumulator[numTerms];
			for (int i = 0; i < numTerms; i++) {
				// get 'accumulator' (concept id/score pair)
				Accumulator a = sortedConcepts.poll();
				bestConcepts[i] = a;
				totalWt += a.score;
			}

			// add cliques corresponding to best expansion concepts
			for (int i = 0; i < numTerms; i++) {
				// get 'accumulator' (concept id/score pair)
				Accumulator a = bestConcepts[i];

				// construct the MRF corresponding to this concept
				String[] concepts = vocab[a.docno].getKey().split(" ");
				MarkovRandomField conceptMRF = curBuilder.buildMRF(concepts);

				// normalized score
				float normalizedScore = a.score / totalWt;
				
				// add cliques
				cliques = conceptMRF.getCliques();
				for (Clique c : cliques) {
					if (c.isDocDependent() && c.getWeight() != 0.0) {
						c.setImportance(normalizedScore * c.getImportance());
						expandedMRF.addClique(c);
					}
				}

				// System.out.println( "*\t" + vocab[a.docno] + "\t" + (a.score
				// / totalWt) );
			}
		}

		return expandedMRF;
	}
}
