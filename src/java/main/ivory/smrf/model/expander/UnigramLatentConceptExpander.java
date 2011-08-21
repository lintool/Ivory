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

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.IntDocVector;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.exception.ConfigurationException;
import ivory.core.exception.RetrievalException;
import ivory.core.util.XMLTools;
import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.VocabFrequencyPair;
import ivory.smrf.model.builder.Expression;
import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.builder.TermExpressionGenerator;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.smrf.model.potential.PotentialFunction;
import ivory.smrf.model.potential.QueryPotential;
import ivory.smrf.model.score.ScoringFunction;
import ivory.smrf.retrieval.Accumulator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.w3c.dom.Node;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public class UnigramLatentConceptExpander extends MRFExpander {

	private List<Parameter> parameters = null;
	private List<Node> scoringFunctionNodes = null;
	private List<ConceptImportanceModel> importanceModels = null;
	
	private final GlobalTermEvidence termEvidence = new GlobalTermEvidence();
	
	public UnigramLatentConceptExpander(RetrievalEnvironment env, int fbDocs, int fbTerms, float expanderWeight,
			List<Parameter> params, List<Node> scoringFunctionNodes, List<ConceptImportanceModel> importanceModels) {
		this.env = Preconditions.checkNotNull(env);
		this.numFeedbackDocs = Preconditions.checkNotNull(fbDocs);
		this.numFeedbackTerms = Preconditions.checkNotNull(fbTerms);
		this.expanderWeight = Preconditions.checkNotNull(expanderWeight);
		this.parameters = Preconditions.checkNotNull(params);
		this.scoringFunctionNodes = Preconditions.checkNotNull(scoringFunctionNodes);
		this.importanceModels = importanceModels;
	}

	@Override
	public MarkovRandomField getExpandedMRF(MarkovRandomField mrf, Accumulator[] results)
			throws ConfigurationException {
		Preconditions.checkNotNull(mrf);
		Preconditions.checkNotNull(results);

		// begin constructing the expanded MRF
		MarkovRandomField expandedMRF = new MarkovRandomField(mrf.getQueryTerms(), env);

		// add cliques corresponding to original MRF
		List<Clique> cliques = mrf.getCliques();
		for (Clique clique : cliques) {
			expandedMRF.addClique(clique);
		}

		// get MRF global evidence
		GlobalEvidence globalEvidence = mrf.getGlobalEvidence();

		// gather Accumulators we're actually going to use for feedback purposes
		Accumulator[] fbResults = new Accumulator[Math.min(results.length, numFeedbackDocs)];
		for (int i = 0; i < Math.min(results.length, numFeedbackDocs); i++) {
			fbResults[i] = results[i];
		}

		// sort the Accumulators by docid
		Arrays.sort(fbResults, new Accumulator.DocnoComparator());

		// get docids that correspond to the accumulators
		int[] docSet = Accumulator.accumulatorsToDocnos(fbResults);

		// get document vectors for results
		IntDocVector[] docVecs = env.documentVectors(docSet);

		// extract tf and doclen information from document vectors
		TFDoclenStatistics stats = null;
		try {
			stats = getTFDoclenStatistics(docVecs);
		} catch (IOException e) {
			throw new RetrievalException(
					"Error: Unable to extract tf and doclen information from document vectors!");
		}

		VocabFrequencyPair[] vocab = stats.getVocab();
		Map<String, Short>[] tfs = stats.getTfs();
		int[] doclens = stats.getDoclens();

		// priority queue for the concepts associated with this builder
		PriorityQueue<Accumulator> sortedConcepts = new PriorityQueue<Accumulator>();

		// create scoring functions
		ScoringFunction[] scoringFunctions = new ScoringFunction[scoringFunctionNodes.size()];
		for (int i = 0; i < scoringFunctionNodes.size(); i++) {
			Node functionNode = scoringFunctionNodes.get(i);
			String functionType = XMLTools.getAttributeValueOrThrowException(functionNode, "scoreFunction",
						"conceptscore node must specify a scorefunction attribute!");
			scoringFunctions[i] = ScoringFunction.create(functionType, functionNode);
		}

		// score each concept
		for (int conceptID = 0; conceptID < vocab.length; conceptID++) {
			// only consider maxCandidates
			if (maxCandidates > 0 && conceptID >= maxCandidates) {
				break;
			}

			// the current concept
			String concept = vocab[conceptID].getKey();

			// get df and cf information for the concept
			PostingsReader reader = env.getPostingsReader(new Expression(concept));
			if (reader == null) {
				continue;
			}
			PostingsList list = reader.getPostingsList();
			int df = list.getDf();
			long cf = list.getCf();
			env.clearPostingsReaderCache();

			// construct concept evidence
			termEvidence.set(df, cf);

			// score the concept
			float score = 0.0f;
			for (int i = 0; i < fbResults.length; i++) {
				float docScore = 0.0f;
				for (int j = 0; j < scoringFunctions.length; j++) {
					float weight = parameters.get(j).getWeight();
					ConceptImportanceModel importanceModel = importanceModels.get(j);
          if (importanceModel != null) {
            weight *= importanceModel.getConceptWeight(concept);
          }
					ScoringFunction fn = scoringFunctions[j];
					fn.initialize(termEvidence, globalEvidence);

					Short tf = tfs[i].get(vocab[conceptID].getKey());
					if (tf == null) {
						tf = 0;
					}
					float s = fn.getScore(tf, doclens[i]);

					docScore += weight*s;
				}
				score += Math.exp(fbResults[i].score + docScore);
			}

			int size = sortedConcepts.size();
			if (size < numFeedbackTerms || sortedConcepts.peek().score < score) {
				if (size == numFeedbackTerms) {
					sortedConcepts.poll(); // remove worst concept
				}
				sortedConcepts.add(new Accumulator(conceptID, score));
			}
		}

		// compute the weights of the expanded terms
		int numTerms = Math.min(numFeedbackTerms, sortedConcepts.size());
		float totalWt = 0.0f;
		Accumulator[] bestConcepts = new Accumulator[numTerms];
		for (int i = 0; i < numTerms; i++) {
			// get 'accumulator' (concept id/score pair)
			Accumulator a = sortedConcepts.poll();
			bestConcepts[i] = a;
			totalWt += a.score;
		}

		// document node (shared across all expansion cliques)
		DocumentNode docNode = new DocumentNode();

		// expression generator (shared across all expansion cliques)
		ExpressionGenerator generator = new TermExpressionGenerator();

		// add cliques corresponding to best expansion concepts
		for (int i = 0; i < numTerms; i++) {
			// get 'accumulator' (concept id/score pair)
			Accumulator a = bestConcepts[i];

			// construct the MRF corresponding to this concept
			String concept = vocab[a.docno].getKey();

			for (int j = 0; j < scoringFunctionNodes.size(); j++) {
				Node functionNode = scoringFunctionNodes.get(j);
				String functionType = XMLTools.getAttributeValue(functionNode, "scoreFunction", null);
				ScoringFunction fn = ScoringFunction.create(functionType, functionNode);

				Parameter parameter = parameters.get(j);
				ConceptImportanceModel importanceModel = importanceModels.get(j);

				List<GraphNode> cliqueNodes = Lists.newArrayList();
				cliqueNodes.add(docNode);

				TermNode termNode = new TermNode(concept);
				cliqueNodes.add(termNode);

				PotentialFunction potential = new QueryPotential(env, generator, fn);

				Clique c = new Clique(cliqueNodes, potential, parameter);
				c.setType(Clique.Type.Term);

				// scale importances by lce likelihood
				float normalizedScore = expanderWeight * (a.score / totalWt);
        if (importanceModel != null) {
          c.setImportance(normalizedScore * importanceModel.getCliqueWeight(c));
        } else {
          c.setImportance(normalizedScore);
        }

				expandedMRF.addClique(c);
			}
		}

		return expandedMRF;
	}
}
