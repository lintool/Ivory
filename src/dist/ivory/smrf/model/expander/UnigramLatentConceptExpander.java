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
import ivory.data.PostingsList;
import ivory.data.PostingsReader;
import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.VocabFrequencyPair;
import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.builder.TermExpressionGenerator;
import ivory.smrf.model.potential.PotentialFunction;
import ivory.smrf.model.potential.QueryPotential;
import ivory.smrf.model.score.ScoringFunction;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.AccumulatorDocnoComparator;
import ivory.smrf.model.builder.FeatureBasedMRFBuilder;
import ivory.smrf.model.MetaFeature;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;
import java.util.Iterator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.w3c.dom.Node;

/**
 * @author metzler
 * @author Lidan Wang
 */
public class UnigramLatentConceptExpander extends MRFExpander {

	private List<Parameter> mParameters = null;
	private List<Node> mScoringFunctionNodes = null;

	public UnigramLatentConceptExpander(RetrievalEnvironment env, int fbDocs, int fbTerms, List<Parameter> params,
			List<Node> scoringFunctionNodes) {
		mEnv = env;
		mFbDocs = fbDocs;
		mFbTerms = fbTerms;
		mParameters = params;
		mScoringFunctionNodes = scoringFunctionNodes;
	}

	/* (non-Javadoc)
	 * @see ivory.smrf.model.expander.MRFExpander#getExpandedMRF(ivory.smrf.model.MarkovRandomField, ivory.smrf.retrieval.Accumulator[])
	 */
	@Override
	public MarkovRandomField getExpandedMRF(MarkovRandomField mrf, Accumulator[] results) throws Exception {
		// begin constructing the expanded MRF
		MarkovRandomField expandedMRF = new MarkovRandomField(mrf.getQueryTerms(), mEnv);

		// add cliques corresponding to original MRF
		List<Clique> cliques = mrf.getCliques();
		for (Clique clique : cliques) {
			expandedMRF.addClique(clique);
		}

		// get MRF global evidence
		GlobalEvidence globalEvidence = mrf.getGlobalEvidence();

		// gather Accumulators we're actually going to use for feedback purposes
		Accumulator [] fbResults = new Accumulator[Math.min(results.length, mFbDocs)];
		for(int i = 0; i < Math.min( results.length, mFbDocs ); i++ ) {
			fbResults[i] = results[i];
		}

		// sort the Accumulators by docid
		Arrays.sort(fbResults, new AccumulatorDocnoComparator());

		// get docids that correspond to the accumulators
		int [] docSet = Accumulator.accumulatorsToDocnos(fbResults);

		// get document vectors for results
		IntDocVector [] docVecs = null;
		docVecs = mEnv.documentVectors(docSet);


		// extract tf and doclen information from document vectors
		TFDoclenStatistics stats = getTFDoclenStatistics(docVecs);

		
		VocabFrequencyPair [] vocab = stats.getVocab();
		Map<String,Short> [] tfs = stats.getTfs();
		int [] doclens = stats.getDoclens();

		// priority queue for the concepts associated with this builder
		PriorityQueue<Accumulator> sortedConcepts = new PriorityQueue<Accumulator>();

		// create scoring functions
		ScoringFunction [] scoringFunctions = new ScoringFunction[mScoringFunctionNodes.size()];
		for(int i = 0; i < mScoringFunctionNodes.size(); i++) {
			Node functionNode = mScoringFunctionNodes.get(i);
			String functionType = XMLTools.getAttributeValue(functionNode, "scoreFunction", null);
			if(functionType == null) {
				throw new Exception("conceptscore node must specify a scorefunction attribute!");
			}
			scoringFunctions[i] = ScoringFunction.create(functionType, functionNode);
		}
		
		// score each concept
		for(int conceptID = 0; conceptID < vocab.length; conceptID++) {
			// only consider _maxCandidates
			if( mMaxCandidates > 0 && conceptID >= mMaxCandidates ) {
				break;
			}

			// the current concept
			String concept = vocab[conceptID].getKey();

			// get df and cf information for the concept


			PostingsReader reader = mEnv.getPostingsReader(concept);
			PostingsList list = reader.getPostingsList();
			int df = list.getDf();
			long cf = list.getCf();
			mEnv.clearPostingsReaderCache();
			

			// construct concept evidence
			GlobalTermEvidence termEvidence = new GlobalTermEvidence(df, cf);
			
			// score the concept
			double score = 0.0;
			for(int i = 0; i < fbResults.length; i++) { 
				double docScore = 0.0;
				for(int j = 0; j < scoringFunctions.length; j++) {
					double weight = mParameters.get(j).weight;
					ScoringFunction fn = scoringFunctions[j];
					fn.initialize(termEvidence, globalEvidence);
					
					Short tf = tfs[i].get(vocab[conceptID].getKey());
					if(tf == null) {
						tf = 0;
					}
					double s = fn.getScore(tf, doclens[i]);
					
					docScore += weight * s;
				}
				score += Math.exp(fbResults[i].score + docScore);
			}

			int size = sortedConcepts.size();
			if(size < mFbTerms || sortedConcepts.peek().score < score) {
				if(size == mFbTerms) {
					sortedConcepts.poll(); // remove worst concept
				}
				sortedConcepts.add(new Accumulator(conceptID, score));
			}

		}

		
		// compute the weights of the expanded terms
		int numTerms = Math.min(mFbTerms, sortedConcepts.size());
		double totalWt = 0.0;
		Accumulator [] bestConcepts = new Accumulator[numTerms];
		for(int i = 0; i < numTerms; i++) {
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
		for(int i = 0; i < numTerms; i++) {
			// get 'accumulator' (concept id/score pair)
			Accumulator a = bestConcepts[i];			

			// construct the MRF corresponding to this concept
			String concept = vocab[a.docno].getKey();

			for(int j = 0; j < mScoringFunctionNodes.size(); j++) {
				Node functionNode = mScoringFunctionNodes.get(j);
				String functionType = XMLTools.getAttributeValue(functionNode, "scoreFunction", null);
				ScoringFunction fn = ScoringFunction.create(functionType, functionNode);

				Parameter parameter = mParameters.get(j);
				
				List<GraphNode> cliqueNodes = new ArrayList<GraphNode>();
				cliqueNodes.add(docNode);

				TermNode termNode = new TermNode(concept);
				cliqueNodes.add(termNode);

				PotentialFunction potential = new QueryPotential(mEnv, generator, fn);

				Clique c = new Clique(cliqueNodes, potential, parameter);
				c.setCliqueType("Term");
				c.setScaleFactor(a.score / totalWt);


				// need clique terms in order to compute query-dependent weight
				String cliqueTerms = "";
 
				List<ivory.smrf.model.GraphNode> mNodes = c.getNodes();
 
				//get clique terms
				for( Iterator<ivory.smrf.model.GraphNode> nodeIter = mNodes.iterator(); nodeIter.hasNext(); ) {
                                                                
					try{
						TermNode tnode = (TermNode) (nodeIter.next());
                                                                        
						String term = tnode.getTerm();
 
						cliqueTerms += term+" ";

						//System.out.println("In UnigramLatentConceptExpander, expansion term is "+term+" ");
					}
					catch(Exception e){}
				}
                                                        
				cliqueTerms = cliqueTerms.trim();

				if (cliqueTerms == ""){
					throw new Exception ("Exception: Invalid clique terms.");
				}
				c.setCliqueTerms (cliqueTerms);
                                                        
				String modelType = FeatureBasedMRFBuilder.getModelType();

				if (modelType.equals("WSD")){

					LinkedList<MetaFeature> metaFeatureSet = FeatureBasedMRFBuilder.getMetaFeatureList();
					String currentCollection = FeatureBasedMRFBuilder.getCurrentCollection();

					//compute query-dependent clique weight
					double wgt = 0;
					for (int w = 0; w < metaFeatureSet.size(); w++){
						MetaFeature mf = (MetaFeature) (metaFeatureSet.get(w));

						//skip using clue external feature for clue data
						if (mf.getName().indexOf("clue")!=-1 && currentCollection.indexOf("clue")!=-1){}
						else{
							double metaWeight = mf.getWeight();
							double cliqueFeatureVal = FeatureBasedMRFBuilder.computeFeatureVal (c, mf);
							//System.out.println("Meta wgt and met value "+metaWeight+" "+cliqueFeatureVal);

							wgt += metaWeight * cliqueFeatureVal;
						}
					}

					System.out.println("In UnigramLatentConceptExpander: Clique terms: "+cliqueTerms+", Wgts is "+wgt);

					c.setCliqueWeight (wgt);
				}

			
				expandedMRF.addClique(c);
			}

			System.out.println( "*\t" + vocab[a.docno] + "\t" + (a.score / totalWt) );
		}
		
		return expandedMRF;
	}
}
