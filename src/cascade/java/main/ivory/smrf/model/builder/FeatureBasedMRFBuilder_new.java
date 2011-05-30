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

package ivory.smrf.model.builder;

import ivory.exception.ConfigurationException;
import ivory.exception.RetrievalException;
import ivory.smrf.model.Clique_cascade;
import ivory.smrf.model.builder.CliqueSet_cascade;
import ivory.smrf.model.Clique;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.Map;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.lang.Math;
import java.lang.Double;
import java.lang.Integer;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author Lidan Wang
 */
public class FeatureBasedMRFBuilder_new extends FeatureBasedMRFBuilder {

	HashMap sanityCheck = new HashMap();

	float weightScale = -1;
	float pruningThresholdBigram = 0.0f;

	public FeatureBasedMRFBuilder_new(RetrievalEnvironment env, Node model) {
		super (env, model);
		weightScale = XMLTools.getAttributeValue(model, "weightScale", -1.0f);
		pruningThresholdBigram= XMLTools.getAttributeValue(mModel, "pruningThresholdBigram", 0.0f);
	}

	@Override
	public MarkovRandomField buildMRF(String[] queryTerms) throws ConfigurationException {

		// This is the MRF we're building.
		MarkovRandomField mrf = new MarkovRandomField(queryTerms, env);

		// Construct MRF feature by feature.
		NodeList children = super.getModel().getChildNodes(); 

		// Sum of query-dependent importance weights.
		float totalImportance = 0.0f;

		// Cliques that have query-dependent importance weights.
		Set<Clique_cascade> cliquesWithImportance = new HashSet<Clique_cascade>();

		//For cascade model, # docs from last stage
		//int numResultsMin = 99999999;

		int cascade_stage = 0;
		int cascade_stage_proper = -1;

		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);

			if ("feature".equals(child.getNodeName())) {
				// Get the feature id.
				String featureID = XMLTools.getAttributeValue(child, "id", "");
				if (featureID.equals("")) {
					throw new RetrievalException("Each feature must specify an id attribute!");
				}

				// Get feature weight (default = 1.0).
				float weight = XMLTools.getAttributeValue(child, "weight", 1.0f);

				// Concept importance model (optional).
				ConceptImportanceModel importanceModel = null;

				// Get concept importance source (if applicable).
				String importanceSource = XMLTools.getAttributeValue(child, "importance", "");
				if (!importanceSource.equals("")) {
					importanceModel = env.getImportanceModel(importanceSource);
					if (importanceModel == null) {
						throw new RetrievalException("ImportanceModel " + importanceSource + " not found!");
					}
				}

				// Get CliqueSet type.
				String cliqueSetType = XMLTools.getAttributeValue(child, "cliqueSet", "");

				// Get Cascade stage (if any)
				int cascadeStage = XMLTools.getAttributeValue(child, "cascadeStage", -1);

				//int numResults = XMLTools.getAttributeValue(child, "hits", -1);

				String pruner_and_params = XMLTools.getAttributeValue(child, "prune", "null");

				String thePruner = (pruner_and_params.trim().split("\\s+"))[0];

				String conceptBinType = XMLTools.getAttributeValue(child, "conceptBinType", "");

				String conceptBinParams = XMLTools.getAttributeValue(child, "conceptBinParams", "");

				String scoreFunction = XMLTools.getAttributeValue(child, "scoreFunction", null);
				
				int width = XMLTools.getAttributeValue(child, "width", -1);

				if (cascadeStage != -1){
					RetrievalEnvironment.setIsNew(true);

					if (!importanceSource.equals("")) {
						//System.out.println("The boosting alg should learn the weight, shouldn't use WSD weights!");
						//System.exit(-1);
					}
				}
				else{
					RetrievalEnvironment.setIsNew(false);
				}

			
				if (cascadeStage != -1){
					if (!conceptBinType.equals("") || !conceptBinParams.equals("")){
						if (conceptBinType.equals("") || conceptBinParams.equals("")){
							throw new RetrievalException("Most specify conceptBinType || conceptBinParams");
						}
						importanceModel = env.getImportanceModel("wsd");

						if (importanceModel == null) {
        	                                        throw new RetrievalException("ImportanceModel " + importanceSource + " not found!");
	                                        }
					}
				}

				cascade_stage_proper = cascadeStage;

				if (cascadeStage !=-1 && conceptBinType.equals("") &&  conceptBinParams.equals("")){
					cascade_stage_proper = cascade_stage;
				}

				// Construct the clique set.
				CliqueSet_cascade cliqueSet = (CliqueSet_cascade) (CliqueSet_cascade.create(cliqueSetType, env, queryTerms, child, cascade_stage_proper, pruner_and_params));//, approxProximity);

				// Get cliques from clique set.
				List<Clique> cliques = cliqueSet.getCliques();


				if (cascadeStage !=-1 && conceptBinType.equals("") &&  conceptBinParams.equals("")){
                                        if (cliques.size() > 0){
                                                cascade_stage++;
                                        }
                                }
				else if (cascadeStage != -1 && !conceptBinType.equals("") && !conceptBinParams.equals("")){
					if (cliques.size()>0){
						int [] order = new int[cliques.size()];
						double [] conceptWeights = new double[cliques.size()];
						int cntr = 0;
						String all_concepts = "";
						for (Clique c : cliques) {
							float importance = importanceModel.getCliqueWeight(c);
							order[cntr] = cntr;
							conceptWeights[cntr] = importance;
							cntr++;
							all_concepts += c.getConcept()+" ";
						}
						ivory.smrf.model.constrained.ConstraintModel.Quicksort(conceptWeights, order, 0, order.length-1);

						int [] keptCliques = getCascadeCliques ( conceptBinType, conceptBinParams, conceptWeights, order, all_concepts, featureID, thePruner, width+"", scoreFunction);

						List<Clique> cliques2 = new ArrayList();
						for (int k=0; k<keptCliques.length; k++){
							int index = keptCliques[k];
							cliques2.add(cliques.get(index));
						}
						cliques = new ArrayList();
						for (int k=0; k<cliques2.size(); k++){
							cliques.add(cliques2.get(k));
						}

						if (keptCliques.length!=0){
                	                                for (Clique c : cliques) {
                        	                                ((Clique_cascade)c).setCascadeStage(cascade_stage);
                                	                }
                                        	        cascade_stage++;
                                        	}
					}
				}	

				for (Clique c : cliques) {

					double w = weight;

					c.setParameterName(featureID); 	 // Parameter id.
					c.setParameterWeight(weight);    // Weight.
					c.setType(cliqueSet.getType());  // Clique type.

					// Get clique weight.
					if (!importanceSource.equals("")) {

						float importance = importanceModel.getCliqueWeight(c);						
	
						if (weightScale != -1.0f){ //if weightScale == -1, then importance and weight will be multiplied without weighting
							if (weight!=-1.0f){ 
								c.setParameterWeight((float) (Math.pow(weight, weightScale)));
								importance = (float)(Math.pow(importance, (1.0f-weightScale)));
							}
						}

						if (weight == -1.0f){ //default value. 
							c.setParameterWeight(1.0f);
						}

						c.setImportance(importance);

						totalImportance += importance;
						cliquesWithImportance.add((Clique_cascade)c);						

						w = importance;
					}

					if (w < pruningThresholdBigram && c.getType()!=Clique.Type.Term){
                                                //System.out.println("Not add "+c);
                                        }
                                        else{
                                                // Add clique to MRF.
                                                mrf.addClique(c);
                                                //System.out.println("Add "+c);
                                        }
				}
			}
		}


		// Normalize query-dependent feature importance values.
		if (mNormalizeImportance) {
			for (Clique c : cliquesWithImportance) {
				c.setImportance(c.getImportance() / totalImportance);
			}
		}

		return mrf;
	}

	public int [] getCascadeCliques ( String conceptBinType, String conceptBinParams, double [] conceptWeights, int [] order, String all_concepts, String featureID, String thePruner, String width, String scoreFunction) throws ConfigurationException {

		if (conceptBinType.equals("default") || conceptBinType.equals("impact")){
						
			//[0]: # bins; [1]: which bin for this feature
			String [] tokens = conceptBinParams.split("\\s+");

			if (tokens.length!=2){
				throw new RetrievalException("For impact binning, should specify # bins(as a fraction of # total cliques) and which bin for this feature");
			}

			//K
			double numBins = Math.floor(Double.parseDouble(tokens[0]));

			//1-indexed!!!!
			int whichBin = Integer.parseInt(tokens[1]);


			if (sanityCheck.containsKey(conceptBinType+" "+numBins+" "+whichBin+" "+all_concepts +" "+featureID+" "+thePruner+" "+width+" "+scoreFunction)){
				throw new RetrievalException("Bin "+whichBin+" has been used by this concept type before "+conceptBinType+" "+numBins+" "+all_concepts +" "+featureID+" "+thePruner+" "+width+" "+scoreFunction);
			}
			else{
				sanityCheck.put(conceptBinType+" "+numBins+" "+whichBin+" "+all_concepts +" "+featureID+" "+thePruner+" "+width+" "+scoreFunction, "1");
			}

			if (conceptBinType.equals("default")){				
				//concept importance in descending order
				int [] order_descending = new int[order.length];
				for (int i=0; i<order_descending.length; i++){
					order_descending[i] = order[order.length-i-1];
				}

				int [] cascadeCliques = null;

				//if there are 5 bigram concepts, if there are 3 bins, the last bin will take concepts 3, 4, 5
				if (numBins == whichBin && order_descending.length > numBins){
					cascadeCliques = new int[order_descending.length - (int)numBins +1];
					for (int j=whichBin-1; j<order_descending.length; j++){ //0-indexed
						cascadeCliques[j-whichBin+1] = order_descending[j];
					}
				}
				else{
					cascadeCliques = new int[1];

					if ((whichBin-1) < order_descending.length){
						cascadeCliques[0] = order_descending[whichBin - 1];	
					}
					else{
						return new int[0];
					}
				}

				//sort by clique numbers
				double [] cascadeCliques_sorted_by_clique_number = new double[cascadeCliques.length];
				int [] order1 = new int[cascadeCliques.length];
				for (int j=0; j<order1.length; j++){
					order1[j] = j;
					cascadeCliques_sorted_by_clique_number[j] = cascadeCliques[j];
				}
				ivory.smrf.model.constrained.ConstraintModel.Quicksort(cascadeCliques_sorted_by_clique_number, order1, 0, order1.length-1);

				for (int j=0; j<cascadeCliques_sorted_by_clique_number.length; j++){
					cascadeCliques[j] = (int) cascadeCliques_sorted_by_clique_number[j];
				}
				return cascadeCliques;
			}

			else if (conceptBinType.equals("impact")){

				double totalCliques = (double)(conceptWeights.length);
				double base = Math.pow((totalCliques + 1), (1/numBins));
		
				double firstBinSize = base - 1;
				if (firstBinSize < 1){
					firstBinSize = 1;
				}

                        	int start = 0;
				int end = (int)(Math.round(firstBinSize));
				double residual = firstBinSize - end;

				for (int i=2; i<=whichBin; i++){
					start = end;
					double v = firstBinSize * Math.pow(base, (i - 1));
					double v_plus_residual = v + residual;
					double v_round = Math.round(v_plus_residual);
					residual = v_plus_residual - v_round;
					end += (int) v_round;			
				}

				if (start >= totalCliques){
					return new int[0];
				}
	
				if (end > totalCliques){
					end = (int) totalCliques;
				}
                        
				int [] cascadeCliques = new int[end-start];

				//concept importance in descending order
				int [] order_descending = new int[order.length];
				for (int i=0; i<order_descending.length; i++){
					order_descending[i] = order[order.length-i-1];
				}

				for (int i=start; i<end; i++){
					cascadeCliques[i-start] = order_descending[i];
				}

				//sort by clique numbers
                                double [] cascadeCliques_sorted_by_clique_number = new double[cascadeCliques.length];
                                int [] order1 = new int[cascadeCliques.length];
                                for (int j=0; j<order1.length; j++){
					cascadeCliques_sorted_by_clique_number[j] = cascadeCliques[j];
                                        order1[j] = j;
                                }
                                ivory.smrf.model.constrained.ConstraintModel.Quicksort(cascadeCliques_sorted_by_clique_number, order1, 0, order1.length-1);
                                
				for (int j=0; j<cascadeCliques_sorted_by_clique_number.length; j++){
                                        cascadeCliques[j] = (int) cascadeCliques_sorted_by_clique_number[j];
                                }
                                return cascadeCliques;
			}
		}
		else{
			throw new RetrievalException("Not yet supported "+conceptBinType);
		}


		return null;
	}

}
