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
import ivory.smrf.model.potential.PotentialFunction;
import ivory.smrf.model.potential.CascadeQueryPotential;

import java.util.Comparator;
import java.util.List;
import java.lang.Double;

import com.google.common.base.Preconditions;

/**
 * @author Lidan Wang
 */
public class CascadeClique extends Clique {

	// Cascade stage
	private int mCascadeStage;
	
	String mPruner = "";
	float mPruner_param = -1;

	//For estimating cascade cost
        static float term_unit_cost = 1; 
        static float ordered_unit_cost = 20;
        static float unordered_unit_cost = 20;
   
	public float mUnitCost;

	private String [] mSingleTerms;

	/**
	 * @param nodes
	 * @param f
	 */
	public CascadeClique(List<GraphNode> nodes, PotentialFunction f, Parameter weight, int cascadeStage, String pruner_and_params) {
		this(nodes, f, weight, 1.0f, null, true, cascadeStage, pruner_and_params);
	}

	public CascadeClique(List<GraphNode> nodes, PotentialFunction f, Parameter param, float importance, Type type, boolean docDependent, int cascadeStage, String pruner_and_params) {


		super (nodes, f, param, importance, type, docDependent);

		String concept = getConcept();
		String [] t = concept.trim().toLowerCase().split("\\s+");
		mSingleTerms = new String[t.length];
		for (int i=0; i<t.length; i++){
			mSingleTerms[i] = t[i];
		}

		mCascadeStage = cascadeStage;

		if (pruner_and_params.indexOf("null")==-1){
			String [] tokens = pruner_and_params.trim().split("\\s+");
			mPruner = tokens[0];
			mPruner_param = (float)(Double.parseDouble(tokens[1]));	
		}
	}

	//If it's a term, then return positions at the current document
	//not supported if it's term proximity feature!
	public int[] getPositions(){
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential)potential).getPositions();
	}

	public int getDocLen(){
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential)potential).getDocLen();
	}
	
	//reset postings readers
	public void resetPostingsListReader(){
		PotentialFunction potential = getPotentialFunction();
		((CascadeQueryPotential)potential).resetPostingsListReader();
	}
	

	public String getPruner(){
		return mPruner;
	}

	public float getPruner_param(){
		return mPruner_param;
	}

	public void setPruner(String pruner){
		mPruner = pruner;
	}

	public void setPruner_param(float pruner_param){
		mPruner_param = pruner_param;
	}

	public int getCascadeStage(){
		return mCascadeStage;
	}

	public void setCascadeStage(int cs){
		mCascadeStage = cs;
	}

	//Collection CF of this term/bigram
	public float termCollectionCF(){
		PotentialFunction potential = getPotentialFunction();
                return ((CascadeQueryPotential) potential).termCollectionCF();
        }

	public float termCollectionDF(){
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential) potential).termCollectionDF();
	}

	public void setType(Type type) {
		//this.type = type;
		super.setType(type);
		
		if (type == Clique.Type.Term){
			mUnitCost = term_unit_cost;
                }
		else if (type == Clique.Type.Unorderd){ //typo
                        mUnitCost = unordered_unit_cost;
                }
		else if (type == Clique.Type.Ordered){
                        mUnitCost = ordered_unit_cost;
                }
                else{
                        System.out.println("Invalid type "+type);
                        System.exit(-1);
                }

	}



	public int getDocno(){
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential)potential).getDocno();
	}

	public int getNumberOfPostings(){
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential)potential).getNumberOfPostings();
	}

	public int getWindowSize(){
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential)potential).getWindowSize();
	}

	public String getScoringFunctionName(){ //dirichlet, bm25
		PotentialFunction potential = getPotentialFunction();
		return ((CascadeQueryPotential)potential).getScoringFunctionName();
	}

	public String [] getSingleTerms(){
		return mSingleTerms;
	}

	public String getParamID(){ //termWt, orderedWt, unorderedWt
		return getParameter().getName();
	}

	        
	public String toString() {
                StringBuilder s = new StringBuilder();
                s.append("<clique type=\"").append(getType()).append("\">");
                        
		s.append("<terms>").append(getConcept()).append("</terms>");
                        
		s.append("<terms>").append(getConcept()).append("</terms>").append(" wgts "+getWeight()).append("  pruner_and_param "+getPruner()+" "+getPruner_param()).append("cascadeStage "+getCascadeStage()).append(" unit_cost "+mUnitCost).append(" cliqueType "+getType());
                
                s.append("</clique>");
                
                return s.toString();
        }  

}
