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

import ivory.smrf.model.potential.PotentialFunction;

import java.util.Iterator;
import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class Clique {

	/**
	 * potential function scale factor
	 */
	protected double mScaleFactor;
	
	/**
	 * nodes associated with this clique
	 */
	protected List<Node> mNodes = null;
	
	/**
	 * potential function associated with this clique
	 */
	protected PotentialFunction mFunction = null; 
	
	/**
	 * parameter associated with this clique 
	 */
	protected Parameter mParam = null;
	
	/**
	 * whether or not this clique is document dependent cliques
	 * it is up to the user to keep track of this! 
	 */
	protected boolean mDocDependent = true;
	
	/**
	 * @param nodes
	 * @param f
	 */
	public Clique(List<Node> nodes, PotentialFunction f, Parameter weight) {
		mScaleFactor = 1.0;
		mNodes = nodes;
		mParam = weight;
		mFunction = f;
		mDocDependent = true;
	}
	
	/**
	 * @param nodes
	 * @param f
	 * @param weight
	 * @param docDependent
	 * @throws Exception
	 */
	public Clique(List<Node> nodes, PotentialFunction f, Parameter weight, boolean docDependent) {
		mScaleFactor = 1.0;
		mNodes = nodes;
		mParam = weight;
		mFunction = f;
		mDocDependent = docDependent;
	}

	/**
	 * @throws SMRFException
	 */
	public void initialize(GlobalEvidence globalEvidence) throws Exception {
		mFunction.initialize( mNodes, globalEvidence );
	}
	
	/**
	 * @return Returns the clique potential given the current
	 *         configuration.
	 * @throws Exception
	 */
	public double getPotential() throws Exception {
		return mScaleFactor * mParam.weight * mFunction.getPotential();
	}
	
	/**
	 * @param scale
	 */
	public void setScaleFactor(double scale) {
		mScaleFactor = scale;
	}
	
	/**
	 * @return scale factor of clique
	 */
	public double getScaleFactor() {
		return mScaleFactor;
	}
	
	/**
	 * @return parameter associated with this clique
	 */
	public Parameter getParameter() {
		return mParam;
	}
	
	/**
	 * @param id
	 */
	public void setParameterID( String id ) {
		mParam.id = id;
	}
	
	/**
	 * @return weight associated with this clique
	 */
	public double getWeight() {
		return mParam.weight;
	}
	
	/**
	 * @param weight
	 */
	public void setWeight(double weight) {
		mParam.weight = weight;
	}
	
	/**
	 * @return nodes in this clique
	 */
	public List<Node> getNodes() {
		return mNodes;
	}
	
	/**
	 * @return if this clique should be optimized or not
	 */
	public boolean isDocDependent() {
		return mDocDependent;
	}
	
	/**
	 * @return next candidate document
	 */
	public int getNextCandidate() {
		return mFunction.getNextCandidate();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String ret = new String();
		ret = "<clique weight=\"" + mParam + "\">\n";
		ret += mFunction;
		for( Iterator<Node> nodeIter = mNodes.iterator(); nodeIter.hasNext(); ) {
			ret += nodeIter.next();
		}
		return ret + "</clique>\n";
	}

	public double getMaxScore() {
		return mScaleFactor * mParam.weight * mFunction.getMaxScore();
	}

	/**
	 * @param docid
	 */
	public void setNextCandidate(int docid) {
		mFunction.setNextCandidate(docid);
	}
}
