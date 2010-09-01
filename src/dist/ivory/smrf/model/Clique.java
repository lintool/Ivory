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

import ivory.smrf.model.potential.PotentialFunction;

import java.util.List;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public class Clique {

	/**
	 * potential function scale factor
	 */
	protected double mScaleFactor;

	/**
	 * nodes associated with this clique
	 */
	protected List<GraphNode> mNodes = null;

	/**
	 * potential function associated with this clique
	 */
	protected PotentialFunction mFunction = null;

	/**
	 * parameter associated with this clique
	 */
	protected Parameter mParam = null;

	/**
	 * whether or not this clique is document dependent cliques it is up to the
	 * user to keep track of this!
	 */
	protected boolean mDocDependent = true;

	/**
	 * query-dependent weight
	 */
	protected double cliqueWgt;

	/**
	 * terms of this clique
	 */
	protected String cliqueTerms = null;

	/**
	 * clique type
	 */
	protected String cliqueType;

	/**
	 * @param nodes
	 * @param f
	 */
	public Clique(List<GraphNode> nodes, PotentialFunction f, Parameter weight) {
		mScaleFactor = 1.0;
		mNodes = nodes;
		mParam = weight;
		cliqueWgt = mParam.weight;
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
	public Clique(List<GraphNode> nodes, PotentialFunction f, Parameter weight, boolean docDependent) {
		mScaleFactor = 1.0;
		mNodes = nodes;
		mParam = weight;
		cliqueWgt = mParam.weight;
		mFunction = f;
		mDocDependent = docDependent;
	}

	public void initialize(GlobalEvidence globalEvidence) throws Exception {
		mFunction.initialize(mNodes, globalEvidence);
	}

	/**
	 * @return Returns the clique potential given the current configuration.
	 * @throws Exception
	 */
	public double getPotential() throws Exception {
		return mScaleFactor * mParam.weight * mFunction.getPotential();
	}

	/**
	 * @return Returns the value of feature function given the current
	 *         configuration.
	 * @throws Exception
	 */
	public double getPotential2() throws Exception {
		return mScaleFactor * mFunction.getPotential();
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
	public void setParameterID(String id) {
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
	 * @return query-dependent weight associated with this clique
	 */
	public double getCliqueWeight() {
		return cliqueWgt;
	}

	public void setCliqueWeight(double w) {
		cliqueWgt = w;
	}

	/**
	 * @return clique terms
	 */
	public String getCliqueTerms() {
		if (cliqueTerms != null)
			return cliqueTerms;

		StringBuilder sb = new StringBuilder();
		for (GraphNode n : mNodes) {
			if (n instanceof TermNode) {
				sb.append(((TermNode) n).getTerm()).append(" ");
			}
		}

		return sb.toString().trim();
	}

	public void setCliqueTerms(String ct) {
		cliqueTerms = ct;
	}

	/**
	 * @return clique type
	 */
	public String getCliqueType() {
		return cliqueType;
	}

	public void setCliqueType(String t) {
		cliqueType = t;
	}

	/**
	 * @return nodes in this clique
	 */
	public List<GraphNode> getNodes() {
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

	@Override
	public String toString() {
		return toString(false);
	}

	public String toString(boolean verbose) {
		StringBuilder s = new StringBuilder();
		s.append("<clique type=\"").append(cliqueType).append("\">");

		if (verbose) {
			s.append(mFunction.toString());
		} else {
			s.append("<terms>").append(getCliqueTerms()).append("</terms>");
		}

		s.append("</clique>");

		return s.toString();
	}

    //	public double getMaxScore() {
    //		return mScaleFactor * mParam.weight * mFunction.getMaxScore();
    //	}

    public double getMaxScore() {
	return mScaleFactor * cliqueWgt * mFunction.getMaxScore();
    } 

	/**
	 * @param docid
	 */
	public void setNextCandidate(int docid) {
		mFunction.setNextCandidate(docid);
	}
}
