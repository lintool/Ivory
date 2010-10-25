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

import java.util.Comparator;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public class Clique {

	/**
	 * nodes associated with this clique
	 */
	private List<GraphNode> mNodes = null;

	/**
	 * potential function associated with this clique
	 */
	private PotentialFunction mFunction = null;

	/**
	 * parameter associated with this clique
	 */
	private Parameter mParam = null;

	/**
	 * whether or not this clique is document dependent cliques it is up to the
	 * user to keep track of this!
	 */
	private boolean mDocDependent = true;

	/**
	 * combined weight = parameter weight * clique importance
	 */
	private float mCombinedWeight;
	
	/**
	 * query-dependent clique importance
	 */
	private float mImportance;

	/**
	 * clique type
	 */
	private String mType;

	/**
	 * textual representation of the term nodes in this clique
	 */
	private String mConcept;
	
	/**
	 * @param nodes
	 * @param f
	 */
	public Clique(List<GraphNode> nodes, PotentialFunction f, Parameter weight) {
		this(nodes, f, weight, 1.0f, null, true);
	}

	public Clique(List<GraphNode> nodes, PotentialFunction f, Parameter param, float importance, String type, boolean docDependent) {
		mNodes = Preconditions.checkNotNull(nodes);
		mParam = Preconditions.checkNotNull(param);
		mFunction = Preconditions.checkNotNull(f);

		mImportance = importance;
		mCombinedWeight = param.weight * importance;
		mType = type;
		mDocDependent = docDependent;
		
		mConcept = generateConcept();
	}

	public void initialize(GlobalEvidence globalEvidence) throws ConfigurationException {
		Preconditions.checkNotNull(globalEvidence);
		
		mFunction.initialize(mNodes, globalEvidence);
	}

	/**
	 * @return Returns the value of feature function given the current
	 *         configuration.
	 * @throws Exception
	 */
	public float getPotential() {
		return mFunction.computePotential();
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
	public void setParameterId(String id) {
		mParam.id = id;
	}

	/**
	 * @return weight associated with this clique
	 */
	public float getWeight() {
		return mCombinedWeight;
	}

	/**
	 * @return clique-dependent importance
	 */
	public float getImportance() {
		return mImportance;
	}
	
	/**
	 * @param weight
	 */
	public void setParameterWeight(float weight) {
		mParam.weight = weight;
		mCombinedWeight = mParam.weight * mImportance;
	}

	public void setImportance(float w) {
		mImportance = w;
		mCombinedWeight = mParam.weight * w;
	}

	/**
	 * @return clique terms
	 */
	public String getConcept() {
		return mConcept;
	}
	
	/**
	 * @return textual representation of the term nodes in this clique
	 */
	private String generateConcept() {
		StringBuilder sb = new StringBuilder();
		for (GraphNode n : mNodes) {
			if (n.getType() == GraphNode.Type.TERM) {
				sb.append(((TermNode) n).getTerm()).append(" ");
			}
		}

		return sb.toString().trim();
	}

	/**
	 * @return clique type
	 */
	public String getType() {
		return mType;
	}

	public void setType(String t) {
		mType = t;
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
		s.append("<clique type=\"").append(mType).append("\">");

		if (verbose) {
			s.append("<docdependent>").append(mDocDependent).append("</docdependent>");
			s.append("<weight>").append(mCombinedWeight).append("</weight>");
			s.append(mFunction.toString());
			s.append("<maxscore>").append(mFunction.getMaxScore()).append("</maxscore>");
		} else {
			s.append("<terms>").append(getConcept()).append("</terms>");
		}

		s.append("</clique>");

		return s.toString();
	}

	public float getMaxScore() {
		return mCombinedWeight * mFunction.getMaxScore();
	}

	/**
	 * @param docid
	 */
	public void setNextCandidate(int docid) {
		mFunction.setNextCandidate(docid);
	}
	
	public static class MaxScoreComparator implements Comparator<Clique> {
		public int compare(Clique a, Clique b) {
			double maxScoreA = a.getMaxScore();
			double maxScoreB = b.getMaxScore();
			if (maxScoreA == maxScoreB) {
				return 0;
			} else if (maxScoreA < maxScoreB) {
				return 1;
			} else {
				return -1;
			}
		}

	}

	public PotentialFunction getScoringFunction() {
		return mFunction;
	}
}
