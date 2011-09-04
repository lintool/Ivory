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

import ivory.core.exception.ConfigurationException;
import ivory.smrf.model.potential.PotentialFunction;

import java.util.Comparator;
import java.util.List;


import com.google.common.base.Preconditions;

/**
 * A clique in a MRF.
 *
 * @author Don Metzler
 * @author Lidan Wang
 */
public class Clique {
	public static enum Type { Document, Term, Ordered, Unordered }

	private final List<GraphNode> nodes;        // Nodes associated with this clique.
	private final PotentialFunction potential;  // Potential function associated with this clique.
	private final Parameter param;              // Parameter associated with this clique.
	private final boolean isDocDependent;  	    // Note: User must keep track of this!

	private float combinedWeight;               // combined weight = parameter weight * clique importance
	private float importance;                   // Query-dependent clique importance.
	private Type type;                          // Clique type.
	private String concept;                     // Textual representation of the term nodes in this clique.
	
	public Clique(List<GraphNode> nodes, PotentialFunction f, Parameter weight) {
		this(nodes, f, weight, 1.0f, null, true);
	}

	public Clique(List<GraphNode> nodes, PotentialFunction f, Parameter param, float importance, Type type, boolean docDependent) {
		this.nodes = Preconditions.checkNotNull(nodes);
		this.param = Preconditions.checkNotNull(param);
		this.potential = Preconditions.checkNotNull(f);
		this.isDocDependent = docDependent;

		this.importance = importance;
		this.combinedWeight = param.getWeight() * importance;
		this.type = type;
		this.concept = generateConcept();
	}
	
	private String generateConcept() {
		StringBuilder sb = new StringBuilder();
		for (GraphNode n : nodes) {
			if (n.getType() == GraphNode.Type.TERM) {
				sb.append(((TermNode) n).getTerm()).append(" ");
			}
		}

		return sb.toString().trim();
	}

	/**
	 * Initializes this clique.
	 */
	public void initialize(GlobalEvidence globalEvidence) throws ConfigurationException {
		Preconditions.checkNotNull(globalEvidence);
		this.potential.initialize(nodes, globalEvidence);
	}

	/**
	 * Returns the potential of this clique.
	 */
	public float getPotential() {
		return potential.computePotential();
	}

	/**
	 * Returns the potential function associated with this clique.
	 */
	public PotentialFunction getPotentialFunction() {
		return potential;
	}

	/**
	 * Returns the parameter associated with this clique.
	 */
	public Parameter getParameter() {
		return param;
	}

	/**
	 * Sets the parameter name.
	 */
	public void setParameterName(String name) {
		this.param.setName(name);
	}

	/**
	 * Sets the parameter weight.
	 */
	public void setParameterWeight(float weight) {
		this.param.setWeight(weight);
		this.combinedWeight = weight * importance;
	}

	/**
	 * Returns the weight associated with this clique
	 */
	public float getWeight() {
		return combinedWeight;
	}

	/**
	 * Returns the clique-dependent importance.
	 */
	public float getImportance() {
		return importance;
	}

	/**
	 * Sets the clique-dependent importance.
	 */
	public void setImportance(float w) {
		this.importance = w;
		this.combinedWeight = param.getWeight() * w;
	}

	/**
	 * Returns a textual representation of the term nodes in this clique.
	 */
	public String getConcept() {
		return concept;
	}

	/**
	 * Returns the clique type.
	 */
	public Type getType() {
		return type;
	}

	/**
	 * Sets the clique type.
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Returns nodes in this clique.
	 */
	public List<GraphNode> getNodes() {
		return nodes;
	}

	/**
	 * Returns whether or not this clique is document dependent.
	 */
	public boolean isDocDependent() {
		return isDocDependent;
	}

	/**
	 * Returns the next candidate document.
	 */
	public int getNextCandidate() {
		return this.potential.getNextCandidate();
	}

	/**
	 * Returns the max score.
	 */
	public float getMaxScore() {
    if (combinedWeight == 0.0) {
      return 0.0f;
    } else if (combinedWeight < 0.0) {
      return combinedWeight * potential.getMinScore();
    } else {
      return combinedWeight * potential.getMaxScore();
    }
	}

	/**
	 * Sets the next candidate for evaluation.
	 */
	public void setNextCandidate(int docid) {
		this.potential.setNextCandidate(docid);
	}

	@Override
	public String toString() {
		return toString(false);
	}

	public String toString(boolean verbose) {
		StringBuilder s = new StringBuilder();
		s.append("<clique type=\"").append(type).append("\">");

		if (verbose) {
			s.append("<docdependent>").append(isDocDependent).append("</docdependent>");
			s.append("<weight>").append(combinedWeight).append("</weight>");
			s.append(potential.toString());
			s.append("<maxscore>").append(potential.getMaxScore()).append("</maxscore>");
		} else {
			s.append("<terms>").append(getConcept()).append("</terms>");
		}

		s.append("</clique>");

		return s.toString();
	}

	/**
	 * Comparator based on the max score of a clique.
	 */
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
}
