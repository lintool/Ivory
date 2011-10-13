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

package ivory.cascade.model;

import ivory.cascade.model.potential.CascadeQueryPotential;
import ivory.smrf.model.Clique;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.potential.PotentialFunction;
import ivory.smrf.model.score.ScoringFunction;

import java.util.List;


/**
 * @author Lidan Wang
 */
public class CascadeClique extends Clique {
  // For estimating cascade cost
  static float term_unit_cost = 1;
  static float ordered_unit_cost = 20;
  static float unordered_unit_cost = 20;

  // Cascade stage
  private int cascadeStage;

  private String pruningFunction = "";
  private float pruningParameter = -1;

  public float cost;

  private String[] singleTerms;

  public CascadeClique(List<GraphNode> nodes, PotentialFunction f, Parameter weight,
      int cascadeStage, String pruner_and_params) {
    this(nodes, f, weight, 1.0f, null, true, cascadeStage, pruner_and_params);
  }

  public CascadeClique(List<GraphNode> nodes, PotentialFunction f, Parameter param,
      float importance, Type type, boolean docDependent, int cascadeStage, String pruner_and_params) {
    super(nodes, f, param, importance, type, docDependent);

    String concept = getConcept();
    String[] t = concept.trim().toLowerCase().split("\\s+");
    singleTerms = new String[t.length];
    for (int i = 0; i < t.length; i++) {
      singleTerms[i] = t[i];
    }

    this.cascadeStage = cascadeStage;

    if (pruner_and_params.indexOf("null") == -1) {
      String[] tokens = pruner_and_params.trim().split("\\s+");
      pruningFunction = tokens[0];
      pruningParameter = (float) (Double.parseDouble(tokens[1]));
    }
  }

  // If it's a term, then return positions at the current document
  // not supported if it's term proximity feature!
  public int[] getPositions() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getPositions();
  }

  public int getDocLen() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getDocLen();
  }

  // reset postings readers
  public void resetPostingsListReader() {
    PotentialFunction potential = getPotentialFunction();
    ((CascadeQueryPotential) potential).resetPostingsListReader();
  }

  public String getPruningFunction() {
    return pruningFunction;
  }

  public float getPruningParameter() {
    return pruningParameter;
  }

  public void setPruningFunction(String pruner) {
    this.pruningFunction = pruner;
  }

  public void setPruningParametes(float pruner_param) {
    pruningParameter = pruner_param;
  }

  public int getCascadeStage() {
    return cascadeStage;
  }

  public void setCascadeStage(int cs) {
    cascadeStage = cs;
  }

  // Collection CF of this term/bigram
  public long termCollectionCF() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).termCollectionCF();
  }

  public int termCollectionDF() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).termCollectionDF();
  }

  public void setType(Type type) {
    super.setType(type);

    if (type == Clique.Type.Term) {
      cost = term_unit_cost;
    } else if (type == Clique.Type.Unordered) {
      cost = unordered_unit_cost;
    } else if (type == Clique.Type.Ordered) {
      cost = ordered_unit_cost;
    } else {
      throw new RuntimeException("Invalid type " + type);
    }
  }

  public int getDocno() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getDocno();
  }

  public int getNumberOfPostings() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getNumberOfPostings();
  }

  public int getWindowSize() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getWindowSize();
  }

  public String getScoringFunctionName() { // dirichlet, bm25
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getScoringFunctionName();
  }

  public ScoringFunction getScoringFunction() {
    PotentialFunction potential = getPotentialFunction();
    return ((CascadeQueryPotential) potential).getScoringFunction();
  }

  public String[] getSingleTerms() {
    return singleTerms;
  }

  public String getParamID() { // termWt, orderedWt, unorderedWt
    return getParameter().getName();
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("<clique type=\"").append(getType()).append("\">");
    s.append("<terms>").append(getConcept()).append("</terms>");

    s.append("<terms>")
     .append(getConcept())
     .append("</terms>")
     .append(" wgts " + getWeight())
     .append("  pruner_and_param " + getPruningFunction() + " " + getPruningParameter())
     .append("cascadeStage " + getCascadeStage())
     .append(" unit_cost " + cost)
     .append(" cliqueType " + getType());

    s.append("</clique>");

    return s.toString();
  }
}
