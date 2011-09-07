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

package ivory.smrf.model.constrained;

import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.core.exception.RetrievalException;
import ivory.core.util.XMLTools;
import ivory.smrf.model.Clique;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.importance.LinearImportanceModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Node;

import edu.umd.cloud9.util.map.HMapKF;

/**
 * @author Lidan Wang
 */
public class GreedyConstrainedMRFBuilder extends ConstrainedMRFBuilder {

  // document frequencies, from which costs will be computed
  private HMapKF<String> dfs;

  // model style (either Indep or Joint)
  private String modelType;

  // bin multiple
  private float qlMultiple;

  // basic thresholds
  private float unigramAddThreshold;
  private float bigramAddThreshold;

  // redundancy thresholds
  private float unigramRedundThreshold;
  private float bigramRedundThreshold;

  // beta value
  private float beta;

  public GreedyConstrainedMRFBuilder(RetrievalEnvironment env, Node model)
      throws ConfigurationException, IOException {
    super(env, model);

    // model type
    modelType = XMLTools.getAttributeValue(model, "style", null);
    if (modelType == null || (!"Indep".equals(modelType) && !"Joint".equals(modelType))) {
      throw new RetrievalException(
          "Error: GreedyConstrainedMRFBuilder requires a model type attribute of Indep or Joint!");
    }

    // query likelihood
    qlMultiple = XMLTools.getAttributeValue(model, "qlMultiple", -1.0f);

    // unigram and bigram basic thresholds
    unigramAddThreshold = XMLTools.getAttributeValue(model, "unigramAddThreshold", -1.0f);
    bigramAddThreshold = XMLTools.getAttributeValue(model, "bigramAddThreshold", -1.0f);

    // unigram and bigram redundancy thresholds
    unigramRedundThreshold = XMLTools.getAttributeValue(model, "unigramRedundThreshold", -1.0f);
    bigramRedundThreshold = XMLTools.getAttributeValue(model, "bigramRedundThreshold", -1.0f);

    // beta value
    beta = XMLTools.getAttributeValue(model, "beta", -1.0f);

    if ("Indep".equals(modelType) && (qlMultiple == -1 || unigramAddThreshold == -1)) {
      throw new RetrievalException(
          "Error: Indep model must specify valid qlMultiple, unigramAddThreshold, and bigramAddThreshold attributes!");
    }

    if ("Joint".equals(modelType) &&
         (qlMultiple == -1 || unigramAddThreshold == -1 || bigramAddThreshold == -1
            || unigramRedundThreshold == -1 || bigramRedundThreshold == -1 || beta == -1)) {
      throw new RetrievalException(
          "Error: Joint model must specify valid qlMultiple, unigramAddThreshold, bigramAddThreshold, unigramRedundThreshold, bigramRedundThreshold, and beta attributes!");
    }

    String file = XMLTools.getAttributeValue(model, "file", null);
    if (file == null) {
      throw new RetrievalException(
          "Error: GreedyConstrainedMRFBuilder requires a file attribute specifying the location of the document frequencies!");
    }

    // Read document frequencies.
    dfs = LinearImportanceModel.readDataStats(file);
  }

  @Override
  protected MarkovRandomField buildConstrainedMRF(String[] queryTerms, MarkovRandomField mrf) {
    List<Clique> cliques = mrf.getCliques();

    float qlCost = 0.0f;
    Set<String> seenTerms = new HashSet<String>();

    int numQueryTerms = queryTerms.length;

    // generate constrained cliques
    List<ConstrainedClique> constrainedCliques = new ArrayList<ConstrainedClique>();
    for (Clique c : cliques) {
      // type of clique
      Clique.Type cliqueType = c.getType();

      // terms associated with clique
      String cliqueTerms = c.getConcept();

      ConstrainedClique newClique = new ConstrainedClique(c);

      // get+set analytical cost
      float analyticalCost = getCost(cliqueTerms);
      newClique.setAnalyticalCost(analyticalCost);

      // get+set profit density
      float profitDensity = c.getWeight() / analyticalCost;
      newClique.setProfitDensity(profitDensity);

      if (cliqueType.equals(Clique.Type.Term)) {
        if (!(seenTerms.contains(cliqueTerms))) {
          qlCost += analyticalCost;
          seenTerms.add(cliqueTerms);
        }
      }

      constrainedCliques.add(newClique);
    }

    float binConstraint = qlMultiple * qlCost;

    List<ConstrainedClique> selectedCliques = null;
    if ("Indep".equals(modelType) || numQueryTerms == 1) {
      selectedCliques = ConstraintModel.greedyKnapsack(constrainedCliques, binConstraint,
          unigramAddThreshold, bigramAddThreshold);
    } else if ("Joint".equals(modelType)) {
      selectedCliques = ConstraintModel.greedyJoint(constrainedCliques, binConstraint,
          unigramAddThreshold, bigramAddThreshold, unigramRedundThreshold,
          bigramRedundThreshold, beta);
    }

    // construct constrained mrf
    MarkovRandomField constrainedMRF = new MarkovRandomField(queryTerms, env);
    for (Clique c : selectedCliques) {
      constrainedMRF.addClique(c);
    }

    return constrainedMRF;
  }

  private float getCost(String cliqueTerms) {
    float r = 0;

    String[] terms = cliqueTerms.trim().split("\\s+");

    for (int k = 0; k < terms.length; k++) {

      if (dfs.containsKey(terms[k])) {
        r += Math.log(dfs.get(terms[k]) + 1.01);
        // Lidan: add 0.01 in case df = 1, log(1) = 0.
      } else {
        r += Math.log(1.01);
      }
    }

    return r;
  }
}
