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
import ivory.smrf.model.Clique;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.List;
import java.util.Set;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * @author Don Metzler
 */
public class FeatureBasedMRFBuilder extends MRFBuilder {
	// XML specification of features.
	private Node model = null;

	// Whether or not to normalize the feature importance weights.
	private boolean normalizeImportance = false;

	public FeatureBasedMRFBuilder(RetrievalEnvironment env, Node model) {
		super(env);
		this.model = Preconditions.checkNotNull(model);

		// Whether or not we should normalize the feature importance weights.
		normalizeImportance = XMLTools.getAttributeValue(model, "normalizeImportance", false);
	}

	@Override
	public MarkovRandomField buildMRF(String[] queryTerms) throws ConfigurationException {
		// This is the MRF we're building.
		MarkovRandomField mrf = new MarkovRandomField(queryTerms, env);

		// Construct MRF feature by feature.
		NodeList children = model.getChildNodes();

		// Sum of query-dependent importance weights.
		float totalImportance = 0.0f;

		// Cliques that have query-dependent importance weights.
		Set<Clique> cliquesWithImportance = Sets.newHashSet();

		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);

			if ("feature".equals(child.getNodeName())) {
				// Get the feature id.
				String featureID = XMLTools.getAttributeValueOrThrowException(child, "id",
				    "Each feature must specify an id attribute!");

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

				// Construct the clique set.
				CliqueSet cliqueSet = CliqueSet.create(cliqueSetType, env, queryTerms, child);

				// Get cliques from clique set.
				List<Clique> cliques = cliqueSet.getCliques();

				for (Clique c : cliques) {
					c.setParameterName(featureID); 	 // Parameter id.
					c.setParameterWeight(weight);    // Weight.
					c.setType(cliqueSet.getType());  // Clique type.

					// Get clique weight.
					if (importanceModel != null) {
						float importance = importanceModel.getCliqueWeight(c);						
						c.setImportance(importance);

						totalImportance += importance;
						cliquesWithImportance.add(c);						
					}

					// Add clique to MRF.
					mrf.addClique(c);
				}
			}
		}

		// Normalize query-dependent feature importance values.
		if (normalizeImportance) {
			for (Clique c : cliquesWithImportance) {
				c.setImportance(c.getImportance() / totalImportance);
			}
		}

		return mrf;
	}
}