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

package ivory.smrf.model.importance;

import ivory.exception.ConfigurationException;
import ivory.smrf.model.Clique;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 */
public abstract class ConceptImportanceModel {

	// Configures the model.
	public abstract void configure(Node model) throws ConfigurationException;

	// Returns the importance of the concept.
	public abstract float getConceptWeight(String concept);

	// Returns the importance of the concepts currently associated with a clique.
	public abstract float getCliqueWeight(Clique c);

	@SuppressWarnings("unchecked")
	public static ConceptImportanceModel get(Node model) throws ConfigurationException {
		Preconditions.checkNotNull(model);

		// Get model type.
		String modelType = XMLTools.getAttributeValue(model, "type", null);
		if (modelType == null) {
			throw new ConfigurationException("Model type must be specified!");
		}

		// Dynamically construct importance model.
		ConceptImportanceModel importanceModel = null;
		try {
			Class<? extends ConceptImportanceModel> clz = (Class<? extends ConceptImportanceModel>) Class.forName(modelType);
			importanceModel = clz.newInstance();
			importanceModel.configure(model);
		} catch (Exception e) {
			throw new ConfigurationException("Error instantiating ConceptImportanceModel! " + e);
		}

		return importanceModel;
	}
}
