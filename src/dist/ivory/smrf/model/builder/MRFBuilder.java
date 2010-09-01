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

import ivory.smrf.model.MarkovRandomField;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public abstract class MRFBuilder {

	protected RetrievalEnvironment mEnv = null;

	public MRFBuilder(RetrievalEnvironment env) {
		mEnv = env;
	}

	public abstract MarkovRandomField buildMRF(String[] queryTerms) throws Exception;

	public static MRFBuilder get(RetrievalEnvironment env, Node model) throws Exception {
		if (model == null) {
			throw new Exception("Unable to generate a MRFBuilder from a null node!");
		}

		// get model type
		String modelType = XMLTools.getAttributeValue(model, "type", null);
		if (modelType == null) {
			throw new Exception("Model type must be specified!");
		}

		// build the builder
		MRFBuilder builder = null;

		System.out.println("The model type is .. "+modelType);

		if (modelType.equals("Feature") || modelType.equals("WSD")) {
			builder = new FeatureBasedMRFBuilder(env, model);

		}
		/* 
		else if (modelType.equals("WSD")) { 
			builder = new FeatureBasedWSDBuilder(env, model);
		} 
		*/

		else {
			throw new Exception("Unrecognized model type: " + modelType);
		}

		return builder;
	}
}
