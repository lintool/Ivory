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
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.builder.MRFBuilder;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * @author Don Metzler
 * @author Lidan Wang
 *
 */
public abstract class ConstrainedMRFBuilder extends MRFBuilder {

	private MRFBuilder mBuilder;
	
	public ConstrainedMRFBuilder(RetrievalEnvironment env, Node model) throws ConfigurationException {
		super(env);

		NodeList children = model.getChildNodes();
		for(int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			
			// this is the model that is going to be constrained
			if("constrainedmodel".equals(child.getNodeName())) {
				mBuilder = MRFBuilder.get(env, child);
			}
		}
		
		if(mBuilder == null) {
			throw new ConfigurationException("ConstrainedMRFBuilder is missing required constrainedModel node!");
		}
	}
	
	@Override
	public MarkovRandomField buildMRF(String[] queryTerms) throws ConfigurationException {
		// build unconstrained MRF model
		MarkovRandomField unconstrainedMRF = mBuilder.buildMRF(queryTerms);
		
		// get constrained version of the model
		return buildConstrainedMRF(queryTerms, unconstrainedMRF);
	}
	
	protected abstract MarkovRandomField buildConstrainedMRF(String [] queryTerms, MarkovRandomField mrf);
}
