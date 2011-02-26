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
import ivory.smrf.model.Clique;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 */
public class OrderedCliqueSet extends CliqueSet {
	@Override
	public void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode) throws ConfigurationException {
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(queryTerms);
		Preconditions.checkNotNull(domNode);

		String dependenceType = XMLTools.getAttributeValue(domNode, "dependence", "sequential");
		boolean docDependent = XMLTools.getAttributeValue(domNode, "docDependent", true);

		// Initialize clique set.
		clearCliques();

		// generate clique set
    if ("sequential".equals(dependenceType)) {
			addCliques(CliqueFactory.getSequentialDependenceCliques(env, queryTerms, domNode, docDependent));
		} else if ("full".equals(dependenceType)) {
			addCliques(CliqueFactory.getFullDependenceCliques(env, queryTerms, domNode, true, docDependent));
		} else {
			throw new ConfigurationException("Unrecognized OrderedCliqueSet type \"" + dependenceType + "\"!");
		}
	}

	@Override
	public Clique.Type getType() {
		return Clique.Type.Ordered;
	}
}
