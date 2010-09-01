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

import ivory.smrf.model.Clique;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public class OrderedCliqueSet extends CliqueSet {

	@Override
	public void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode)
			throws Exception {
		String dependenceType = XMLTools.getAttributeValue(domNode, "dependence", "sequential");
		boolean docDependent = XMLTools.getAttributeValue(domNode, "docDependent", true);

		// initialize clique set
		cliques = new ArrayList<Clique>();

		// generate clique set
		if (dependenceType.equals("sequential")) {
			cliques = CliqueFactory.getSequentialDependenceCliques(env, queryTerms, domNode,
					docDependent);
		} else if (dependenceType.equals("full")) {
			cliques = CliqueFactory.getFullDependenceCliques(env, queryTerms, domNode, true,
					docDependent);
		} else {
			throw new Exception("Unrecognized OrderedCliqueSet type: " + dependenceType);
		}
	}

	public String getType() {
		return "Ordered";
	}
}
