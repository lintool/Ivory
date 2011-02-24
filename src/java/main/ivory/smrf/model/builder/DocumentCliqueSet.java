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
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.potential.PotentialFunction;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author Don Metzler
 */
public class DocumentCliqueSet extends CliqueSet {
	@Override
	public void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode) throws ConfigurationException {
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(domNode);

		// Initialize clique set.
		clearCliques();

		DocumentNode docNode = new DocumentNode();
		ArrayList<GraphNode> cliqueNodes = Lists.newArrayList();
		cliqueNodes.add(docNode);

		String paramId = XMLTools.getAttributeValue(domNode, "id");
		if (paramId == null) {
			throw new ConfigurationException("Error: A potential attribute must be specified in order to generate a clique set!");
		}

		float weight = XMLTools.getAttributeValue(domNode, "weight", 1.0f);
		Parameter parameter = new Parameter(paramId, weight);
		String potentialType = XMLTools.getAttributeValue(domNode, "potential");
		if (potentialType == null) {
			throw new ConfigurationException("Error: A potential type must be specified!");
		}
		PotentialFunction potential = PotentialFunction.create(env, potentialType, domNode);

		Clique c = new Clique(cliqueNodes, potential, parameter, 1.0f, getType(), true);
		addClique(c);
	}

	@Override
	public Clique.Type getType() {
		return Clique.Type.Document;
	}
}
