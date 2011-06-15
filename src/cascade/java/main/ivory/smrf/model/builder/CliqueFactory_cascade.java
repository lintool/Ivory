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
import ivory.smrf.model.Clique_cascade;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.potential.PotentialFunction;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;

import com.google.common.collect.Lists;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public class CliqueFactory_cascade {

	public static List<Clique> getSequentialDependenceCliques(RetrievalEnvironment env,
			String[] queryTerms, Node domNode, boolean docDependent, int cascadeStage, String pruner_and_params) throws ConfigurationException {
		// Clique set.
		List<Clique> cliques = Lists.newArrayList();

		// The document node.
		DocumentNode docNode = new DocumentNode();

		// If there are no terms, then return an empty MRF.
		if (queryTerms.length == 0) {
			return cliques;
		}

		// Default parameter associated with CliqueSet.
		Parameter parameter = new Parameter(Parameter.DEFAULT, 1.0f);

		// Get potential type.
		String potentialType = XMLTools.getAttributeValue(domNode, "potential", null);
		if (potentialType == null) {
			throw new ConfigurationException("A potential attribute must be specified in order to generate a clique set!");
		}

		// If there is more than one term, then add appropriate cliques.
		List<GraphNode> cliqueNodes = null;
		Clique c = null;
		TermNode lastTermNode = null;

		for (String element : queryTerms) {
			// Term node.
			TermNode termNode = new TermNode(element);

			// Add sequential cliques.
			if (lastTermNode != null) {
				cliqueNodes = Lists.newArrayList();
				if (docDependent) {
					cliqueNodes.add(docNode);
				}
				cliqueNodes.add(lastTermNode);
				cliqueNodes.add(termNode);

				// Get the potential function.
				PotentialFunction potential = PotentialFunction.create(env, potentialType, domNode);

				c = new Clique_cascade(cliqueNodes, potential, parameter, cascadeStage, pruner_and_params);

				cliques.add(c);
			}

			// Update last term node.
			lastTermNode = termNode;
		}

		return cliques;
	}

	public static List<Clique> getFullDependenceCliques(RetrievalEnvironment env,
			String[] queryTerms, Node domNode, boolean ordered, boolean docDependent, int cascadeStage, String pruner_and_params)
			throws ConfigurationException {
		// Clique set.
		List<Clique> cliques = Lists.newArrayList();

		// The document node.
		DocumentNode docNode = new DocumentNode();

		// If there are no terms, then return an empty MRF.
		if (queryTerms.length == 0) {
			return cliques;
		}

		// Default parameter associated with CliqueSet.
		Parameter parameter = new Parameter(Parameter.DEFAULT, 1.0f);

		// Get potential type.
		String potentialType = XMLTools.getAttributeValue(domNode, "potential", null);
		if (potentialType == null) {
			throw new ConfigurationException("A potential attribute must be specified in order to generate a clique set!");
		}
		// If there is more than one term, then add appropriate cliques.
		ArrayList<GraphNode> cliqueNodes = null;
		Clique c = null;

		for (int i = 1; i < Math.pow(2, queryTerms.length); i++) {
			String binary = Integer.toBinaryString(i);
			int padding = queryTerms.length - binary.length();
			for (int j = 0; j < padding; j++) {
				binary = "0" + binary;
			}

			boolean singleTerm = false;
			boolean contiguous = true;

			int firstOne = binary.indexOf('1');
			int lastOne = binary.lastIndexOf('1');
			if (lastOne == firstOne) {
				singleTerm = true;
			}

			for (int j = binary.indexOf('1') + 1; j <= binary.lastIndexOf('1') - 1; j++) {
				if (binary.charAt(j) == '0') {
					contiguous = false;
					break;
				}
			}

			if (ordered && !singleTerm && contiguous) {
				cliqueNodes = Lists.newArrayList();
				if (docDependent) {
					cliqueNodes.add(docNode);
				}
				for (int j = firstOne; j <= lastOne; j++) {
					TermNode termNode = new TermNode(queryTerms[j]);
					cliqueNodes.add(termNode);
				}

				// Get the potential function.
				PotentialFunction potential = PotentialFunction.create(env, potentialType, domNode);

				c = new Clique_cascade(cliqueNodes, potential, parameter, cascadeStage, pruner_and_params);
				cliques.add(c);
			} else if (!ordered && !singleTerm && !contiguous) {
				cliqueNodes = Lists.newArrayList();
				if (docDependent) {
					cliqueNodes.add(docNode);
				}
				for (int j = 0; j < binary.length(); j++) {
					if (binary.charAt(j) == '1') {
						TermNode termNode = new TermNode(queryTerms[j]);
						cliqueNodes.add(termNode);
					}
				}

				// Get the potential function.
				PotentialFunction potential = PotentialFunction.create(env, potentialType, domNode);

				c = new Clique_cascade(cliqueNodes, potential, parameter, cascadeStage, pruner_and_params);
				cliques.add(c);
			}
		}

		return cliques;
	}
}
