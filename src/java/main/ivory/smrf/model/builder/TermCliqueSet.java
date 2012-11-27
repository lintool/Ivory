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

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.util.XMLTools;
import ivory.smrf.model.Clique;
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GraphNode;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.potential.PotentialFunction;

import java.util.List;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author Don Metzler
 */
public class TermCliqueSet extends CliqueSet {
  @Override
  public void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode)
      throws ConfigurationException {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryTerms);
    Preconditions.checkNotNull(domNode);

    boolean docDependent = XMLTools.getAttributeValue(domNode, "docDependent", true);

    // Initialize clique set.
    clearCliques();

    // The document node.
    DocumentNode docNode = new DocumentNode();

    // Default parameter associated with clique set.
    Parameter termParameter = new Parameter(Parameter.DEFAULT, 1.0f);

    // Get potential type.
    String potentialType = XMLTools.getAttributeValueOrThrowException(domNode, "potential",
        "A potential attribute must be specified in order to generate a CliqueSet!");

    // Add clique for each query term.
    for (String element : queryTerms) {
      // Add term node.
      TermNode termNode = new TermNode(element);

      // Add document/term clique.
      List<GraphNode> cliqueNodes = Lists.newArrayList();
      if (docDependent) {
        cliqueNodes.add(docNode);
      }
      cliqueNodes.add(termNode);

      // Get the potential function.
      PotentialFunction potential = PotentialFunction.create(env, potentialType, domNode);

      Clique c = new Clique(cliqueNodes, potential, termParameter);
      addClique(c);
    }
  }

  @Override
  public Clique.Type getType() {
    return Clique.Type.Term;
  }
}
