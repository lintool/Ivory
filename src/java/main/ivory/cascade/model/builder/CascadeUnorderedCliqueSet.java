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

package ivory.cascade.model.builder;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.util.XMLTools;
import ivory.smrf.model.Clique;

import org.w3c.dom.Node;


import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public class CascadeUnorderedCliqueSet extends CascadeCliqueSet {

  @Override
  public void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode,
      int cascadeStage, String pruner_and_params) throws ConfigurationException {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryTerms);
    Preconditions.checkNotNull(domNode);

    String dependenceType = XMLTools.getAttributeValue(domNode, "dependence", "sequential");
    boolean docDependent = XMLTools.getAttributeValue(domNode, "docDependent", true);

    // Initialize clique set.
    clearCliques();

    if (dependenceType.equals("sequential")) {
      throw new ConfigurationException(
          "Unsupported operation: there are no unordered cliques within a sequentially dependent graph.");
    } else if (dependenceType.equals("full")) {
      addCliques(CascadeCliqueFactory.getFullDependenceCliques(env, queryTerms, domNode, false,
          docDependent, cascadeStage, pruner_and_params));
    } else {
      throw new ConfigurationException("Unrecognized UnorderedCliqueSet type: " + dependenceType);
    }
  }

  @Override
  public Clique.Type getType() {
    return Clique.Type.Unordered;
  }
}
