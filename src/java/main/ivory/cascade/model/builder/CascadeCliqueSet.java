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

import ivory.core.RetrievalEnvironment;
import ivory.core.exception.ConfigurationException;
import ivory.smrf.model.Clique;

import java.util.List;

import org.w3c.dom.Node;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public abstract class CascadeCliqueSet {

  /**
   * cliques that make up this clique set
   */
  private final List<Clique> cliques = Lists.newArrayList();

  public abstract void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode,
      int cascadeStage, String pruner_and_params) throws ConfigurationException;

  protected void addClique(Clique c) {
    cliques.add(c);
  }

  protected void addCliques(List<Clique> list) {
    cliques.addAll(list);
  }

  public List<Clique> getCliques() {
    return cliques;
  }

  protected void clearCliques() {
    cliques.clear();
  }

  public abstract Clique.Type getType();

  @SuppressWarnings("unchecked")
  public static CascadeCliqueSet create(String type, RetrievalEnvironment env, String[] queryTerms,
      Node domNode, int cascadeStage, String pruner_and_params) throws ConfigurationException {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryTerms);
    Preconditions.checkNotNull(domNode);

    try {
      Class<? extends CascadeCliqueSet> clz = (Class<? extends CascadeCliqueSet>) Class.forName(type);
      CascadeCliqueSet f = clz.newInstance();

      f.configure(env, queryTerms, domNode, cascadeStage, pruner_and_params);

      return f;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Unable to instantiate CliqueSet!");
    }
  }
}
