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

import java.util.List;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author Don Metzler
 */
public abstract class CliqueSet {
	private final List<Clique> cliques = Lists.newArrayList();

  public abstract void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode)
      throws ConfigurationException;

	protected void addClique(Clique c) {
		cliques.add(c);
	}

	protected void addCliques(List<Clique> cliques) {
		this.cliques.addAll(cliques);
	}

	public List<Clique> getCliques() {
		return cliques;
	}

	protected void clearCliques() {
		cliques.clear();
	}
	
	public abstract Clique.Type getType();

	@SuppressWarnings("unchecked")
	public static CliqueSet create(String type, RetrievalEnvironment env, String[] queryTerms, Node domNode) throws ConfigurationException {
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(queryTerms);
		Preconditions.checkNotNull(domNode);

		try {
			Class<? extends CliqueSet> clz = (Class<? extends CliqueSet>) Class.forName(type);
			CliqueSet f = clz.newInstance();
			f.configure(env, queryTerms, domNode);

			return f;
		} catch (Exception e) {
			throw new ConfigurationException("Unable to instantiate CliqueSet type " + type, e);
		}
	}
}