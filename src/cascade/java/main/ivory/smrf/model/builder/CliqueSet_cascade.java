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

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 * @author Lidan Wang
 */
public abstract class CliqueSet_cascade {

	/**
	 * cliques that make up this clique set
	 */
	private final List<Clique> mCliques = new ArrayList<Clique>();

	public abstract void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode, int cascadeStage, String pruner_and_params)
			throws ConfigurationException;

	protected void addClique(Clique c) {
		mCliques.add(c);
	}

	protected void addCliques(List<Clique> cliques) {
		mCliques.addAll(cliques);
	}

	public List<Clique> getCliques() {
		return mCliques;
	}

	protected void clearCliques() {
		mCliques.clear();
	}
	
	public abstract Clique.Type getType();

	@SuppressWarnings("unchecked")
	public static CliqueSet_cascade create(String type, RetrievalEnvironment env, String[] queryTerms, Node domNode, int cascadeStage, String pruner_and_params)
			throws ConfigurationException {
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(env);
		Preconditions.checkNotNull(queryTerms);
		Preconditions.checkNotNull(domNode);

		try {
			Class<? extends CliqueSet_cascade> clz = (Class<? extends CliqueSet_cascade>) Class.forName(type);
			CliqueSet_cascade f = clz.newInstance();

			f.configure(env, queryTerms, domNode, cascadeStage, pruner_and_params);

			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to instantiate CliqueSet!");
		}
	}
}
