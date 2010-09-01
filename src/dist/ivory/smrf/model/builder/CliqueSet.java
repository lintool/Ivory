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

import java.util.List;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public abstract class CliqueSet {

	public static enum DEPENDENCE_TYPE {
		SEQUENTIAL, FULL;
	}

	public abstract void configure(RetrievalEnvironment env, String[] queryTerms, Node domNode)
			throws Exception;

	/**
	 * cliques that make up this clique set
	 */
	protected List<Clique> cliques = null;

	public List<Clique> getCliques() {
		return cliques;
	}

	public abstract String getType();

	@SuppressWarnings("unchecked")
	public static CliqueSet create(String type, RetrievalEnvironment env, String[] queryTerms,
			Node domNode) throws Exception {
		if (domNode == null) {
			throw new Exception("Unable to generate a CliqueSet from a null node!");
		}

		try {
			Class<? extends CliqueSet> clz = (Class<? extends CliqueSet>) Class.forName(type);
			CliqueSet f = clz.newInstance();

			f.configure(env, queryTerms, domNode);

			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error: Unable to instantiate CliqueSet!");
		}
	}
}
