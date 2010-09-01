/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

import java.util.ArrayList;

/**
 * @author Don Metzler
 *
 */
public class UnorderedCliqueSet extends CliqueSet {

	public UnorderedCliqueSet(RetrievalEnvironment env, String queryText, org.w3c.dom.Node csNode, DEPENDENCE_TYPE type, boolean docDependent) throws Exception {
		// initialize clique set
		cliques = new ArrayList<Clique>();

		// generate clique set
		if( type == DEPENDENCE_TYPE.SEQUENTIAL ) {
			//cliques = CliqueSetHelper.getSequentialDependenceCliques(queryText, potential, docDependent);
			throw new Exception("Unsupported operation -- there are no unordered cliques within a sequentially dependent graph.");
		}
		else if( type == DEPENDENCE_TYPE.FULL ) {
			cliques = CliqueSetHelper.getFullDependenceCliques(env, queryText, csNode, false, docDependent);
		}
		else {
			throw new Exception("Unrecognized UnorderedCliqueSet type -- " + type);
		}
	}

}
