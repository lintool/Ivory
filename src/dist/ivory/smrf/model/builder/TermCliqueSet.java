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
import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.Node;
import ivory.smrf.model.Parameter;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.potential.PotentialFunction;
import ivory.smrf.model.potential.PotentialFunctionFactory;
import ivory.util.RetrievalEnvironment;
import ivory.util.TextTools;
import ivory.util.XMLTools;

import java.util.ArrayList;

/**
 * @author Don Metzler
 *
 */
public class TermCliqueSet extends CliqueSet {

	/**
	 * @param env
	 * @param queryText
	 */
	public TermCliqueSet(RetrievalEnvironment env, String queryText, org.w3c.dom.Node csNode, boolean docDependent ) throws Exception {
		// initialize clique set
		cliques = new ArrayList<Clique>();
		
		// tokenize the query
		String [] terms = TextTools.getTokens( queryText );

		// the document node
		DocumentNode docNode = new DocumentNode();
	
		// default parameter associated with clique set
		Parameter termParameter = new Parameter( Parameter.DEFAULT, 1.0 );
		
		// get potential type
		String potentialType = XMLTools.getAttributeValue(csNode, "potential", null);
		if( potentialType == null ) {
			throw new Exception("A potential attribute must be specified in order to generate a clique set!");
		}
		
		// add clique for each query term
		for (String element : terms) {
			// add term node
			TermNode termNode = new TermNode(element);
			
			// add document/term clique
			ArrayList<Node> cliqueNodes = new ArrayList<Node>();
			if( docDependent ) {
				cliqueNodes.add( docNode );
			}
			cliqueNodes.add( termNode );
			
			// get the potential function
			PotentialFunction potential = PotentialFunctionFactory.getPotentialFunction( env, potentialType, csNode );

			Clique c = new Clique( cliqueNodes, potential, termParameter );
			cliques.add( c );
		}
	}
}
