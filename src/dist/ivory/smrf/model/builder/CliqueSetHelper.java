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
import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class CliqueSetHelper {

	public static List<Clique> getSequentialDependenceCliques(RetrievalEnvironment env, String queryText, org.w3c.dom.Node csNode, boolean docDependent) throws Exception {
		// clique set
		List<Clique> cliques = new ArrayList<Clique>();
		
		// tokenize the query
		String [] terms = TextTools.getTokens( queryText );

		// the document node
		DocumentNode docNode = new DocumentNode();
		
		// if there are no terms, then return an empty MRF
		if( terms.length == 0 ) {
			return cliques;
		}

		// default parameter associated with clique set
		Parameter parameter = new Parameter( Parameter.DEFAULT, 1.0 );
		
		// get potential type
		String potentialType = XMLTools.getAttributeValue(csNode, "potential", null);
		if( potentialType == null ) {
			throw new Exception("A potential attribute must be specified in order to generate a clique set!");
		}

		// if there is more than one term, then add appropriate cliques
		ArrayList<Node> cliqueNodes = null;
		Clique c = null;
		TermNode lastTermNode = null;

		for (String element : terms) {
			// term node
			TermNode termNode = new TermNode(element);
												
			// add sequential cliques
			if( lastTermNode != null ) {
				cliqueNodes = new ArrayList<Node>();
				if( docDependent ) {
					cliqueNodes.add( docNode );
				}
				cliqueNodes.add( lastTermNode );
				cliqueNodes.add( termNode );
				
				// get the potential function
				PotentialFunction potential = PotentialFunctionFactory.getPotentialFunction( env, potentialType, csNode );

				c = new Clique( cliqueNodes, potential, parameter );
				cliques.add( c );
			}
			
			// update last term node
			lastTermNode = termNode;
		}
		
		return cliques;
	}

	public static List<Clique> getFullDependenceCliques(RetrievalEnvironment env, String queryText, org.w3c.dom.Node csNode, boolean ordered, boolean docDependent) throws Exception {
		// clique set
		List<Clique> cliques = new ArrayList<Clique>();

		// tokenize the query
		String [] terms = TextTools.getTokens( queryText );

		// the document node
		DocumentNode docNode = new DocumentNode();
		
		// if there are no terms, then return an empty MRF
		if( terms.length == 0 ) {
			return cliques;
		}

		// default parameter associated with clique set
		Parameter parameter = new Parameter( Parameter.DEFAULT, 1.0 );
		
		// get potential type
		String potentialType = XMLTools.getAttributeValue(csNode, "potential", null);
		if( potentialType == null ) {
			throw new Exception("A potential attribute must be specified in order to generate a clique set!");
		}
		// if there is more than one term, then add appropriate cliques
		ArrayList<Node> cliqueNodes = null;
		Clique c = null;

		for( int i = 1; i <  Math.pow(2, terms.length); i++ ) {
			String binary = Integer.toBinaryString( i );
			int padding = terms.length - binary.length();
			for( int j = 0; j < padding; j++ ) {
				binary = "0" + binary;
			}
							
			boolean singleTerm = false;
			boolean contiguous = true;
				
			int firstOne = binary.indexOf( '1' );
			int lastOne = binary.lastIndexOf( '1' );			
			if( lastOne == firstOne ) {
				singleTerm = true;
			}
				
			for( int j = binary.indexOf( '1' ) + 1; j <= binary.lastIndexOf( '1' ) - 1; j++ ) {
				if( binary.charAt( j ) == '0' ) {
					contiguous = false;
					break;
				}
			}
			
			if( ordered && !singleTerm && contiguous ) {
				cliqueNodes = new ArrayList<Node>();
				if( docDependent ) {
					cliqueNodes.add( docNode );
				}
				for( int j = firstOne; j <= lastOne; j++ ) {
					TermNode termNode = new TermNode(terms[j]);
					cliqueNodes.add( termNode );
				}
				
				// get the potential function
				PotentialFunction potential = PotentialFunctionFactory.getPotentialFunction( env, potentialType, csNode );

				c = new Clique( cliqueNodes, potential, parameter );
				cliques.add( c );
			}
			else if( !ordered && !singleTerm && !contiguous ) {
				cliqueNodes = new ArrayList<Node>();
				if( docDependent ) {
					cliqueNodes.add( docNode );
				}
				for( int j = 0; j < binary.length(); j++ ) {
					if( binary.charAt( j ) == '1' ) {
						TermNode termNode = new TermNode(terms[j]);
						cliqueNodes.add( termNode );
					}
				}
				
				// get the potential function
				PotentialFunction potential = PotentialFunctionFactory.getPotentialFunction( env, potentialType, csNode );

				c = new Clique( cliqueNodes, potential, parameter );
				cliques.add( c );
			}
		}
		
		return cliques;
	}
	
}