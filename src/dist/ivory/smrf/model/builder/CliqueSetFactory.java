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

import ivory.smrf.model.builder.CliqueSet.DEPENDENCE_TYPE;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 *
 */
public class CliqueSetFactory {

	public static CliqueSet getCliqueSet(RetrievalEnvironment env, String queryText, Node csNode) throws Exception {
		if( csNode == null ) {
			throw new Exception("Unable to generate a CliqueSet from a null node!");
		}
		
		// get clique set type
		String csType = XMLTools.getAttributeValue(csNode, "cliqueSet", null );
		if( csType == null ) {
			throw new Exception("A cliqueSet attribute must be specified in order to generate a clique set!");
		}

		// get normalized cs type
		String normCSType = csType.toLowerCase().trim();
		
		// build the clique set
		CliqueSet cliqueSet = null;
		if( "term".equals(normCSType) ) {
			boolean docDependent = XMLTools.getAttributeValue(csNode, "docDependent", true);
			
			cliqueSet = new TermCliqueSet( env, queryText, csNode, docDependent );
		}
		else if( "ordered".equals(normCSType) ) {
			String dependenceType = XMLTools.getAttributeValue(csNode, "dependence", "sequential");
			boolean docDependent = XMLTools.getAttributeValue(csNode, "docDependent", true);
			
			if( "sequential".equals(dependenceType) ) {
				cliqueSet = new OrderedCliqueSet( env, queryText, csNode, DEPENDENCE_TYPE.SEQUENTIAL, docDependent );
			}
			else if( "full".equals(dependenceType) ) {
				cliqueSet = new OrderedCliqueSet( env, queryText, csNode, DEPENDENCE_TYPE.FULL, docDependent );
			}
			else {
				throw new Exception( "Unrecognized dependence type -- " + dependenceType );
			}
		}
		else if( "unordered".equals(normCSType) ) {
			String dependenceType = XMLTools.getAttributeValue(csNode, "dependence", "sequential");
			boolean docDependent = XMLTools.getAttributeValue(csNode, "docDependent", true);
			
			if( "sequential".equals(dependenceType) ) {
				cliqueSet = new UnorderedCliqueSet( env, queryText, csNode, DEPENDENCE_TYPE.SEQUENTIAL, docDependent );
			}
			else if( "full".equals(dependenceType) ) {
				cliqueSet = new UnorderedCliqueSet( env, queryText, csNode, DEPENDENCE_TYPE.FULL, docDependent );
			}
			else {
				throw new Exception( "Unrecognized dependence type -- " + dependenceType );
			}
		}
		else {
			throw new Exception("Unrecognized cliqueSet type -- " + csType);
		}

		return cliqueSet;
	}
	
}