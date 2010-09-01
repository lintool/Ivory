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

import ivory.smrf.model.Parameter;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import org.w3c.dom.Attr;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * @author Don Metzler
 *
 */
public class MRFBuilderFactory {

	public static MRFBuilder getBuilder(RetrievalEnvironment env, Node model) throws Exception {
		if( model == null ) {
			throw new Exception("Unable to generate a MRFBuilder from a null node!");
		}
		
		// get model type
		String modelType = XMLTools.getAttributeValue(model, "type", null );
		if( modelType == null ) {
			throw new Exception("Model type must be specified!");
		}

		// get normalized model type
		String normModelType = modelType.toLowerCase().trim();
		
		// build the builder
		MRFBuilder builder = null;
		if( "fullindependence".equals(normModelType) ) {
			// smoothing parameter
			double mu = XMLTools.getAttributeValue( model, "mu", 2500.0 );

			// parameter ids
			String termParamID = XMLTools.getAttributeValue( model, "termParamID", Parameter.TERM_ID );

			// clique weights
			double termWt = XMLTools.getAttributeValue( model, termParamID, 1.0 );
			
			// construct term feature node
			Node termNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap termAttributes = termNode.getAttributes();
			termAttributes.setNamedItem( getAttribute( "id", termParamID, model ) );
			termAttributes.setNamedItem( getAttribute( "weight", termWt+"", model ) );
			termAttributes.setNamedItem( getAttribute( "cliqueSet", "term", model ) );
			termAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			termAttributes.setNamedItem( getAttribute( "generator", "Term", model ) );
			termAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			termAttributes.setNamedItem( getAttribute( "mu", mu+"", model ) );			
			model.appendChild( termNode );
						
			// change the model type to 'Feature'
			NamedNodeMap modelAttributes = model.getAttributes();
			Node modelTypeNode = modelAttributes.getNamedItem("type");
			modelTypeNode.setNodeValue("Feature");
			
			// get the builder
			builder = new FeatureBasedMRFBuilder( env, model );			
		}
		else if( "sequentialdependence".equals(normModelType) ) {
			// smoothing parameters
			double termMu = XMLTools.getAttributeValue( model, "termMu", 2500.0 );
			double windowMu = XMLTools.getAttributeValue( model, "windowMu", 3500.0 );

			// parameter ids
			String termParamID = XMLTools.getAttributeValue( model, "termParamID", Parameter.TERM_ID );
			String orderedParamID = XMLTools.getAttributeValue( model, "orderedParamID", Parameter.ORDERED_ID );
			String unorderedParamID = XMLTools.getAttributeValue( model, "unorderedParamID", Parameter.UNORDERED_ID );
			
			// clique weights
			double termWt = XMLTools.getAttributeValue( model, termParamID, 0.8 );
			double orderedWt = XMLTools.getAttributeValue( model, orderedParamID, 0.1 );
			double unorderedWt = XMLTools.getAttributeValue( model, unorderedParamID, 0.1 );
			
			// construct term feature node
			Node termNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap termAttributes = termNode.getAttributes();
			termAttributes.setNamedItem( getAttribute( "id", termParamID, model ) );
			termAttributes.setNamedItem( getAttribute( "weight", termWt+"", model ) );
			termAttributes.setNamedItem( getAttribute( "cliqueSet", "term", model ) );
			termAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			termAttributes.setNamedItem( getAttribute( "generator", "Term", model ) );
			termAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			termAttributes.setNamedItem( getAttribute( "mu", termMu+"", model ) );			
			model.appendChild( termNode );
						
			// construct ordered window feature node
			Node orderedNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap orderedAttributes = orderedNode.getAttributes();
			orderedAttributes.setNamedItem( getAttribute( "id", orderedParamID, model ) );
			orderedAttributes.setNamedItem( getAttribute( "weight", orderedWt+"", model ) );
			orderedAttributes.setNamedItem( getAttribute( "cliqueSet", "ordered", model ) );
			orderedAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			orderedAttributes.setNamedItem( getAttribute( "generator", "OrderedWindow", model ) );
			orderedAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			orderedAttributes.setNamedItem( getAttribute( "mu", windowMu+"", model ) );			
			model.appendChild( orderedNode );
			
			// construct unordered window feature node
			Node unorderedNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap unorderedAttributes = unorderedNode.getAttributes();
			unorderedAttributes.setNamedItem( getAttribute( "id", unorderedParamID, model ) );
			unorderedAttributes.setNamedItem( getAttribute( "weight", unorderedWt+"", model ) );
			unorderedAttributes.setNamedItem( getAttribute( "cliqueSet", "ordered", model ) );
			unorderedAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			unorderedAttributes.setNamedItem( getAttribute( "generator", "UnorderedWindow", model ) );
			unorderedAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			unorderedAttributes.setNamedItem( getAttribute( "mu", windowMu+"", model ) );			
			model.appendChild( unorderedNode );
			
			// change the model type to 'Feature'
			NamedNodeMap modelAttributes = model.getAttributes();
			Node modelTypeNode = modelAttributes.getNamedItem("type");
			modelTypeNode.setNodeValue("Feature");
			
			// get the builder
			builder = new FeatureBasedMRFBuilder( env, model );
		}
		else if( "fulldependence".equals(normModelType) ) {
			// smoothing parameters
			double termMu = XMLTools.getAttributeValue( model, "termMu", 2500.0 );
			double windowMu = XMLTools.getAttributeValue( model, "windowMu", 3500.0 );

			// parameter ids
			String termParamID = XMLTools.getAttributeValue( model, "termParamID", Parameter.TERM_ID );
			String orderedParamID = XMLTools.getAttributeValue( model, "orderedParamID", Parameter.ORDERED_ID );
			String unorderedParamID = XMLTools.getAttributeValue( model, "unorderedParamID", Parameter.UNORDERED_ID );
			
			// clique weights
			double termWt = XMLTools.getAttributeValue( model, termParamID, 0.8 );
			double orderedWt = XMLTools.getAttributeValue( model, orderedParamID, 0.1 );
			double unorderedWt = XMLTools.getAttributeValue( model, unorderedParamID, 0.1 );
			
			// construct term feature node
			Node termNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap termAttributes = termNode.getAttributes();
			termAttributes.setNamedItem( getAttribute( "id", termParamID, model ) );
			termAttributes.setNamedItem( getAttribute( "weight", termWt+"", model ) );
			termAttributes.setNamedItem( getAttribute( "cliqueSet", "term", model ) );
			termAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			termAttributes.setNamedItem( getAttribute( "generator", "Term", model ) );
			termAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			termAttributes.setNamedItem( getAttribute( "mu", termMu+"", model ) );			
			model.appendChild( termNode );
						
			// construct ordered window feature node
			Node orderedNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap orderedAttributes = orderedNode.getAttributes();
			orderedAttributes.setNamedItem( getAttribute( "id", orderedParamID, model ) );
			orderedAttributes.setNamedItem( getAttribute( "weight", orderedWt+"", model ) );
			orderedAttributes.setNamedItem( getAttribute( "cliqueSet", "ordered", model ) );
			orderedAttributes.setNamedItem( getAttribute( "dependence", "full", model ) );
			orderedAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			orderedAttributes.setNamedItem( getAttribute( "generator", "OrderedWindow", model ) );
			orderedAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			orderedAttributes.setNamedItem( getAttribute( "mu", windowMu+"", model ) );			
			model.appendChild( orderedNode );
			
			// construct unordered window feature node
			Node unorderedONode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap unorderedOAttributes = unorderedONode.getAttributes();
			unorderedOAttributes.setNamedItem( getAttribute( "id", unorderedParamID, model ) );
			unorderedOAttributes.setNamedItem( getAttribute( "weight", unorderedWt+"", model ) );
			unorderedOAttributes.setNamedItem( getAttribute( "cliqueSet", "ordered", model ) );
			unorderedOAttributes.setNamedItem( getAttribute( "dependence", "full", model ) );
			unorderedOAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			unorderedOAttributes.setNamedItem( getAttribute( "generator", "UnorderedWindow", model ) );
			unorderedOAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			unorderedOAttributes.setNamedItem( getAttribute( "mu", windowMu+"", model ) );			
			model.appendChild( unorderedONode );
			
			// construct unordered window feature node
			Node unorderedUNode = model.getOwnerDocument().createElement( "feature" );			
			NamedNodeMap unorderedUAttributes = unorderedUNode.getAttributes();
			unorderedUAttributes.setNamedItem( getAttribute( "id", unorderedParamID, model ) );
			unorderedUAttributes.setNamedItem( getAttribute( "weight", unorderedWt+"", model ) );
			unorderedUAttributes.setNamedItem( getAttribute( "cliqueSet", "unordered", model ) );
			unorderedUAttributes.setNamedItem( getAttribute( "dependence", "full", model ) );
			unorderedUAttributes.setNamedItem( getAttribute( "potential", "IvoryExpression", model ) );
			unorderedUAttributes.setNamedItem( getAttribute( "generator", "UnorderedWindow", model ) );
			unorderedUAttributes.setNamedItem( getAttribute( "scoreFunction", "Dirichlet", model ) );			
			unorderedUAttributes.setNamedItem( getAttribute( "mu", windowMu+"", model ) );			
			model.appendChild( unorderedUNode );
			
			// change the model type to 'Feature'
			NamedNodeMap modelAttributes = model.getAttributes();
			Node modelTypeNode = modelAttributes.getNamedItem("type");
			modelTypeNode.setNodeValue("Feature");
			
			// get the builder
			builder = new FeatureBasedMRFBuilder( env, model );			
		}
		else if( "feature".equals(normModelType) ) {
			// get the builder
			builder = new FeatureBasedMRFBuilder( env, model );
		}
		else {
			throw new Exception("Unrecognized model type -- " + modelType);
		}
		
		return builder;
	}

	/**
	 * @param attribute
	 * @param value
	 * @param model
	 */
	public synchronized static Node getAttribute( String attribute, String value, Node model ) {		
		Attr attr = model.getOwnerDocument().createAttribute( attribute );
		attr.setValue( value );
		return attr;
	}
	
}
