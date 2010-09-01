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
import ivory.smrf.model.MarkovRandomField;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.List;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author Don Metzler
 *
 */
public class FeatureBasedMRFBuilder extends MRFBuilder {

	/**
	 * XML specification of features  
	 */
	private Node mModel = null;
	
	/**
	 * @param env
	 * @param model
	 */
	public FeatureBasedMRFBuilder( RetrievalEnvironment env, Node model ) {
		super(env);
		mModel = model;
	}
	
	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.MRFBuilder#buildMRF(java.lang.String)
	 */
	@Override
	public MarkovRandomField buildMRF(String queryText) throws Exception {
		// this is the MRF we're building
		MarkovRandomField mrf = new MarkovRandomField(queryText, mEnv);
		
		// construct MRF feature by feature
		NodeList children = mModel.getChildNodes();
		for( int i = 0; i < children.getLength(); i++ ) {
			Node child = children.item( i );
			if( "feature".equals( child.getNodeName() ) ) {
				// get the feature id
				String featureID = XMLTools.getAttributeValue(child, "id", null);
				if( featureID == null ) {
					throw new Exception("Each feature must specify an id attribute!");
				}
				
				// get feature weight
				double weight = XMLTools.getAttributeValue(child, "weight", -1.0);
				if( weight < 0.0 ) {
					throw new Exception("Each feature must specify a non-negative weight!");
				}
				
				// get feature scale factor
				double scaleFactor = XMLTools.getAttributeValue(child, "scaleFactor", 1.0);
				
				// construct the clique set
				CliqueSet cliqueSet = CliqueSetFactory.getCliqueSet( mEnv, queryText, child );
				
				// get cliques from clique set
				List<Clique> cliques = cliqueSet.getCliques();
				
				// add cliques to MRF
				for( Clique c : cliques ) {
					c.setParameterID( featureID );
					c.setWeight( weight );
					c.setScaleFactor( scaleFactor );
					mrf.addClique( c );
				}
			}
		}

		return mrf;
	}

}
