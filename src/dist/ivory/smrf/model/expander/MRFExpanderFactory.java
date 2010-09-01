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

package ivory.smrf.model.expander;

import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 *
 */
public class MRFExpanderFactory {

	/**
	 * @param env
	 */
	public static MRFExpander getExpander( RetrievalEnvironment env, Node model ) throws Exception {
		if( model == null ) {
			throw new Exception("Unable to generate a MRFExpander from a null node!");
		}
		
		// get model type
		String expanderType = XMLTools.getAttributeValue(model, "type", null );
		if( expanderType == null ) {
			throw new Exception("Expander type must be specified!");
		}

		// get normalized model type
		String normExpanderType = expanderType.toLowerCase().trim();
		
		// build the expander
		MRFExpander expander = null;

		if(expander == null) {
			throw new Exception("Unrecognized expander type -- " + expanderType);
		}

		return expander;
	}
	
}
