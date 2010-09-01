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

import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 *
 */
public class ExpressionGeneratorFactory {

	public static ExpressionGenerator getExpressionGenerator(String generatorType, Node generatorNode) throws Exception {
		if( generatorNode == null ) {
			throw new Exception("Unable to generate an ExpressionGenerator from a null node!");
		}
		
		// get normalized expression generator type
		String normGeneratorType = generatorType.toLowerCase().trim();
		
		// build the expression generator
		ExpressionGenerator generator = null;
		if( "term".equals(normGeneratorType) ) {
			generator = new TermExpressionGenerator();
		}
		else if( "orderedwindow".equals(normGeneratorType) ) {
			int width = XMLTools.getAttributeValue(generatorNode, "width", 1);
			generator = new OrderedWindowExpressionGenerator( width );
		}
		else if( "unorderedwindow".equals(normGeneratorType) ) {
			int width = XMLTools.getAttributeValue(generatorNode, "width", 4);
			generator = new UnorderedWindowExpressionGenerator( width );				
		}
		else {
			throw new Exception("Unrecognized generator type -- " + generatorType );
		}

		return generator;
	}
	
}
