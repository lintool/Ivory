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

package ivory.smrf.model.potential;

import ivory.smrf.model.builder.ExpressionGenerator;
import ivory.smrf.model.builder.ExpressionGeneratorFactory;
import ivory.smrf.model.score.ScoringFunction;
import ivory.smrf.model.score.ScoringFunctionFactory;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 *
 */
public class PotentialFunctionFactory {

	public static PotentialFunction getPotentialFunction(RetrievalEnvironment env, String potentialType, Node potentialNode) throws Exception {
		if( potentialNode == null ) {
			throw new Exception("Unable to generate a PotentialFunction from a null node!");
		}
		
		// get normalized potential function type
		String normPotentialType = potentialType.toLowerCase().trim();
		
		// build the potential function
		PotentialFunction potential = null;
		if( "ivoryexpression".equals(normPotentialType) ) {
			// get expression generator type
			String generatorType = XMLTools.getAttributeValue(potentialNode, "generator", null );
			if( generatorType == null ) {
				throw new Exception("A generator attribute must be specified in order to generate a potential function!");
			}
			ExpressionGenerator generator = ExpressionGeneratorFactory.getExpressionGenerator( generatorType, potentialNode );

			// get score function
			String scoreFunctionType = XMLTools.getAttributeValue(potentialNode, "scoreFunction", null );
			if( scoreFunctionType == null ) {
				throw new Exception("A scoreFunction attribute must be specified in order to generate a potential function!");
			}			
			ScoringFunction scoreFunction = ScoringFunctionFactory.getScoringFunction( scoreFunctionType, potentialNode );

			// get the potential function
			potential = new IvoryExpressionPotential( env, scoreFunction, generator );
		}
		else {
			throw new Exception("Unrecognized potential type -- " + potentialType);
		}

		return potential;
	}

}
