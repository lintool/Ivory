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

package ivory.smrf.model.score;

import ivory.util.XMLTools;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 *
 */
public class ScoringFunctionFactory {

	public static ScoringFunction getScoringFunction(String functionType, Node functionNode) throws Exception {
		if( functionNode == null ) {
			throw new Exception("Unable to generate a ScoringFunction from a null node!");
		}
		
		// get normalized scoring function type
		String normFunctionType = functionType.toLowerCase().trim();
		
		// build the scoring function
		ScoringFunction function = null;
		if( "dirichlet".equals( normFunctionType ) ) {
			double mu = XMLTools.getAttributeValue(functionNode, "mu", 2500.0 );
			function = new DirichletScoringFunction( mu );
		}
		else if( "jelinekmercer".equals( normFunctionType ) ) {
			double lambda = XMLTools.getAttributeValue(functionNode, "lambda", 0.5 );
			function = new JelinekMercerScoringFunction( lambda );
		}
		else if( "bm25".equals( normFunctionType ) ) {
			double k1 = XMLTools.getAttributeValue(functionNode, "k1", 1.2 );
			double b = XMLTools.getAttributeValue(functionNode, "b", 0.75 );
			String idfType = XMLTools.getAttributeValue(functionNode, "idf", "okapi" );
			function = new BM25ScoringFunction( k1, b, idfType );
		}
		else if( "f2exp".equals( normFunctionType ) ) {
			double s = XMLTools.getAttributeValue(functionNode, "s", 0.5 );
			double k = XMLTools.getAttributeValue(functionNode, "k", 1.0 );
			function = new F2EXPScoringFunction( s, k );
		}
		else if( "binarytf".equals( normFunctionType ) ) {
			function = new BinaryTFScoringFunction();
		}
		else if( "tf".equals( normFunctionType ) ) {
			function = new TFScoringFunction();
		}
		else if( "cf".equals( normFunctionType ) ) {
			function = new CFScoringFunction();
		}
		else if( "doclen".equals( normFunctionType ) ) {
			function = new DocLenScoringFunction();
		}
		else if( "collen".equals( normFunctionType ) ) {
			function = new ColLenScoringFunction();
		}
		else {
			throw new Exception("Unrecognized scoring function type -- " + functionType );
		}
		
		return function;
	}
}
