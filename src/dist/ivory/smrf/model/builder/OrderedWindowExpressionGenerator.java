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


/**
 * @author Don Metzler
 *
 */
public class OrderedWindowExpressionGenerator extends ExpressionGenerator {

	/**
	 * ordered window width 
	 */
	private int mWidth;
	
	/**
	 * default constructor 
	 */
	public OrderedWindowExpressionGenerator() {
		mWidth = 1;
	}
	
	/**
	 * @param width
	 */
	public OrderedWindowExpressionGenerator( int width ) {
		mWidth = width;
	}
	
	/* (non-Javadoc)
	 * @see edu.umass.cs.SMRF.model.expressiongenerator.ExpressionGenerator#getExpression(java.lang.String)
	 */
	@Override
	public String getExpression(String terms) {
		return "#od" + mWidth + "( " + terms + " )";
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "<expressiongenerator type=\"Ordered\" width=\"" + mWidth + "\"/>\n";
	}

}
