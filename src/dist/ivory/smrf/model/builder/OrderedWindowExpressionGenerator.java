/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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
public class OrderedWindowExpressionGenerator extends ExpressionGenerator {

	/**
	 * ordered window width
	 */
	private int mWidth;

	@Override
	public void configure(Node domNode) throws Exception {
		mWidth = XMLTools.getAttributeValue(domNode, "width", 1);
	}

	@Override
	public String getExpression(String[] terms) {
		return "#od" + mWidth + "( " + join(terms, " ") + " )";
	}

	@Override
	public String toString() {
		return "<expressiongenerator type=\"Ordered\" width=\"" + mWidth + "\"/>\n";
	}

	private static String join(String[] terms, String sep) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < terms.length; i++) {
			sb.append(terms[i]);
			if (i < terms.length - 1)
				sb.append(sep);
		}

		return sb.toString();
	}

}
