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

import ivory.exception.ConfigurationException;
import ivory.smrf.model.builder.Expression.Type;
import ivory.util.XMLTools;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 */
public class OrderedWindowExpressionGenerator extends ExpressionGenerator {
	private int width;  // With of the ordered window.

	@Override
	public void configure(Node domNode) throws ConfigurationException {
		Preconditions.checkNotNull(domNode);
		width = XMLTools.getAttributeValue(domNode, "width", 1);
	}

	@Override
	public Expression getExpression(String[] terms) {
		Preconditions.checkNotNull(terms);
		return new Expression(Type.OD, width, terms);
	}

	@Override
	public String toString() {
		return "<expressiongenerator type=\"Ordered\" width=\"" + width + "\"/>\n";
	}
}
