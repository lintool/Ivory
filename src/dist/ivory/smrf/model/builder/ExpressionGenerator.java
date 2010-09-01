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

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public abstract class ExpressionGenerator {

	public abstract void configure(Node domNode) throws Exception;

	/**
	 * @param terms
	 */
	public abstract String getExpression(String[] terms);

	@SuppressWarnings("unchecked")
	public static ExpressionGenerator create(String type, Node domNode) throws Exception {
		if (domNode == null) {
			throw new Exception("Unable to generate a ExpressionGenerator from a null node!");
		}

		try {
			Class<? extends ExpressionGenerator> clz = (Class<? extends ExpressionGenerator>) Class
					.forName(type);
			ExpressionGenerator f = clz.newInstance();

			f.configure(domNode);

			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error: Unable to instantiate ExpressionGenerator!");
		}
	}
}
