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

package ivory.smrf.model.potential;

import ivory.smrf.model.DocumentNode;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GraphNode;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.util.List;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public class DocumentPotential extends PotentialFunction {

	private RetrievalEnvironment mEnv = null;
	private DocumentNode mDocNode = null;

	private String mType;

	@Override
	public void configure(RetrievalEnvironment env, Node domNode) throws Exception {
		String type = XMLTools.getAttributeValue(domNode, "type", "");
		if (type.equals("")) {
			throw new Exception("A DocumentPotential requires a type attribute!");
		}

		mEnv = env;
		mType = type;
	}

	public void initialize(List<GraphNode> nodes, GlobalEvidence globalEvidence) throws Exception {
		mDocNode = null;

		for (GraphNode node : nodes) {
			if (node.getType() == GraphNode.DOCUMENT && mDocNode != null) {
				throw new Exception(
						"Only one document node allowed in cliques associated with IndriExpressionPotential!");
			} else if (node.getType() == GraphNode.DOCUMENT) {
				mDocNode = (DocumentNode) node;
			} else if (node.getType() == GraphNode.TERM) {
				throw new Exception(
						"TermNodes are not allowed in cliques associated with QueryPotential!");
			}
		}

	}

	@Override
	public double computePotential() {
		return mEnv.getDocScore(mType, mDocNode.getDocno());
	}

	public int getNextCandidate() {
		return Integer.MAX_VALUE;
	}

	@Override
	public String toString() {
		return "<potential type=\"DocumentPotential\" />\n";
	}

	@Override
	public void reset() {
	}

	@Override
	public void setNextCandidate(int docid) {
	}

	@Override
	public double getMaxScore() {
		return Double.POSITIVE_INFINITY;
	}
}
