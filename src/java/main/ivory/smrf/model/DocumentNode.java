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

package ivory.smrf.model;

/**
 * A document node in a MRF.
 *
 * @author Don Metzler
 *
 */
public class DocumentNode extends GraphNode {
	private int docno = 0;

	/**
	 * Returns the docno associated with this node.
	 */
	public int getDocno() {
		return docno;
	}

	/**
	 * Sets the docno associated with this node.
	 */
	public void setDocno(int docno) {
		this.docno = docno;
	}

	@Override
	public Type getType() {
		return Type.DOCUMENT;
	}

	@Override
	public String toString() {
		return new StringBuilder("<node type=\"Document\" docid=\"").append(docno).append("\" />\n").toString();
	}
}
