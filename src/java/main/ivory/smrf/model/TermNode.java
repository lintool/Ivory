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
 * A term node in a MRF.
 *
 * @author Don Metzler
 *
 */
public class TermNode extends GraphNode {
	private String term = null;

	/**
	 * Creates a <code>TermNode</code> for a particular term.
	 */
	public TermNode(String term) {
		this.term = term;
	}

	/**
	 * Returns the term associated with this node.
	 */
	public String getTerm() {
		return term;
	}

	/**
	 * Sets the term associated with this node.
	 */
	public void setTerm(String term) {
		this.term = term;
	}

	/**
	 * Clones this node.
	 */
	public TermNode clone() {
		return new TermNode(term);
	}

	@Override
	public Type getType() {
		return Type.TERM;
	}

	@Override
	public String toString() {
		return "<node type=\"Term\" term=\"" + term + "\" />\n";
	}
}
