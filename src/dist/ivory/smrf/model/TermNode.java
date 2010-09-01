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

package ivory.smrf.model;

/**
 * @author Don Metzler
 *
 */
public class TermNode extends Node {
	
	protected String mTerm = null;
	
	/**
	 * @param term
	 */
	public TermNode(String term) {
		mTerm = term;
	}
	
	public String getTerm() {
		return mTerm;
	}

	/**
	 * @param term
	 */
	public void setTerm(String term) {
		mTerm = term;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public TermNode clone() {
		return new TermNode(mTerm);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "<node type=\"Term\" term=\"" + mTerm + "\" />\n";
	}
}
