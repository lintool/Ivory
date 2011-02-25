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

public class Expression {
	
	public static enum Type { OD, UW, TERM, XSameBin, XOADJ, XUADJ }
	
	private final Type type;
	private final String[] terms;
	private final int window;
	
	public Expression(Type type, int window, String[] terms) {
		this.type = type;
		this.window = window;
		this.terms = terms;
	}
	
	public Expression(String term) {
		this.type = Type.TERM;
		this.window = -1;
		this.terms = new String[] { term };
	}
	
	public String[] getTerms() {
		return terms;
	}
	
	public int getWindow() {
		return window;
	}
	
	public Type getType() {
		return type;
	}	
}
