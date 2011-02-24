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
 * Object encapsulating collection-level term evidence (df and cf).
 *
 * @author Don Metzler
 *
 */
public class GlobalTermEvidence {
	private int df;
	private long cf;

	public GlobalTermEvidence() {
		df = 0;
		cf = 0L;
	}

	public GlobalTermEvidence(int df, long cf) {
		this.df = df;
		this.cf = cf;
	}

	public void set(int df, long cf) {
		this.df = df;
		this.cf = cf;
	}

	public int getDf() {
		return df;
	}

	public long getCf() {
		return cf;
	}
}
