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

package ivory.ltr;


/**
 * @author Don Metzler
 *
 */
public class QuotientFeature extends Feature {

	private static final long serialVersionUID = 1L;

	private int indexA;  // id of first feature
	private int indexB;  // id of second feature
	
	private String name; // feature name

	public QuotientFeature(int indexA, int indexB, String name) {
		this.indexA = indexA;
		this.indexB = indexB;
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see edu.isi.rankir.Feature#eval(float[])
	 */
	@Override
	public float eval(float[] fv) {
		if(fv[indexB] != 0.0) {
			return fv[indexA] / fv[indexB];
		}
		return 0;
	}

	/* (non-Javadoc)
	 * @see edu.isi.rankir.Feature#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

}
