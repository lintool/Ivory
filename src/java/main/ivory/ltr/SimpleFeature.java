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
public class SimpleFeature extends Feature {

	private static final long serialVersionUID = -7499607924780674760L;

	private int index;   // feature number
	private String name; // feature name
	
	public SimpleFeature(int index, String name) {
		this.index = index;
		this.name = name;
	}
	
	/* (non-Javadoc)
	 * @see edu.isi.rankir.Feature#eval(float[])
	 */
	@Override
	public float eval(float[] fv) {
		return fv[index];
	}

	/* (non-Javadoc)
	 * @see edu.isi.rankir.Feature#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

}
