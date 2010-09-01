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

package ivory.smrf.model.builder;

import ivory.smrf.model.Clique;

import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class CliqueSet {
	
	public static enum DEPENDENCE_TYPE {
		SEQUENTIAL, FULL;
	}

	/**
	 * cliques that make up this clique set 
	 */
	protected List<Clique> cliques = null;
	
	public List<Clique> getCliques() {
		return cliques;
	}

}
