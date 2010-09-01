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

import ivory.smrf.model.MarkovRandomField;
import ivory.util.RetrievalEnvironment;

/**
 * @author Don Metzler
 *
 */
public abstract class MRFBuilder {
	
	/**
	 * indri query environment 
	 */
	protected RetrievalEnvironment mEnv = null;
	
	/**
	 * @param env
	 */
	public MRFBuilder(RetrievalEnvironment env) {
		mEnv = env;
	}
	
	/**
	 * @param queryText
	 * @return markov random field model based on query
	 * @throws Exception 
	 */
	public abstract MarkovRandomField buildMRF(String queryText) throws Exception;

}
