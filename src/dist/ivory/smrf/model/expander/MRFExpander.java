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

package ivory.smrf.model.expander;

import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.retrieval.Accumulator;

import java.util.Set;

/**
 * @author Don Metzler
 *
 */
public abstract class MRFExpander {

	/**
	 * stopword list 
	 */
	protected Set<String> mStopwords = null;

	/**
	 * Maximum number of candidates to consider for expansion
	 * non-positive numbers result in *all* candidates being considered 
	 */
	protected int mMaxCandidates = 0;

	/**
	 * @param mrf
	 * @param results
	 */
	public abstract MarkovRandomField getExpandedMRF( MarkovRandomField mrf, Accumulator [] results ) throws Exception;
	
	/**
	 * @param words list of words to ignore when constructing expansion concepts
	 */
	public void setStopwordList( Set<String> words ) {
		mStopwords = words;
	}

	/**
	 * @param maxCandidates
	 */
	public void setMaxCandidates(int maxCandidates) {
		mMaxCandidates = maxCandidates;
	}
}
