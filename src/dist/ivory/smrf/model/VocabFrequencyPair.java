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

import java.util.Map.Entry;

/**
 * @author Don Metzler
 *
 */
public class VocabFrequencyPair implements Entry<String, Integer>, Comparable<VocabFrequencyPair> {

	/**
	 * entry key
	 */
	private String mKey = null;
	
	/**
	 * entry value
	 */
	private Integer mValue = null;
	
	/**
	 * @param key
	 * @param value
	 */
	public VocabFrequencyPair(String key, Integer value) {
		mKey = key;
		mValue = value;
	}
		
	/* (non-Javadoc)
	 * @see java.util.Map$Entry#getKey()
	 */
	public String getKey() {
		return mKey;
	}

	/* (non-Javadoc)
	 * @see java.util.Map$Entry#getValue()
	 */
	public Integer getValue() {
		return mValue;
	}

	/* (non-Javadoc)
	 * @see java.util.Map$Entry#setValue(java.lang.Object)
	 */
	public Integer setValue(Integer value) {
		mValue = value;
		return mValue;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(VocabFrequencyPair o) {
		return o.getValue().compareTo(this.getValue());
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return mKey + "\t" + mValue;
	}
}
