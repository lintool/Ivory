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
 */
public class VocabFrequencyPair implements Entry<String, Integer>, Comparable<VocabFrequencyPair> {
	private String key;
	private Integer value;

	public VocabFrequencyPair(String key, Integer value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public Integer getValue() {
		return value;
	}

	public Integer setValue(Integer value) {
		this.value = value;
		return value;
	}

	public int compareTo(VocabFrequencyPair o) {
		return o.getValue().compareTo(this.getValue());
	}

	@Override
	public String toString() {
		return key + "\t" + value;
	}
}
