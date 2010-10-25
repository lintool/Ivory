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

package ivory.smrf.model.score;

import ivory.exception.ConfigurationException;
import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GlobalTermEvidence;

import org.w3c.dom.Node;

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 * 
 */
public abstract class ScoringFunction {

	public void configure(Node domNode) {
	}

	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
	}

	public abstract float getScore(int tf, int docLen);

	public float getMaxScore() {
		return Float.POSITIVE_INFINITY;
	}

	@SuppressWarnings("unchecked")
	public static ScoringFunction create(String functionType, Node functionNode)
			throws ConfigurationException {
		Preconditions.checkNotNull(functionType);
		Preconditions.checkNotNull(functionNode);

		try {
			Class<? extends ScoringFunction> clz = (Class<? extends ScoringFunction>) Class.forName(functionType);
			ScoringFunction f = clz.newInstance();

			f.configure(functionNode);

			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConfigurationException("Unable to instantiate scoring function \"" + functionType + "\"!");
		}
	}
}
