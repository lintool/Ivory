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

package ivory.smrf.model.potential;

import ivory.smrf.model.GlobalEvidence;
import ivory.smrf.model.GraphNode;
import ivory.util.RetrievalEnvironment;

import java.util.List;

import org.w3c.dom.Node;

/**
 * @author Don Metzler
 * 
 */
public abstract class PotentialFunction {

	public abstract void configure(RetrievalEnvironment env, Node domNode) throws Exception;

	public abstract void initialize(List<GraphNode> nodes, GlobalEvidence evidence)
			throws Exception;

	/**
	 * @return potential
	 * @throws Exception
	 */
	public double getPotential() {
		return computePotential();
	}

	/**
	 * @return potential
	 */
	public abstract double computePotential();

	/**
	 * @return next candidate
	 */
	public abstract int getNextCandidate();

	public abstract void reset();

	public abstract double getMaxScore();

	/**
	 * @param docno
	 */
	public abstract void setNextCandidate(int docno);

	@SuppressWarnings("unchecked")
	public static PotentialFunction create(RetrievalEnvironment env, String type, Node functionNode)
			throws Exception {
		if (functionNode == null) {
			throw new Exception("Unable to generate a PotentialFunction from a null node!");
		}

		try {
			Class<? extends PotentialFunction> clz = (Class<? extends PotentialFunction>) Class
					.forName(type);
			PotentialFunction f = clz.newInstance();

			f.configure(env, functionNode);

			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error: Unable to instantiate scoring function!");
		}
	}
}
