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

import java.util.Comparator;

/**
 * @author Don Metzler
 */
public class MaxScoreCliqueSorter implements Comparator<Clique> {

	public int compare(Clique a, Clique b) {
		double maxScoreA = a.getMaxScore();
		double maxScoreB = b.getMaxScore();
		if (maxScoreA == maxScoreB) {
			return 0;
		} else if (maxScoreA < maxScoreB) {
			return 1;
		} else {
			return -1;
		}
	}

}
