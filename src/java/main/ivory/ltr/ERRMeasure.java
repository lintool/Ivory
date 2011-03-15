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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Don Metzler
 *
 */
public class ERRMeasure extends Measure {

	@Override
	public double evaluate(ScoreTable table) {
		String [] qids = table.getQids();
		float [] grades = table.getGrades();
		float [] scores = table.getScores();

		float err = 0;
		int numQueries = 0;
		
		String lastQid = null;
		List<ScoreGradePair> items = new ArrayList<ScoreGradePair>();
		for(int i = 0; i < qids.length; i++) {
			String curQid = qids[i];
			float curGrade = grades[i];
			float curScore = scores[i];
			
			if(lastQid == null || !lastQid.equals(curQid)) {
				if(lastQid != null) {
					err += _computeQueryERR(items);
					numQueries++;
				}
				
				lastQid = curQid;
				items = new ArrayList<ScoreGradePair>();
			}
			items.add(new ScoreGradePair(curScore, curGrade));
		}
		err += _computeQueryERR(items);
		numQueries++;

		if(numQueries == 0) {
			return 0;
		}
		
		return err / numQueries;
	}

	private double _computeQueryERR(List<ScoreGradePair> items) {
		// rank documents for this query
		Collections.sort(items, new ScoreComparator());
		
		// compute err for query
		float err = 0;
		float p = 1;
		for(int i = 0; i < items.size(); i++) {
			double g = (Math.pow(2.0, items.get(i).grade) - 1.0) / 16.0;
			err += g * p / (i + 1.0);
			p *= (1.0 - g);
		}
		
		return err;
	}

}
