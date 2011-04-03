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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Don Metzler
 *
 */
public class NDCGMeasure extends Measure {

	private final Map<String, Double> idealGainLookup = new HashMap<String,Double>(); // query id -> ideal gain mapping

	public NDCGMeasure() {
		idealGainLookup.clear();
	}
	
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
				//System.out.println("Computing ERR for: " + lastQid);
				if(lastQid != null) {
					err += computeQueryNDCG(curQid, items);
					numQueries++;
				}
				
				lastQid = curQid;
				items = new ArrayList<ScoreGradePair>();
			}
			items.add(new ScoreGradePair(curScore, curGrade));
		}
		err += computeQueryNDCG(qids[qids.length-1], items);
		numQueries++;

		if(numQueries == 0) {
			return 0;
		}
		
		return err / numQueries;
	}

	private double computeQueryNDCG(String qid, List<ScoreGradePair> items) {
		// get ideal gain
		Double idealGain = idealGainLookup.get(qid);
		if(idealGain == null) {
			List<ScoreGradePair> newList = new ArrayList<ScoreGradePair>(items);
			Collections.sort(newList, new GradeComparator());
			
			double idcg = 0.0;
			for(int i = 0; i < newList.size(); i++) {
				idcg += (Math.pow(2.0, newList.get(i).grade) - 1.0) / Math.log(i + 2.0);
			}
			
			idealGain = idcg;
			idealGainLookup.put(qid, idealGain);
		}

		// rank documents for this query
		Collections.sort(items, new ScoreComparator());
		
		// compute err for query
		double dcg = 0.0;
		for(int i = 0; i < items.size(); i++) {
			dcg += (Math.pow(2.0, items.get(i).grade) - 1.0) / Math.log(i + 2.0);
		}
		
		if(idealGain == 0.0) {
			return 0.0;
		}
		
		return dcg / idealGain;
	}

}
