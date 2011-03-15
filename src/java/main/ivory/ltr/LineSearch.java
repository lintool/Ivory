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


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Don Metzler
 * 
 */
public class LineSearch implements Callable<Map<Feature,AlphaMeasurePair>> {

	private static final double SCALE_FACTOR = 0.01;
	private static final int MAX_STEPS = 5;
	private static final double MULTIPLIER = Math.pow((1.0 / SCALE_FACTOR), (1.0 / MAX_STEPS));
	
	private Model model;             // model
	private List<Feature> features;  // features
	private ScoreTable scores;       // score table (instances, scores)
	private Measure measure;         // evaluation metric

	public LineSearch(Model model, List<Feature> features, ScoreTable scoreTable, Measure evaluator) {
		this.model = model;
		this.features = features;
		this.scores = scoreTable;
		this.measure = evaluator;
	}

	public Map<Feature,AlphaMeasurePair> call() throws Exception {
		Map<Feature,AlphaMeasurePair> results = new HashMap<Feature,AlphaMeasurePair>();
		
		for (Feature f : features) {
			results.put(f, lineSearch(model, f, scores, measure));
		}
		
		return results;
	}

	public static AlphaMeasurePair lineSearch(Model model, Feature feature, ScoreTable scores, Measure measure) {
		AlphaMeasurePair bestAlphaMeasure;

		if (model.getNumFeatures() == 0) {
			ScoreTable newScoreTable = scores.translate(feature, 1.0, 1.0);
			double m = measure.evaluate(newScoreTable);
		    System.err.println("Feature: " + feature.getName() + ", Measure: " + m);
			return new AlphaMeasurePair(1.0, m);
		}

		bestAlphaMeasure = new AlphaMeasurePair(0.0, measure.evaluate(scores));

		double alpha;
		double maxWeight = model.getMaxWeight();

		alpha = maxWeight * SCALE_FACTOR;
		for (int iter = 0; iter < MAX_STEPS; iter++) {
			ScoreTable newScoreTable = scores.translate(feature, alpha, 1.0);
			double m = measure.evaluate(newScoreTable);
			// System.err.println("Alpha: " + alpha + ", ERR: " + measure);
			if (m < bestAlphaMeasure.alpha) {
				break;
			}
			if (m > bestAlphaMeasure.measure) {
				bestAlphaMeasure.alpha = alpha;
				bestAlphaMeasure.measure = m;
			}
			alpha *= MULTIPLIER;
		}
		if(bestAlphaMeasure.alpha != 0.0) {
			System.err.println("Feature: " + feature.getName() + ", Measure: " + bestAlphaMeasure.measure);
			return bestAlphaMeasure;
		}

		alpha = maxWeight * SCALE_FACTOR;
		for (int iter = 0; iter < MAX_STEPS; iter++) {
			ScoreTable newScoreTable = scores.translate(feature, -alpha, 1.0);
			double m = measure.evaluate(newScoreTable);
			// System.err.println("Alpha: " + alpha + ", ERR: " + measure);
			if (m < bestAlphaMeasure.alpha) {
				break;
			}
			if (m > bestAlphaMeasure.measure) {
				bestAlphaMeasure.alpha = -alpha;
				bestAlphaMeasure.measure = m;
			}
			alpha *= MULTIPLIER;
		}
		
//		double maxWeight = model.getMaxWeight();
//
//		alpha = maxWeight;
//		for (int iter = 0; iter < 5; iter++) {
//			ScoreTable newScoreTable = scores.translate(feature, alpha, 1.0);
//			double m = measure.evaluate(newScoreTable);
//			// System.err.println("Alpha: " + alpha + ", ERR: " + measure);
//			if (m > bestAlphaMeasure.measure) {
//				bestAlphaMeasure.alpha = alpha;
//				bestAlphaMeasure.measure = m;
//			}
//			alpha *= 0.1;
//		}
//
//		alpha = maxWeight;
//		for (int iter = 0; iter < 5; iter++) {
//			ScoreTable newScoreTable = scores.translate(feature, -alpha, 1.0);
//			double m = measure.evaluate(newScoreTable);
//			// System.err.println("Alpha: " + alpha + ", ERR: " + measure);
//			if (m > bestAlphaMeasure.measure) {
//				bestAlphaMeasure.alpha = -alpha;
//				bestAlphaMeasure.measure = m;
//			}
//			alpha *= 0.1;
//		}
		
		System.err.println("Feature: " + feature.getName() + ", Measure: " + bestAlphaMeasure.measure);
		return bestAlphaMeasure;
	}

}
