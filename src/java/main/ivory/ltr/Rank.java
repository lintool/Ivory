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



import ivory.core.ConfigurationException;

import java.io.IOException;
import java.util.List;


/**
 * @author Don Metzler
 *
 */
public class Rank {

	/**
	 * @param args
	 * @throws RankIRException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ConfigurationException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, ConfigurationException {
		if(args.length != 2) {
			System.out.println("Usage: Rank <test-file> <model-file>");
			System.exit(-1);
		}

		String testFile = args[0];
		String modelFile = args[1];
		
		Instances instances = new Instances(testFile);

		Model model = Model.readFromFile(modelFile);
		
		List<Feature> features = model.getFeatures();
		List<Double> weights = model.getWeights();

		ScoreTable scores = new ScoreTable(instances);
		for(int i = 0; i < features.size(); i++) {
			scores = scores.translate(features.get(i), weights.get(i), 1.0);
		}
		System.out.print(scores);
		
	}

}
