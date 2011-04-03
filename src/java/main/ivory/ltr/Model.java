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


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Don Metzler
 *
 */
public class Model implements Serializable {

	private static final long serialVersionUID = 3350348178427870466L;

	private Map<Feature,Integer> featureMap = null; // feature -> id map
	private List<Feature> features = null;          // features
	private List<Double> weights = null;            // weights
	
	public Model() {
		this.featureMap = new HashMap<Feature,Integer>();
		this.features = new ArrayList<Feature>();
		this.weights = new ArrayList<Double>();
	}
	
	public Model(Model model) {
		this.featureMap = new HashMap<Feature,Integer>(model.getFeatureMap());
		this.features = new ArrayList<Feature>(model.getFeatures());
		this.weights = new ArrayList<Double>(model.getWeights());
	}

	private Map<Feature, Integer> getFeatureMap() {
		return featureMap;
	}

	public void addFeature(Feature f, double weight) {
		Integer index = featureMap.get(f);
		if(index != null) {
			weights.set(index, weights.get(index) + weight);
		}
		else {
			featureMap.put(f, features.size());
			features.add(f);
			weights.add(weight);
		}
		_normalizeWeights();
	}

	private void _normalizeWeights() {
		double totalWeight = 0.0;
		for(Double w : weights) {
			totalWeight += w;
		}

		for(int i = 0; i < weights.size(); i++) {
			weights.set(i, weights.get(i) / totalWeight);
		}
	}

	public double getMaxWeight() {
		double maxWeight = Double.MIN_VALUE;
		for(Double w : weights) {
			if(w > maxWeight) {
				maxWeight = w;
			}
		}
		return maxWeight;
	}

	public int getNumFeatures() {
		return features.size();
	}
	
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append("[ ");
		for(int i = 0; i < features.size(); i++) {
			str.append(features.get(i).getName());
			str.append(":");
			str.append(weights.get(i));
			str.append(" ");
		}
		str.append("]");
		return str.toString();
	}

	public void write(String modelFile) throws FileNotFoundException, IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(modelFile));
		out.writeObject(this);
		out.close();
	}

	public static Model readFromFile(String modelFile) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(modelFile));
		Model m = (Model)in.readObject();
		return m;
	}

	public List<Feature> getFeatures() {
		return features;
	}

	public List<Double> getWeights() {
		return weights;
	}

	public boolean containsFeature(Feature feature) {
		return featureMap.containsKey(feature);
	}
}
