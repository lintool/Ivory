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


/**
 * @author Don Metzler
 *
 */
public class ScoreTable {

	private Instances instances = null; // instances
	private float [] scores = null;     // instance scores
	
	public ScoreTable(Instances instances) {
		this.instances = instances;		
		this.scores = new float[instances.getNumInstances()];
	}
	
	public ScoreTable(ScoreTable table, float [] scores) {
		this.instances = table.getInstances();
		this.scores = scores;
	}

	public ScoreTable translate(Feature feat, double weight, double scale) {
		float [] newScores = new float[instances.getNumInstances()];
		for(int i = 0; i < scores.length; i++) {
			newScores[i] = (float) (scale * (scores[i] + weight * feat.eval(instances.getInstance(i))));
		}
		return new ScoreTable(this, newScores);
	}

	public Instances getInstances() {
		return instances;
	}
	
	public String[] getQids() {
		return instances.getQids();
	}

	public float[] getGrades() {
		return instances.getGrades();
	}

	public float[] getScores() {
		return scores;
	}
	
	public String toString() {
		StringBuffer str = new StringBuffer();

		String [] qids = instances.getQids();
		String [] docids = instances.getDocids();
		for(int i = 0; i < instances.getNumInstances(); i++) {
			str.append(qids[i]);
			str.append("\t");
			str.append(docids[i]);
			str.append("\t");
			str.append(scores[i]);
			str.append("\n");
		}
		
		return str.toString();
	}
}
