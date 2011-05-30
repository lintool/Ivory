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


import ivory.exception.ConfigurationException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author metzler
 *
 */
public class Instances {

	private static final String QID_COLUMN_NAME = "qid";      // query id feature name
	private static final String DOCID_COLUMN_NAME = "docid";  // document id feature name
	private static final String GRADE_COLUMN_NAME = "grade";  // relevance grade feature name
	
	private String [] queryIds = null;              // query ids
	private String [] docIds = null;                // document ids
	private float [] grades = null;                 // relevance grades
	private String [] featureNames = null;          // feature names
	private int [] featureCols = null;              // feature column indexes
	private Map<String,Integer> featureMap = null;  // mapping from feature name to column index
	private float [][] features = null;             // feature values
	
	public Instances(String featFile) throws IOException, ConfigurationException {
		initialize(featFile);
	}

	private void initialize(String featFile) throws IOException, ConfigurationException {
		// open feature file for reading
		BufferedReader in = new BufferedReader(new FileReader(featFile));
		
		// make pass through training data to count number of
		// columns (features) and rows (instances)
		int numRows;
		int numCols;
		int numFeats;
		
		// query id, document id, and grade column indexes
		int qidCol = -1;
		int docidCol = -1;
		int gradeCol = -1;
		
		// read header
		String line = in.readLine();
		String [] colNames = line.split("\t");
		numCols = colNames.length;
		
		numFeats = 0;
		for(int i = 0; i < numCols; i++) {
			String colName = colNames[i];
			if(QID_COLUMN_NAME.equals(colName)) {
				qidCol = i;
			}
			else if(DOCID_COLUMN_NAME.equals(colName)) {
				docidCol = i;
			}
			else if(GRADE_COLUMN_NAME.equals(colName)) {
				gradeCol = i;
			}
			else {
				numFeats++;
			}
		}
		
		System.err.println("Query ID column: "  + qidCol);
		System.err.println("Document ID column: "  + docidCol);
		System.err.println("Grade column: "  + gradeCol);
		System.err.println("Number of features: "  + numFeats);
		
		// process the rest of the file
		numRows = 0;
		while((line = in.readLine()) != null) {
			numRows++;
		}
		
		System.err.println("Number of instances: " + numRows);
		
		// close feature file
		in.close();
		
		// initialize query ids, doc ids, grades, and features
		queryIds = new String[numRows];
		docIds = new String[numRows];
		grades = new float[numRows];
		featureNames = new String[numFeats];
		featureCols = new int[numFeats];
		featureMap = new HashMap<String,Integer>();
		features = new float[numRows][numFeats];
		
		// make second pass through feature file
		in = new BufferedReader(new FileReader(featFile));
		
		// process header
		line = in.readLine();
		int featureId = 0;
		for(int i = 0; i < numCols; i++) {
			String colName = colNames[i];
			if(QID_COLUMN_NAME.equals(colName)) {
				continue;
			}
			else if(DOCID_COLUMN_NAME.equals(colName)) {
				continue;
			}
			else if(GRADE_COLUMN_NAME.equals(colName)) {
				continue;
			}
			else {
				featureNames[featureId] = colName;
				featureCols[featureId] = i;
				featureMap.put(colName, featureId);
				featureId++;
			}
		}
		
		// read instances
		int rowNum = 0;
		while((line = in.readLine()) != null) {
			String [] fvals = line.split("\t");
			if(fvals.length != numCols) {
				throw new ConfigurationException("Line -- " + line + " has the incorrect number of columns! "+fvals.length+" "+numCols);
			}
		
			queryIds[rowNum] = new String(fvals[qidCol]);
			docIds[rowNum] = new String(fvals[docidCol]);
			grades[rowNum] = Float.parseFloat(fvals[gradeCol]);
			
			for(int i = 0; i < featureCols.length; i++) {
				int featureCol = featureCols[i];
				features[rowNum][i] = Float.parseFloat(fvals[featureCol]);
			}

			rowNum++;
			if(rowNum % 1000 == 0) {
				System.err.println("Read " + rowNum + " instances...");
			}
		}
		
		// close feature file
		in.close();
	}

	public int getNumInstances() {
		return features.length;
	}

	public float [] getInstance(int i) {
		return features[i];
	}

	public Map<String, Integer> getFeatureMap() {
		return featureMap;
	}

	public String[] getQids() {
		return queryIds;
	}

	public String[] getDocids() {
		return docIds;
	}

	public float[] getGrades() {
		return grades;
	}

	public boolean featureIsConstant(Feature f) {
		String lastQid = null;
		float lastFv = Float.NaN;
		for(int i = 0; i < queryIds.length; i++) {
			String qid = queryIds[i];
			float fv = f.eval(features[i]);
			if(lastQid == null) {
				lastQid = qid;
				lastFv = fv;
			}
			if(lastQid.equals(qid) && lastFv != fv) {
				return false;
			}
			lastQid = qid;
			lastFv = fv;
		}
		
		return true;
	}

	public double getCorrelation(Feature featA, Feature featB) {
		double a = 0.0;
		double b = 0.0;
		
		double ab = 0.0;
		
		double aa = 0.0;
		double bb = 0.0;
		
		int n = features.length;
		
		for(int i = 0; i < n; i++) {
			float x = featA.eval(features[i]);
			float y = featB.eval(features[i]);

			a += x;
			b += y;
			
			ab += x*y;
			
			aa += x*x;
			bb += y*y;
		}
		
		double ma = a / n;
		double maa = aa / n;
		
		double mb = b / n;
		double mbb = bb / n;
		
		return ( ab - mb*a - ma*b + ma*mb*n ) / ( ( n - 1 ) * Math.sqrt(maa - ma*ma) * Math.sqrt(mbb - mb*mb) );
	}
}
