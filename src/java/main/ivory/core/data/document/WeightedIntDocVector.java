
package ivory.core.data.document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.map.HMapIFW;

/**
 * Implementation of {@link IntDocVector} with term weights
 * 
 * @author Earl J. Wagner
 * 
 */
public class WeightedIntDocVector implements IntDocVector {

	private static final Logger sLogger = Logger.getLogger (WeightedIntDocVector.class);
	{
		sLogger.setLevel (Level.DEBUG);
	}

	private int docLength;
	private HMapIFW weightedTerms;

	public WeightedIntDocVector () {
		docLength = 0;
		weightedTerms = new HMapIFW ();
	}


	public WeightedIntDocVector (int docLength, HMapIFW weightedTerms) {
		this.docLength = docLength;
		this.weightedTerms = weightedTerms;
	}

	public HMapIFW getWeightedTerms() {
		return weightedTerms;
	}

	public void setWeightedTerms(HMapIFW weightedTerms) {
		this.weightedTerms = weightedTerms;
	}
	
	public int getDocLength () {
		return docLength;
	}

	public void setDocLength (int docLength) {
		this.docLength = docLength;
	}

	public void write (DataOutput out) throws IOException {
		WritableUtils.writeVInt (out, docLength);
		weightedTerms.write (out);
	}

	public void readFields (DataInput in) throws IOException {
		docLength = WritableUtils.readVInt (in);
		weightedTerms = new HMapIFW ();
		weightedTerms.readFields (in);
	}

	public Reader getReader () throws IOException {
		return null;
	}

	public float dot (WeightedIntDocVector otherVector) {
		//sLogger.debug ("dot (otherVector: " + otherVector + ")");
		float result = weightedTerms.dot (otherVector.weightedTerms);
		//sLogger.debug ("in KMeansClusterDocs mapper dotProduct () returning: " + result);
		return result;
	}

	public void plus (WeightedIntDocVector otherVector) {
		//sLogger.debug ("plus (otherVector: " + otherVector + ")");
		//sLogger.debug ("weightedTerms == null: " + (weightedTerms == null));
		//sLogger.debug ("otherVector.mWeightedTerms == null: " + (otherVector.mWeightedTerms == null));
		weightedTerms.plus (otherVector.weightedTerms);
		docLength += otherVector.docLength;
	}

	public void normalizeWith (float l) {
		for (int f : weightedTerms.keySet ()) {
			weightedTerms.put (f, weightedTerms.get (f) / l);
		}
	}

	@Override
	public String toString() {
	  return weightedTerms.toString();
	}

	public boolean containsTerm(int termid) {
	  return weightedTerms.containsKey(termid);
	}

	public float getWeight(int termid) {
	  return weightedTerms.get(termid);
	}
}
	

