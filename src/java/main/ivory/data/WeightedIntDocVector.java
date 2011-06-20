
package ivory.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.WritableUtils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.util.map.MapIF;

import ivory.util.RetrievalEnvironment;


/**
 * Implementation of {@link IntDocVector} with term weights
 * 
 * @author Earl J. Wagner
 * 
 */
public class WeightedIntDocVector implements IntDocVector {

	private static final Logger LOG = Logger.getLogger (WeightedIntDocVector.class);
	{
		LOG.setLevel (Level.DEBUG);
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

	public boolean isInfinite () {
		return weightedTerms.get (0) == Float.POSITIVE_INFINITY;
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

	public IntDocVectorReader getDocVectorReader () throws IOException {
		return null;
	}

	public float dot (WeightedIntDocVector otherVector) {
		//LOG.debug ("dot (otherVector: " + otherVector + ")");
		float result = weightedTerms.dot (otherVector.weightedTerms);
		//LOG.debug ("in KMeansClusterDocs mapper dotProduct () returning: " + result);
		return result;
	}

	public void plus (WeightedIntDocVector otherVector) {
		//LOG.debug ("plus (otherVector: " + otherVector + ")");
		//LOG.debug ("weightedTerms == null: " + (weightedTerms == null));
		//LOG.debug ("otherVector.mWeightedTerms == null: " + (otherVector.mWeightedTerms == null));
		weightedTerms.plus (otherVector.weightedTerms);
		docLength += otherVector.docLength;
	}

	public void normalizeWith (float l) {
		for (int f : weightedTerms.keySet ()) {
			weightedTerms.put (f, weightedTerms.get (f) / l);
		}
	}

	public String toString () {
		return "vector of length: " + docLength +
			(weightedTerms.size () == 2 
			 ? " (" + weightedTerms.get (0) + ", " + weightedTerms.get (1) + ")"
			 : "");
	}


	public String topTerms (RetrievalEnvironment env) {
		StringBuilder terms = new StringBuilder ();
		try {
			int entryId = 0;
			for (MapIF.Entry entry : weightedTerms.getEntriesSortedByValue ()) {
				if (entryId == 50) break;
				String spacer = (entryId == 0 ? "" : " ");
				String term = (env != null ? env.getTermFromId (entry.getKey ()).toString () : Integer.toString (entry.getKey ()));
				terms.append (spacer + term);
				entryId += 1;
			}
		} catch (Exception e) {
			e.printStackTrace ();
			throw new RuntimeException ("Error converting printing centroids!");
		}
		return terms.toString ();
	}
}
	

