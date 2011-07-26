
package ivory.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.map.HMapIFW;

import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.MapIF;
//import edu.umd.cloud9.util.pair.KeyValuePair;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import ivory.util.RetrievalEnvironment;


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

	private int nDocLength;
	//private HMapIFW mWeightedTerms;
	public HMapIFW mWeightedTerms;

	public WeightedIntDocVector () {
		nDocLength = 0;
		mWeightedTerms = new HMapIFW ();
	}


	public WeightedIntDocVector (int docLength, HMapIFW weightedTerms) {
		nDocLength = docLength;
		mWeightedTerms = weightedTerms;
	}


	public boolean isInfinite () {
		return mWeightedTerms.get (0) == Float.POSITIVE_INFINITY;
	}


	public int getDocLength () {
		return nDocLength;
	}

	public void setDocLength (int docLength) {
		nDocLength = docLength;
	}
	


	public void write (DataOutput out) throws IOException {
		WritableUtils.writeVInt (out, nDocLength);
		mWeightedTerms.write (out);
	}

	public void readFields (DataInput in) throws IOException {
		nDocLength = WritableUtils.readVInt (in);
		mWeightedTerms = new HMapIFW ();
		mWeightedTerms.readFields (in);
	}


	public IntDocVectorReader getDocVectorReader () throws IOException {
		//return new Reader (weightedTerms);
		return null; //new Reader (weightedTerms);
	}

	/*
	public static class Reader implements IntDocVectorReader {
		private ByteArrayInputStream mBytesIn;
		private BitInputStream mBitsIn;
		private int p = -1;
		private int mPrevTermID = -1;
		private short mPrevTf = -1;
		private int nTerms;
		private boolean mNeedToReadPositions = false;

		HMapIFW weightedTerms;


		public Reader (HMapIFW weightedTerms) throws IOException {
			this.weightedTerms = weightedTerms;
		}

		public int getNumberOfTerms () {
			return weightedTerms.size ();
		}

		public short getTf () {
			return mPrevTf;
		}

		public void reset() {
			try {
				mBytesIn.reset();
				mBitsIn = new BitInputStream(mBytesIn);
				p = -1;
				mPrevTf = -1;
				mNeedToReadPositions = false;
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Error resetting postings.");
			}
		}

		public int nextTerm() {
			int id = -1;
			try {
				p++;
				if (mNeedToReadPositions) {
					skipPositions(mPrevTf);
				}
				mNeedToReadPositions = true;
				if (p == 0) {
					mPrevTermID = mBitsIn.readBinary(32);
					mPrevTf = (short) mBitsIn.readGamma();
					return mPrevTermID;
				} else {
					if (p > nTerms - 1)
						return -1;
					id = mBitsIn.readGamma() + mPrevTermID;
					mPrevTermID = id;
					mPrevTf = (short) mBitsIn.readGamma();
					return id;
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
		}

		public int[] getPositions() {
			int[] pos = null;
			try {
				if (mPrevTf == 1) {
					pos = new int[1];
					pos[0] = mBitsIn.readGamma();
				} else {
					mBitsIn.readGamma();
					pos = new int[mPrevTf];
					pos[0] = mBitsIn.readGamma();
					for (int i = 1; i < mPrevTf; i++) {
						pos[i] = (pos[i - 1] + mBitsIn.readGamma());
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("A problem in reading bits?" + e);
			}
			mNeedToReadPositions = false;

			return pos;
		}

		public boolean getPositions(TermPositions tp) {
			int[] pos = getPositions();

			if (pos == null)
				return false;

			tp.set(pos, (short) pos.length);

			return true;
		}

		public boolean hasMoreTerms() {
			return !(p >= nTerms - 1);
		}

		private void skipPositions(int tf) throws IOException {
			if (tf == 1) {
				mBitsIn.readGamma();
			} else {
				mBitsIn.skipBits(mBitsIn.readGamma());
			}
		}
	}
	*/


	/*
public double innerProductWith (WeightedIntDocVector otherVector) {
		//		double dotProduct (int docVector1ID, WeightedIntDocVector docVector1, int docVector2ID, WeightedIntDocVector docVector2) {
		sLogger.debug ("innerProductWith (otherVector: " + otherVector + ")");
		//int docLength = 0;

		long productsSum = 0;
		int termCount = 0;
		HMapIFW otherWeightedTerms = otherVector.mWeightedTerms;
		Iterator<MapIF.Entry> otherWeightedTermsIter = otherWeightedTerms.entrySet ().iterator ();

		//sLogger.debug ("in innerProductWith (), mWeightedTerms: " + mWeightedTerms);
		//sLogger.debug ("in innerProductWith (), otherWeightedTerms: " + otherWeightedTerms);
		//int otherWeightedTermsSize = otherWeightedTerms.size ();
		while (otherWeightedTermsIter.hasNext ()) {
			MapIF.Entry otherEntry = otherWeightedTermsIter.next ();
			int term = otherEntry.getKey();
			if (mWeightedTerms.containsKey (term)) {
				float otherWeight = otherEntry.getValue();
				float myWeight = mWeightedTerms.get (term);
				//docLength1 += freq1;
				//docLength2 += freq1;
				productsSum += myWeight * otherWeight;
				termCount += 1;
			}
		}
		sLogger.debug ("in innerProductWith (), productsSum: " + productsSum);
		sLogger.debug ("in innerProductWith (), nDocLength: " + DocLength);
		sLogger.debug ("in innerProductWith (), otherVector.nDocLength: " + otherVector.nDocLength);

		double result = productsSum / (nDocLength + otherVector.nDocLength);
		sLogger.debug ("in KMeansClusterDocs mapper dotProduct () returning: " + result);
		return result;
	}
	*/

	public String toString () {
		return "vector of length: " + nDocLength +
			(mWeightedTerms.size () == 2 
			 ? " (" + mWeightedTerms.get (0) + ", " + mWeightedTerms.get (1) + ")"
			 : "");
	}


	public String topTerms (RetrievalEnvironment env) {
		StringBuilder terms = new StringBuilder ();
		try {
			int entryId = 0;
			for (MapIF.Entry entry : mWeightedTerms.getEntriesSortedByValue ()) {
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


	public void plus (WeightedIntDocVector otherVector) {
		sLogger.trace ("plus (otherVector: " + otherVector + ")");
		sLogger.trace ("mWeightedTerms == null: " + (mWeightedTerms == null));
		sLogger.trace ("otherVector.mWeightedTerms == null: " + (otherVector.mWeightedTerms == null));
		mWeightedTerms.plus (otherVector.mWeightedTerms);
		nDocLength += otherVector.nDocLength;
	}

	public void normalizeWith (float l) {
		for (int f : mWeightedTerms.keySet ()) {
			mWeightedTerms.put (f, mWeightedTerms.get (f) / l);
		}
	}

}
	

