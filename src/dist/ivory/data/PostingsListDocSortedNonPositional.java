/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package ivory.data;

import ivory.index.TermPositions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import uk.ac.gla.terrier.compression.BitInputStream;
import uk.ac.gla.terrier.compression.BitOutputStream;

/**
 * <p>
 * Object representing a document-sorted postings list that does not hold
 * positional information for terms.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class PostingsListDocSortedNonPositional implements PostingsList {
	private static final Logger sLogger = Logger
			.getLogger(PostingsListDocSortedNonPositional.class);
	private static final int MAX_DOCNO_BITS = 32;

	static {
		sLogger.setLevel(Level.WARN);
	}

	private int mCollectionSize = -1;
	private int mNumPostings = -1;
	private int mGolombParam;
	private int mPrevDocno;
	private byte[] mRawBytes;
	private int mPostingsAdded;
	private long mSumOfPostingsScore;

	private int mDf;
	private long mCf;

	private ByteArrayOutputStream mBytesOut;
	private BitOutputStream mBitOut;

	public PostingsListDocSortedNonPositional() {
		this.mSumOfPostingsScore = 0;
		this.mPostingsAdded = 0;
		this.mDf = 0;
		this.mCf = 0;
		this.mPrevDocno = -1;

		try {
			mBytesOut = new ByteArrayOutputStream();
			mBitOut = new BitOutputStream(mBytesOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void clear() {
		this.mSumOfPostingsScore = 0;
		this.mPostingsAdded = 0;
		this.mDf = 0;
		this.mCf = 0;
		this.mPrevDocno = -1;

		try {
			mBytesOut = new ByteArrayOutputStream();
			mBitOut = new BitOutputStream(mBytesOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void add(int docno, short score, TermPositions pos) {
		add(docno, score);
	}
	public void add(int docno, short score) {
		sLogger.info("adding posting: " + docno + ", " + score);

		try {
			if (mPostingsAdded == 0) {
				// write out the first docno
				mBitOut.writeBinary(MAX_DOCNO_BITS, docno);
				mBitOut.writeGamma(score);

				mPrevDocno = docno;
			} else {
				// use d-gaps for subsequent docnos
				int dgap = docno - mPrevDocno;

				if (dgap <= 0) {
					throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
				}

				mBitOut.writeGolomb(dgap, mGolombParam);
				mBitOut.writeGamma(score);

				mPrevDocno = docno;
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error adding postings.");
		} catch (ArithmeticException e) {
			e.printStackTrace();
			throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
					+ "\": check to see if collection size or df is set properly. docno=" + docno
					+ ", tf=" + score + ", previous docno=" + mPrevDocno + ", df=" + mNumPostings
					+ ", collection size=" + mCollectionSize + ", Golomb param=" + mGolombParam);
		}

		mPostingsAdded++;
		mSumOfPostingsScore += score;
	}

	public int size() {
		return mPostingsAdded;
	}

	public PostingsReader getPostingsReader() {
		try {
			return new PostingsReader(mRawBytes, mPostingsAdded, mCollectionSize, this);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public byte[] getRawBytes() {
		return mRawBytes;
	}

	public void setCollectionDocumentCount(int docs) {
		mCollectionSize = docs;
		recomputeGolombParameter();
	}

	public int getCollectionDocumentCount() {
		return mCollectionSize;
	}

	public void setNumberOfPostings(int n) {
		mNumPostings = n;
		recomputeGolombParameter();
	}

	public int getNumberOfPostings() {
		return mNumPostings;
	}

	private void recomputeGolombParameter() {
		mGolombParam = (int) Math.ceil(0.69 * ((float) mCollectionSize) / (float) mNumPostings);
	}

	public int getDf() {
		return mDf;
	}
	
	public void setDf(int df) {
		this.mDf = df;
	}

	public long getCf() {
		return mCf;
	}
	
	public void setCf(long cf) {
		this.mCf = cf;
	}

	public void readFields(DataInput in) throws IOException {
		mPostingsAdded = WritableUtils.readVInt(in);
		mNumPostings = mPostingsAdded;

		mDf = WritableUtils.readVInt(in);
		mCf = WritableUtils.readVLong(in);
		mSumOfPostingsScore = mCf;

		mRawBytes = new byte[WritableUtils.readVInt(in)];
		in.readFully(mRawBytes);
	}

	public void write(DataOutput out) throws IOException {
		if (mRawBytes != null) {
			// this would happen if we're reading in an already-encoded
			// postings; if that's the case, simply write out the byte array
			WritableUtils.writeVInt(out, mPostingsAdded);
			WritableUtils.writeVInt(out, mDf == 0 ? mPostingsAdded : mDf); // df
			WritableUtils.writeVLong(out, mCf == 0 ? mSumOfPostingsScore : mCf); // cf
			WritableUtils.writeVInt(out, mRawBytes.length);
			out.write(mRawBytes);
		} else {
			try {
				mBitOut.padAndFlush();
				mBitOut.close();

				if (mNumPostings != mPostingsAdded) {
					throw new RuntimeException(
							"Error, number of postings added doesn't match number of expected postings.  Expected "
									+ mNumPostings + ", got " + mPostingsAdded);
				}

				WritableUtils.writeVInt(out, mPostingsAdded);
				WritableUtils.writeVInt(out, mDf == 0 ? mPostingsAdded : mDf); // df
				WritableUtils.writeVLong(out, mCf == 0 ? mSumOfPostingsScore : mCf); // cf
				byte[] bytes = mBytesOut.toByteArray();
				WritableUtils.writeVInt(out, bytes.length);
				out.write(bytes);
			} catch (ArithmeticException e) {
				throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
						+ "\": check to see if collection size or df is set properly.");
			}
		}
	}

	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	public static PostingsListDocSortedNonPositional create(DataInput in) throws IOException {
		PostingsListDocSortedNonPositional p = new PostingsListDocSortedNonPositional();
		p.readFields(in);

		return p;
	}

	public static PostingsListDocSortedNonPositional create(byte[] bytes) throws IOException {
		return PostingsListDocSortedNonPositional.create(new DataInputStream(
				new ByteArrayInputStream(bytes)));
	}

	public static String positionsToString(int[] pos) {
		StringBuffer sb = new StringBuffer();
		sb.append("[");

		for (int i = 0; i < pos.length; i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(pos[i]);
		}
		sb.append("]");

		return sb.toString();
	}

	/**
	 * <p>PostingsReader for PostingsListDocSortedNonPositional.</p>
	 * 
	 * @author Jimmy Lin
	 *
	 */
	public static class PostingsReader implements ivory.data.PostingsReader {
		private ByteArrayInputStream mBytesIn;
		private BitInputStream mBitsIn;
		private int mCnt = 0;
		private int mPrevDocno;
		private int mInnerNumPostings;
		private int mInnerGolombParam;
		private int mInnerCollectionSize;
		private PostingsList mPostingsList;

		public PostingsReader(byte[] bytes, int cnt, int n, PostingsListDocSortedNonPositional list)
				throws IOException {
			mBytesIn = new ByteArrayInputStream(bytes);
			mBitsIn = new BitInputStream(mBytesIn);
			mInnerNumPostings = cnt;
			mInnerCollectionSize = n;
			mInnerGolombParam = (int) Math.ceil(0.69 * ((float) mInnerCollectionSize)
					/ (float) mInnerNumPostings);
			mPostingsList = list;
		}

		public int getNumberOfPostings() {
			return mInnerNumPostings;
		}

		public void reset() {
			try {
				mBytesIn.reset();
				mBitsIn = new BitInputStream(mBytesIn);
				mCnt = 0;
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Error resetting postings.");
			}
		}

		public boolean nextPosting(Posting p) {
			try {
				if (mCnt == 0) {
					p.setDocno(mBitsIn.readBinary(MAX_DOCNO_BITS));
					p.setScore((short) mBitsIn.readGamma());
				} else {
					if (mCnt >= mInnerNumPostings)
						return false;

					p.setDocno(mPrevDocno + mBitsIn.readGolomb(mInnerGolombParam));
					p.setScore((short) mBitsIn.readGamma());
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e.toString());
			}

			mCnt++;
			mPrevDocno = p.getDocno();

			return true;
		}

		public int[] getPositions() {
			throw new UnsupportedOperationException();
		}

		public boolean hasMorePostings() {
			throw new UnsupportedOperationException();
		}

		public short peekNextScore() {
			throw new UnsupportedOperationException();
		}

		public int peekNextDocno() {
			throw new UnsupportedOperationException();
		}

		public PostingsList getPostingsList() {
			return mPostingsList;
		}
	}
}
