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
 * Object representing a document-sorted postings list that holds positional
 * information for terms.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class PostingsListDocSortedPositional implements PostingsList {
	private static final Logger sLogger = Logger.getLogger(PostingsListDocSortedPositional.class);
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
	private BitOutputStream mBitsOut;

	public PostingsListDocSortedPositional() {
		this.mSumOfPostingsScore = 0;
		this.mPostingsAdded = 0;
		this.mPrevDocno = -1;

		try {
			mBytesOut = new ByteArrayOutputStream();
			mBitsOut = new BitOutputStream(mBytesOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void clear() {
		this.mSumOfPostingsScore = 0;
		this.mPostingsAdded = 0;
		this.mPrevDocno = -1;

		try {
			mBytesOut = new ByteArrayOutputStream();
			mBitsOut = new BitOutputStream(mBytesOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void add(int docno, short score, TermPositions pos) {
		sLogger.info("adding posting: " + docno + ", " + score + ", " + pos.toString());

		try {
			if (mPostingsAdded == 0) {
				// write out the first docno
				mBitsOut.writeBinary(MAX_DOCNO_BITS, docno);
				mBitsOut.writeGamma(score);
				writePositions(mBitsOut, pos, docno, score);

				mPrevDocno = docno;
			} else {
				// use d-gaps for subsequent docnos
				int dgap = docno - mPrevDocno;

				if (dgap <= 0) {
					throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
				}

				mBitsOut.writeGolomb(dgap, mGolombParam);
				mBitsOut.writeGamma(score);
				writePositions(mBitsOut, pos, docno, score);

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

	// passing in docno and tf basically for error checking purposes
	private static void writePositions(BitOutputStream t, TermPositions p, int docno, short tf)
			throws IOException {
		int[] pos = p.getPositions();

		if (tf != p.getTf()) {
			throw new RuntimeException("Error: tf and number of positions don't match. docno="
					+ docno + ", tf=" + tf + ", positions=" + p.toString());
		}

		if (p.getTf() == 1) {
			// if tf=1, just write out the single term position
			t.writeGamma(pos[0]);
		} else {
			// if tf > 1, write out skip information if we want to bypass the
			// positional information during decoding
			t.writeGamma(p.getEncodedSize());

			// keep track of where we are in the stream
			int skip_pos1 = (int) t.getByteOffset() * 8 + t.getBitOffset();

			// write out first position
			t.writeGamma(pos[0]);
			// write out rest of positions using p-gaps (first order positional
			// differences)
			for (int c = 1; c < p.getTf(); c++) {
				int pgap = pos[c] - pos[c - 1];
				if (pos[c] <= 0 || pgap == 0) {
					throw new RuntimeException("Error: invalid term positions. positions="
							+ p.toString() + ", docno=" + docno + ", tf=" + tf);
				}
				t.writeGamma(pgap);
			}

			// find out where we are in the stream no
			int skip_pos2 = (int) t.getByteOffset() * 8 + t.getBitOffset();

			// verify that the skip information is indeed valid
			if (skip_pos1 + p.getEncodedSize() != skip_pos2) {
				throw new RuntimeException("Ivalid skip information: skip_pos1=" + skip_pos1
						+ ", skip_pos2=" + skip_pos2 + ", size=" + p.getEncodedSize());
			}

		}
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

	public long getSumOfPostingsScore() {
		return mSumOfPostingsScore;
	}

	public int getDf() {
		return mDf;
	}

	public long getCf() {
		return mCf;
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
			WritableUtils.writeVInt(out, mPostingsAdded); // df
			WritableUtils.writeVLong(out, mSumOfPostingsScore); // cf
			WritableUtils.writeVInt(out, mRawBytes.length);
			out.write(mRawBytes);
		} else {
			try {
				mBitsOut.padAndFlush();
				mBitsOut.close();

				if (mNumPostings != mPostingsAdded) {
					throw new RuntimeException(
							"Error, number of postings added doesn't match number of expected postings.  Expected "
									+ mNumPostings + ", got " + mPostingsAdded);
				}

				WritableUtils.writeVInt(out, mPostingsAdded);
				WritableUtils.writeVInt(out, mPostingsAdded); // df
				WritableUtils.writeVLong(out, mSumOfPostingsScore); // cf
				byte[] bytes = mBytesOut.toByteArray();
				WritableUtils.writeVInt(out, bytes.length);
				out.write(bytes);
			} catch (ArithmeticException e) {
				throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
						+ "\": check to see if collection size or df is set properly.");
			}

			sLogger
					.info("serializing postings: cf=" + mSumOfPostingsScore + ", df="
							+ mNumPostings);
		}
	}

	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	public static PostingsListDocSortedPositional create(DataInput in) throws IOException {
		PostingsListDocSortedPositional p = new PostingsListDocSortedPositional();
		p.readFields(in);

		return p;
	}

	public static PostingsListDocSortedPositional create(byte[] bytes) throws IOException {
		return PostingsListDocSortedPositional.create(new DataInputStream(new ByteArrayInputStream(
				bytes)));
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
	 * <p>PostingsReader for PostingsListDocSortedPositional.</p>
	 * 
	 * @author Jimmy Lin
	 *
	 */
	public static class PostingsReader implements ivory.data.PostingsReader {
		private ByteArrayInputStream mBytesIn;
		private BitInputStream mBitsIn;
		private int mCnt = 0;
		private int mPrevDocno;
		private int mPrevTf;
		private int mInnerNumPostings;
		private int mInnerGolombParam;
		private int mInnerCollectionSize;
		private boolean mNeedToReadPositions;
		private PostingsList mPostingsList;

		public PostingsReader(byte[] bytes, int cnt, int n, PostingsListDocSortedPositional list)
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
				if (mNeedToReadPositions) {
					skipPositions(mPrevTf);
					mNeedToReadPositions = false;
				}

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
				throw new RuntimeException();
			}

			mCnt++;
			mPrevDocno = p.getDocno();
			mPrevTf = p.getScore();
			mNeedToReadPositions = true;

			return true;
		}

		public int[] getPositions() {
			int[] pos = null;
			try {
				if (mPrevTf == 1) {
					pos = new int[1];
					pos[0] = (short) mBitsIn.readGamma();
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
				return null;
			}

			mNeedToReadPositions = false;
			return pos;
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

		private void skipPositions(int tf) throws IOException {
			if (tf == 1) {
				mBitsIn.readGamma();
			} else {
				mBitsIn.skipBits(mBitsIn.readGamma());
			}
		}

		public PostingsList getPostingsList() {
			return mPostingsList;
		}
	}
}
