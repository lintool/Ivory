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
 * Object representing a score-sorted postings list.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class PostingsListScoreSorted implements PostingsList {
	private static final Logger sLogger = Logger.getLogger(PostingsListScoreSorted.class);
	private static final int MAX_DOCNO_BITS = 32;

	static {
		sLogger.setLevel(Level.WARN);
	}

	public static final int GAMMA_SCORE_ENCODING = 1;
	public static final int BINARY_SCORE_ENCODING = 2;

	public static int sScoreEncoding = GAMMA_SCORE_ENCODING;
	public static int sScoreBits = 6;

	public static void setScoreEncodingMode(int m) {
		sScoreEncoding = m;
	}

	public static int getScoreEncodingMode() {
		return sScoreEncoding;
	}

	public static void setScoreBits(int b) {
		sScoreBits = b;
	}

	public static int getScoreBits() {
		return sScoreBits;
	}

	private int mCollectionSize = -1;
	private int mNumPostings = -1;
	private byte[] mRawBytes;
	private int mPostingsAdded;
	private int mPrevDocno;
	private int mPrevScore;
	private int mDocsWithSameScore;

	private ByteArrayOutputStream mCurrentScoreBytes;
	private BitOutputStream mCurrentScoreBits;

	private ByteArrayOutputStream mBytesOut;
	private BitOutputStream mBitsOut;

	public PostingsListScoreSorted() {
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
		sLogger.info("adding posting: (docno=" + docno + ", score=" + score + ")");

		try {
			if (mPostingsAdded == 0) {
				mPrevDocno = docno;
				mPrevScore = score;
				mDocsWithSameScore = 1;

				mCurrentScoreBytes = new ByteArrayOutputStream();
				mCurrentScoreBits = new BitOutputStream(mCurrentScoreBytes);

				mCurrentScoreBits.writeGamma(docno);
			} else if (score != mPrevScore) {
				flushDocnosWithSameScore();

				mPrevDocno = docno;
				mPrevScore = score;
				mDocsWithSameScore = 1;

				mCurrentScoreBytes = new ByteArrayOutputStream();
				mCurrentScoreBits = new BitOutputStream(mCurrentScoreBytes);

				mCurrentScoreBits.writeGamma(docno);
			} else {
				int dgap = docno - mPrevDocno;

				if (dgap <= 0) {
					throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
				}

				mCurrentScoreBits.writeGamma(dgap);
				mPrevDocno = docno;
				mDocsWithSameScore++;
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error adding postings.");
		} catch (ArithmeticException e) {
			e.printStackTrace();
			throw new RuntimeException("ArithmeticException caught \"" + e.getMessage() + "\"");
		}

		mPostingsAdded++;
	}

	private void flushDocnosWithSameScore() throws IOException {
		int golombParam = (int) Math.ceil(0.69 * ((float) mCollectionSize)
				/ (float) mDocsWithSameScore);

		mCurrentScoreBits.padAndFlush();
		mCurrentScoreBits.close();

		BitInputStream din = new BitInputStream(new ByteArrayInputStream(mCurrentScoreBytes
				.toByteArray()));

		if (sScoreEncoding == GAMMA_SCORE_ENCODING) {
			mBitsOut.writeGamma(mPrevScore);
		} else {
			mBitsOut.writeBinary(sScoreBits, mPrevScore);
		}
		mBitsOut.writeGamma(mDocsWithSameScore);

		mBitsOut.writeBinary(MAX_DOCNO_BITS, din.readGamma());
		for (int i = 0; i < mDocsWithSameScore - 1; i++) {
			mBitsOut.writeGolomb(din.readGamma(), golombParam);
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
	}

	public int getCollectionDocumentCount() {
		return mCollectionSize;
	}

	public void setNumberOfPostings(int n) {
		mNumPostings = n;
	}

	public int getNumberOfPostings() {
		return mNumPostings;
	}

	public int getDf() {
		throw new UnsupportedOperationException();
	}

	public void setDf(int df) {
		throw new UnsupportedOperationException();
	}

	public long getCf() {
		throw new UnsupportedOperationException();
	}

	public void setCf(long cf) {
		throw new UnsupportedOperationException();
	}

	public void readFields(DataInput in) throws IOException {
		mPostingsAdded = WritableUtils.readVInt(in);
		mNumPostings = mPostingsAdded;
		mRawBytes = new byte[WritableUtils.readVInt(in)];
		in.readFully(mRawBytes);
	}

	public void write(DataOutput out) throws IOException {
		if (mRawBytes != null) {
			// this would happen if we're reading in an already-encoded
			// postings; if that's the case, simply write out the byte array
			WritableUtils.writeVInt(out, mPostingsAdded);
			WritableUtils.writeVInt(out, mRawBytes.length);
			out.write(mRawBytes);
		} else {
			try {
				flushDocnosWithSameScore();

				mBitsOut.padAndFlush();
				mBitsOut.close();

				WritableUtils.writeVInt(out, mPostingsAdded);
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

	public static PostingsListScoreSorted create(DataInput in) throws IOException {
		PostingsListScoreSorted p = new PostingsListScoreSorted();
		p.readFields(in);

		return p;
	}

	public static PostingsListScoreSorted create(byte[] bytes) throws IOException {
		return PostingsListScoreSorted.create(new DataInputStream(new ByteArrayInputStream(bytes)));
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
	 * <p>
	 * PostingsReader for PostingsListScoreSorted.
	 * </p>
	 * 
	 * @author Jimmy Lin
	 * 
	 */
	public static class PostingsReader implements ivory.data.PostingsReader {
		private ByteArrayInputStream mBytesIn;
		private BitInputStream mBitsIn;
		private int mPrevDocno;
		private short mCurScore = 0;
		private int mTotalPostingsRead = 0;
		private int mPostingsWithCurScore;
		private int mPostingsWithCurScoreRead = 0;
		private int mInnerNumPostings;
		private int mInnerGolombParam;
		private int mInnerCollectionSize;

		private int mPeekDocno = 0;
		private short mPeekScore = 0;
		private Posting mPeekPosting = new Posting();

		private PostingsList mPostingsList;

		public PostingsReader(byte[] bytes, int cnt, int n, PostingsList list) throws IOException {
			mBytesIn = new ByteArrayInputStream(bytes);
			mBitsIn = new BitInputStream(mBytesIn);
			mInnerNumPostings = cnt;
			mInnerCollectionSize = n;
			mInnerGolombParam = (int) Math.ceil(0.69 * ((float) mInnerCollectionSize)
					/ (float) mInnerNumPostings);
			list = mPostingsList;
		}

		public int getNumberOfPostings() {
			return mInnerNumPostings;
		}

		public void reset() {
			try {
				mBytesIn.reset();
				mBitsIn = new BitInputStream(mBytesIn);
				mPostingsWithCurScoreRead = 0;
				mTotalPostingsRead = 0;
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Error resetting postings.");
			}
		}

		public boolean nextPosting(Posting p) {
			// we might have already read the posting from peeking ahead
			if (mPeekDocno != 0) {
				p.setDocno(mPeekDocno);
				p.setScore(mPeekScore);
				mPeekDocno = 0;

				return true;
			}

			// check to see if we're at the end
			if (mTotalPostingsRead >= mInnerNumPostings)
				return false;

			try {
				if (mCurScore == 0) {
					if (sScoreEncoding == GAMMA_SCORE_ENCODING) {
						mCurScore = (short) mBitsIn.readGamma();
					} else {
						mCurScore = (short) mBitsIn.readBinary(sScoreBits);
					}
					mPostingsWithCurScore = mBitsIn.readGamma();

					// compute Golomb parameter for current run of docs with the
					// same score
					mInnerGolombParam = (int) Math.ceil(0.69 * ((float) mInnerCollectionSize)
							/ (float) mPostingsWithCurScore);

					// the first docno is written as a binary int
					p.setDocno(mBitsIn.readBinary(MAX_DOCNO_BITS));
					p.setScore(mCurScore);
					mPostingsWithCurScoreRead++;
				} else {
					// subsequent docnos are stored using d-gaps
					p.setDocno(mPrevDocno + mBitsIn.readGolomb(mInnerGolombParam));
					p.setScore(mCurScore);
					mPostingsWithCurScoreRead++;
				}

				mPrevDocno = p.getDocno();

				// reset if we've decoded all postings with the same score
				if (mPostingsWithCurScoreRead == mPostingsWithCurScore) {
					mPostingsWithCurScoreRead = 0;
					mCurScore = 0;
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException();
			}

			mTotalPostingsRead++;

			return true;
		}

		public int[] getPositions() {
			throw new UnsupportedOperationException();
		}

		public boolean hasMorePostings() {
			return mTotalPostingsRead < mInnerNumPostings || mPeekDocno != 0;
		}

		public short peekNextScore() {
			if (mPeekDocno == 0) {
				if (!nextPosting(mPeekPosting)) {
					throw new RuntimeException("Reached end of postings!");
				}

				mPeekDocno = mPeekPosting.getDocno();
				mPeekScore = mPeekPosting.getScore();

				return mPeekScore;
			}

			return mPeekScore;
		}

		public int peekNextDocno() {
			if (mPeekDocno == 0) {
				if (!nextPosting(mPeekPosting)) {
					throw new RuntimeException("Reached end of postings!");
				}

				mPeekDocno = mPeekPosting.getDocno();
				mPeekScore = mPeekPosting.getScore();

				return mPeekDocno;
			}

			return mPeekDocno;
		}

		public PostingsList getPostingsList() {
			return mPostingsList;
		}
	}
}
