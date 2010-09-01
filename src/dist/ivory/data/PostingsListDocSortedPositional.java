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
import java.util.ArrayList;
import java.util.Comparator;

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
 * @author Tamer Elsayed
 * 
 */

public class PostingsListDocSortedPositional implements PostingsList {
	private static final Logger sLogger = Logger.getLogger(PostingsListDocSortedPositional.class);
	private static final int MAX_DOCNO_BITS = 32;

	static {
		sLogger.setLevel(Level.WARN);
	}

	private int mCollectionDocumentCount = -1;
	private int mNumPostings = -1;
	private int mGolombParam;
	private int mPrevDocno;
	private byte[] mRawBytes;
	private int mPostingsAdded;
	private long mSumOfPostingsScore;

	private int mDf;
	private long mCf;

	transient private ByteArrayOutputStream mBytesOut;
	transient private BitOutputStream mBitsOut;

	public PostingsListDocSortedPositional() {
		this.mSumOfPostingsScore = 0;
		this.mPostingsAdded = 0;
		this.mDf = 0;
		this.mCf = 0;
		this.mPrevDocno = -1;

		try {
			mBytesOut = new ByteArrayOutputStream();
			mBitsOut = new BitOutputStream(mBytesOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void clear() {
		mSumOfPostingsScore = 0;
		mPostingsAdded = 0;
		mDf = 0;
		mCf = 0;
		mPrevDocno = -1;
		mNumPostings = -1;
		mRawBytes = null;
		try {
			mBytesOut = new ByteArrayOutputStream();
			mBitsOut = new BitOutputStream(mBytesOut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void add(int docno, short score, TermPositions pos) {
		sLogger.info("adding posting: " + docno + ", " + score + ", " + pos.toString());
		if (pos.getPositions().length == 0) {
			throw new RuntimeException("Error: encountered invalid number of positions = 0");
		}
		if (score != pos.getTf()) {
			throw new RuntimeException("Error: tf and number of positions don't match. docno="
					+ docno + ", tf=" + score + ", positions=" + pos.toString());
		}
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
					+ ", collection size=" + mCollectionDocumentCount + ", Golomb param=" + mGolombParam);
		}

		mPostingsAdded++;
		mSumOfPostingsScore += score;
	}

	// passing in docno and tf basically for error checking purposes
	private static void writePositions(BitOutputStream t, TermPositions p, int docno, short tf)
			throws IOException {
		int[] pos = p.getPositions();
		/*for(int i = 0; i < pos.length; i++){
			if(pos[i]<=0){
				throw new RuntimeException("Invalid Pos #"+i+": "+pos[i]
						+ ", doc: "+docno + ", tf=" + tf + ", positions=" + p.toString());
			}
		}*/
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

			if (pos[0] <= 0) {
				throw new RuntimeException("Error: invalid term positions. positions="
						+ p.toString() + ", docno=" + docno + ", tf=" + tf);
			}
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
			if(mCollectionDocumentCount<=0) 
				throw new RuntimeException("Invalid Collection Document Count: "+mCollectionDocumentCount);
			if(mRawBytes == null) 
				throw new RuntimeException("Invalid rawBytes .. Postings must be serialized!!");
			if(mPostingsAdded<=0)
				throw new RuntimeException("Invalid number of postings: "+mPostingsAdded);
			return new PostingsReader(mRawBytes, mPostingsAdded, mCollectionDocumentCount, this);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public byte[] getRawBytes() {
		return mRawBytes;
	}

	public void setCollectionDocumentCount(int docs) {
		if(docs<=0) 
			throw new RuntimeException("Invalid Collection Document Count: "+mCollectionDocumentCount);
		mCollectionDocumentCount = docs;
		recomputeGolombParameter();
	}

	public int getCollectionDocumentCount() {
		return mCollectionDocumentCount;
	}

	public void setNumberOfPostings(int n) {
		mNumPostings = n;
		recomputeGolombParameter();
	}

	public int getNumberOfPostings() {
		return mNumPostings;
	}

	private void recomputeGolombParameter() {
		mGolombParam = (int) Math.ceil(0.69 * ((float) mCollectionDocumentCount) / (float) mNumPostings);
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
				mBitsOut.padAndFlush();
				mBitsOut.close();

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

			sLogger
					.info("writing postings: cf=" + mSumOfPostingsScore + ", df="
							+ mNumPostings);
		}
	}

	public byte[] serialize() throws IOException {
		if(mPostingsAdded<=0)
			throw new RuntimeException("Invalid number of added postings: "+mPostingsAdded+" !! nPostings="+mNumPostings +", CollSize="+ mCollectionDocumentCount);
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
	 * <p>
	 * PostingsReader for PostingsListDocSortedPositional.
	 * </p>
	 * 
	 * @author Jimmy Lin
	 * 
	 */
	public static class PostingsReader implements ivory.data.PostingsReader {
		private ByteArrayInputStream mBytesIn;
		private BitInputStream mBitsIn;
		private int mCnt = 0;
		private int mPrevDocno;
		private short mPrevTf;
		private int [] mCurPositions;
		private int mInnerNumPostings;
		private int mInnerGolombParam;
		private int mInnerCollectionSize;
		private boolean mNeedToReadPositions = false;
		private PostingsList mPostingsList;

		public PostingsReader(byte[] bytes, int nPostings, int collectionSize, PostingsListDocSortedPositional list)
				throws IOException {
			mBytesIn = new ByteArrayInputStream(bytes);
			mBitsIn = new BitInputStream(mBytesIn);
			if(nPostings<=0)
				throw new RuntimeException("Invalid number of postings: "+nPostings);
			mInnerNumPostings = nPostings;
			if(collectionSize<=0)
				throw new RuntimeException("Invalid Collection size: "+collectionSize);
			mInnerCollectionSize = collectionSize;
			mInnerGolombParam = (int) Math.ceil(0.69 * ((float) mInnerCollectionSize)
					/ (float) mInnerNumPostings);
			mPostingsList = list;
			mNeedToReadPositions = false;
		}

		public int getNumberOfPostings() {
			return mInnerNumPostings;
		}

		public void reset() {
			try {
				mBytesIn.reset();
				mBitsIn = new BitInputStream(mBytesIn);
				mCnt = 0;
				mNeedToReadPositions = false;
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
				throw new RuntimeException("Error in reading posting: mCnt=" + mCnt + ", mInnerNumPostings=" + mInnerNumPostings + ", "+e);
			}
			
			mCnt++;
			mPrevDocno = p.getDocno();
			mPrevTf = p.getScore();
			mCurPositions = null;
			mNeedToReadPositions = true;

			return true;
		}

		public int[] getPositions() {
			if(mCurPositions != null) {
				return mCurPositions;
			}

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
				throw new RuntimeException("A problem in reading bits! "+e);
				//return null;
			}

			mNeedToReadPositions = false;
			mCurPositions = pos;
			
			return pos;
		}

		public boolean getPositions(TermPositions tp) {
			int[] pos = getPositions();

			if (pos == null)
				return false;

			tp.set(pos, (short) pos.length);

			return true;
		}

		public boolean hasMorePostings() {
			return !(mCnt >= mInnerNumPostings);
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

		public int getDocno() {
			//System.err.println("[term] getDocno() = " + mPrevDocno);
			return mPrevDocno;
		}

		public short getScore() {
			return mPrevTf;
		}
	}

	public static PostingsListDocSortedPositional merge(PostingsListDocSortedPositional plist1,
			PostingsListDocSortedPositional plist2, int docs) {

		plist1.setCollectionDocumentCount(docs);
		plist2.setCollectionDocumentCount(docs);

		int numPostings1 = plist1.getNumberOfPostings();
		int numPostings2 = plist2.getNumberOfPostings();

		//System.out.println("number of postings (1): " + numPostings1);
		//System.out.println("number of postings (2): " + numPostings2);

		PostingsListDocSortedPositional newPostings = new PostingsListDocSortedPositional();
		newPostings.setCollectionDocumentCount(docs);
		newPostings.setNumberOfPostings(numPostings1 + numPostings2);

		Posting posting1 = new Posting();
		PostingsReader reader1 = plist1.getPostingsReader();

		Posting posting2 = new Posting();
		PostingsReader reader2 = plist2.getPostingsReader();

		reader1.nextPosting(posting1);
		reader2.nextPosting(posting2);

		TermPositions tp1 = new TermPositions();
		TermPositions tp2 = new TermPositions();

		reader1.getPositions(tp1);
		reader2.getPositions(tp2);

		while (true) {
			if (posting1 == null) {
				newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);
				//System.out.println("2: " + posting2);

				// read the rest from reader 2
				while (reader2.nextPosting(posting2)) {
					reader2.getPositions(tp2);
					newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);
					//System.out.println("2: " + posting2);
				}

				break;
			} else if (posting2 == null) {
				newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);
				//System.out.println("1: " + posting1);

				// read the rest from reader 1
				while (reader1.nextPosting(posting1)) {
					reader1.getPositions(tp1);
					newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);
					//System.out.println("1: " + posting1);
				}

				break;

			} else if (posting1.getDocno() < posting2.getDocno()) {
				//System.out.println("1: " + posting1);
				newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);

				if (reader1.nextPosting(posting1) == false) {
					posting1 = null;
				} else {
					reader1.getPositions(tp1);
				}
			} else {
				//System.out.println("2: " + posting2);
				newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);

				if (reader2.nextPosting(posting2) == false) {
					posting2 = null;
				} else {
					reader2.getPositions(tp2);
				}
			}
		}

		return newPostings;
	}

	public static PostingsListDocSortedPositional merge(PostingsList plist1,
			PostingsList plist2, int docs) {

		plist1.setCollectionDocumentCount(docs);
		plist2.setCollectionDocumentCount(docs);

		int numPostings1 = plist1.getNumberOfPostings();
		int numPostings2 = plist2.getNumberOfPostings();

		//System.out.println("number of postings (1): " + numPostings1);
		//System.out.println("number of postings (2): " + numPostings2);

		PostingsListDocSortedPositional newPostings = new PostingsListDocSortedPositional();
		newPostings.setCollectionDocumentCount(docs);
		newPostings.setNumberOfPostings(numPostings1 + numPostings2);

		Posting posting1 = new Posting();
		ivory.data.PostingsReader reader1 = plist1.getPostingsReader();

		Posting posting2 = new Posting();
		ivory.data.PostingsReader reader2 = plist2.getPostingsReader();

		reader1.nextPosting(posting1);
		reader2.nextPosting(posting2);

		TermPositions tp1 = new TermPositions();
		TermPositions tp2 = new TermPositions();

		reader1.getPositions(tp1);
		reader2.getPositions(tp2);

		while (true) {
			if (posting1 == null) {
				newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);
				//System.out.println("2: " + posting2);

				// read the rest from reader 2
				while (reader2.nextPosting(posting2)) {
					reader2.getPositions(tp2);
					newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);
					//System.out.println("2: " + posting2);
				}

				break;
			} else if (posting2 == null) {
				newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);
				//System.out.println("1: " + posting1);

				// read the rest from reader 1
				while (reader1.nextPosting(posting1)) {
					reader1.getPositions(tp1);
					newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);
					//System.out.println("1: " + posting1);
				}

				break;

			} else if (posting1.getDocno() < posting2.getDocno()) {
				//System.out.println("1: " + posting1);
				newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);

				if (reader1.nextPosting(posting1) == false) {
					posting1 = null;
				} else {
					reader1.getPositions(tp1);
				}
			} else {
				//System.out.println("2: " + posting2);
				newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);

				if (reader2.nextPosting(posting2) == false) {
					posting2 = null;
				} else {
					reader2.getPositions(tp2);
				}
			}
		}

		return newPostings;
	}

	public static class DocList{
		public int id;
		public int listIndex;
		/**
		 * @param id
		 * @param listIndex
		 */
		public DocList(int id, int listIndex) {
			this.id = id;
			this.listIndex = listIndex;
		}
		
		public void set(int id, int listIndex) {
			this.id = id;
			this.listIndex = listIndex;
		}
		
		@Override
		public String toString() {
			return "{"+id+" - "+listIndex+"}";
		}

	}
	
	public static class DocListComparator implements Comparator<DocList>{

		public int compare(DocList t1, DocList t2) {
			if(t1.id < t2.id) return -1;
			else if(t1.id > t2.id) return 1;
			return 0;
		}
	}
	
	private static DocListComparator comparator = new DocListComparator();
	
	public static void mergeList(PostingsList newPostings, ArrayList<PostingsList> list, int nCollDocs){
		//sLogger.setLevel(Level.INFO);
		
		int nLists = list.size();
		
		// a reader for each pl
		ivory.data.PostingsReader[] reader = new PostingsReader[nLists];
		
		// the cur posting of each list
		Posting[] posting = new Posting[nLists];
		
		// the cur positions of each list
		TermPositions[] tp = new TermPositions[nLists];
		
		// min-heap for merging
		java.util.PriorityQueue<DocList> heap = new java.util.PriorityQueue<DocList>(nLists, comparator);
		
		int totalPostings = 0;
		int i = 0;
		for(PostingsList pl : list){
			pl.setCollectionDocumentCount(nCollDocs);
			
			totalPostings += pl.getNumberOfPostings();
			
			reader[i] = pl.getPostingsReader();
			
			posting[i] = new Posting();
			reader[i].nextPosting(posting[i]);
			
			tp[i] = new TermPositions();
			reader[i].getPositions(tp[i]);
			heap.add(new DocList(posting[i].getDocno(), i));
			
			i++;
		}
		sLogger.info(">> merging a list of "+list.size()+" partial lists");
		newPostings.setCollectionDocumentCount(nCollDocs);
		newPostings.setNumberOfPostings(totalPostings);
		sLogger.info("\ttotalPostings: "+totalPostings);
		//sLogger.info("Total # of lists = " + nLists);
		//sLogger.info("Total # of postings = " + totalPostings);

		DocList dl; 
		while (heap.size() > 0) {
			//sLogger.info("Heap size: "+heap.size()+" peek = "+heap.peek());
			dl = heap.remove();
			i = dl.listIndex;
			newPostings.add(dl.id, posting[i].getScore(), tp[i]);
			/*k++;
			sLogger.info("==Added posting #"+k);
			sLogger.info("\t"+posting[i]);
			sLogger.info("\t"+tp[i]);*/
			if(reader[i].nextPosting(posting[i])){
				reader[i].getPositions(tp[i]);
				dl.set(posting[i].getDocno(), i);
				heap.add(dl);
			}
		}
		sLogger.info("\tdone.");
		//sLogger.setLevel(Level.WARN);
	}
	
	
	public static void main(String[] args) throws Exception{
		
		sLogger.setLevel(Level.INFO);
		
		ByteArrayOutputStream mBytesOut;
		DataOutputStream mDataOut;
		
		ByteArrayInputStream mBytesIn;
		DataInputStream mDataIn;

		
		ivory.data.PostingsReader r;
		Posting p;
		
		PostingsList pl1, pl2, pl3, pl4, pl5, pl6, pl7, pl8, plTotal;
		
		TermPositions tp = new TermPositions();
		
		int[] pos;
		short tf;
		
		pl1 = new PostingsListDocSortedPositional();
		pl1.setCollectionDocumentCount(100);
		pl1.setNumberOfPostings(3);
		pos = new int[1];
		pos[0] = 5;
		tf = (short)pos.length; tp.set(pos, tf);
		pl1.add(1, tf, tp);
		
		pos = new int[2];
		pos[0] = 2;pos[1] = 3;
		tf = (short)pos.length; tp.set(pos, tf);
		pl1.add(3, tf, tp);
		
		pos = new int[1];
		pos[0] = 4;
		tf = (short)pos.length; tp.set(pos, tf);
		pl1.add(5, tf, tp);
		
		p = new Posting();
		
		
		mBytesOut = new ByteArrayOutputStream();
		mDataOut = new DataOutputStream(mBytesOut);
		
		pl1.write(mDataOut);
		
		mDataOut.flush();
		
		mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
		mDataIn = new DataInputStream(mBytesIn);
		
		
		pl5 = new PostingsListDocSortedPositional();
		pl5.setCollectionDocumentCount(100);
		pl5.readFields(mDataIn);
		r = pl5.getPostingsReader();
		while(r.hasMorePostings()){
			r.nextPosting(p);
			System.out.println(p);
			r.getPositions(tp);
			System.out.println(tp);
		}
		
		
		pl2 = new PostingsListDocSortedPositional();
		pl2.setCollectionDocumentCount(100);
		pl2.setNumberOfPostings(3);
		pos = new int[1];
		pos[0] = 25;
		tf = (short)pos.length; tp.set(pos, tf);
		pl2.add(2, tf, tp);
		
		pos = new int[2];
		pos[0] = 22;pos[1] = 23;
		tf = (short)pos.length; tp.set(pos, tf);
		pl2.add(13, tf, tp);
		
		pos = new int[1];
		pos[0] = 24;
		tf = (short)pos.length; tp.set(pos, tf);
		pl2.add(20, tf, tp);
		
		p = new Posting();
		
		
		mBytesOut = new ByteArrayOutputStream();
		mDataOut = new DataOutputStream(mBytesOut);
		
		pl2.write(mDataOut);
		
		mDataOut.flush();
		
		mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
		mDataIn = new DataInputStream(mBytesIn);
		
		
		pl6 = new PostingsListDocSortedPositional();
		pl6.setCollectionDocumentCount(100);
		pl6.readFields(mDataIn);
		r = pl6.getPostingsReader();
		while(r.hasMorePostings()){
			r.nextPosting(p);
			System.out.println(p);
			r.getPositions(tp);
			System.out.println(tp);
		}

		
		
		pl3 = new PostingsListDocSortedPositional();
		pl3.setCollectionDocumentCount(100);
		pl3.setNumberOfPostings(3);
		pos = new int[1];
		pos[0] = 35;
		tf = (short)pos.length; tp.set(pos, tf);
		pl3.add(12, tf, tp);
		
		pos = new int[2];
		pos[0] = 32;pos[1] = 33;
		tf = (short)pos.length; tp.set(pos, tf);
		pl3.add(14, tf, tp);
		
		pos = new int[1];
		pos[0] = 34;
		tf = (short)pos.length; tp.set(pos, tf);
		pl3.add(26, tf, tp);
		
		p = new Posting();
		
		
		mBytesOut = new ByteArrayOutputStream();
		mDataOut = new DataOutputStream(mBytesOut);
		
		pl3.write(mDataOut);
		
		mDataOut.flush();
		
		mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
		mDataIn = new DataInputStream(mBytesIn);
		
		
		pl7 = new PostingsListDocSortedPositional();
		pl7.setCollectionDocumentCount(100);
		pl7.readFields(mDataIn);
		r = pl7.getPostingsReader();
		while(r.hasMorePostings()){
			r.nextPosting(p);
			System.out.println(p);
			r.getPositions(tp);
			System.out.println(tp);
		}

		
		pl4 = new PostingsListDocSortedPositional();
		pl4.setCollectionDocumentCount(100);
		pl4.setNumberOfPostings(3);
		pos = new int[1];
		pos[0] = 45;
		tf = (short)pos.length; tp.set(pos, tf);
		pl4.add(7, tf, tp);
		
		pos = new int[2];
		pos[0] = 42;pos[1] = 33;
		tf = (short)pos.length; tp.set(pos, tf);
		pl4.add(77, tf, tp);
		
		pos = new int[1];
		pos[0] = 44;
		tf = (short)pos.length; tp.set(pos, tf);
		pl4.add(777, tf, tp);
		
		p = new Posting();
		
		
		mBytesOut = new ByteArrayOutputStream();
		mDataOut = new DataOutputStream(mBytesOut);
		
		pl4.write(mDataOut);
		
		mDataOut.flush();
		
		mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
		mDataIn = new DataInputStream(mBytesIn);
		
		
		pl8 = new PostingsListDocSortedPositional();
		pl8.setCollectionDocumentCount(100);
		pl8.readFields(mDataIn);
		r = pl8.getPostingsReader();
		while(r.hasMorePostings()){
			r.nextPosting(p);
			System.out.println(p);
			r.getPositions(tp);
			System.out.println(tp);
		}
		
		
		ArrayList<PostingsList> list = new ArrayList<PostingsList>();
		list.add(pl5);
		list.add(pl6);
		list.add(pl7);
		list.add(pl8);
		plTotal = new PostingsListDocSortedPositional();
		plTotal.setCollectionDocumentCount(100);
		PostingsListDocSortedPositional.mergeList(plTotal, list, 100);
		
		
		mBytesOut = new ByteArrayOutputStream();
		mDataOut = new DataOutputStream(mBytesOut);
		
		plTotal.write(mDataOut);
		
		mDataOut.flush();
		
		mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
		mDataIn = new DataInputStream(mBytesIn);
		
		pl1 = new PostingsListDocSortedPositional();
		pl1.setCollectionDocumentCount(100);
		pl1.readFields(mDataIn);
		r = pl1.getPostingsReader();
		while(r.hasMorePostings()){
			r.nextPosting(p);
			System.out.println(p);
			r.getPositions(tp);
			System.out.println(tp);
		}
	}
}
