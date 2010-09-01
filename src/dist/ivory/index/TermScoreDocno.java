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

package ivory.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * <p>
 * A Hadoop <code>WritableComparable</code> that represents a term, a score,
 * and a docno with the score. These objects serve as intermediate keys in
 * building score-sorted inverted indexes. Most frequently, these scores are
 * term frequencies, although they can be impact scores in a impact-sorted
 * index.
 * </p>
 * 
 * <p>
 * Note, this class is currently not used by Ivory since there is no functional
 * score-sorted inverted index builder.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class TermScoreDocno implements WritableComparable {

	private String mTerm;
	private int mDocno;
	private short mScore;

	/**
	 * Creates an empty <code>TermScoreDocno</code> object.
	 */
	public TermScoreDocno() {
	}

	/**
	 * Creates a <code>TermScoreDocno</code> with specific initial values.
	 */
	public TermScoreDocno(String term, short score, int docno) {
		this.mTerm = term;
		this.mScore = score;
		this.mDocno = docno;
	}

	/**
	 * Sets the term, score, and docno of this object.
	 */
	public void set(String term, short score, int docno) {
		this.mTerm = term;
		this.mScore = score;
		this.mDocno = docno;
	}

	/**
	 * Returns the term.
	 */
	public String getTerm() {
		return mTerm;
	}

	/**
	 * Returns the score.
	 */
	public short getScore() {
		return mScore;
	}

	/**
	 * Returns the docno.
	 */
	public int getDocno() {
		return mDocno;
	}

	/**
	 * Deserializes this object.
	 */
	public void readFields(DataInput in) throws IOException {
		mTerm = in.readUTF();
		mScore = in.readShort();
		mDocno = in.readInt();
	}

	/**
	 * Serializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(mTerm);
		out.writeShort(mScore);
		out.writeInt(mDocno);
	}

	/**
	 * Clones this object.
	 */
	public TermScoreDocno clone() {
		return new TermScoreDocno(this.getTerm(), this.getScore(), this.getDocno());
	}

	/**
	 * Generates a human-readable String representation of this object.
	 */
	public String toString() {
		return "(" + mTerm + ", " + mScore + ", " + mDocno + ")";
	}

	/**
	 * Defines a sort order. Objects are sorted first by term, and then by
	 * score, and finally by docno.
	 * 
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(Object obj) {
		TermScoreDocno that = (TermScoreDocno) obj;

		if (mTerm.equals(that.mTerm)) {
			if (this.mScore == that.mScore) {
				if (this.mDocno == that.mDocno)
					return 0;

				return this.mDocno > that.mDocno ? 1 : -1;
			}

			return this.mScore > that.mScore ? -1 : 1;
		}

		return mTerm.compareTo(that.getTerm());
	}

	/**
	 * Comparator optimized for <code>TermScoreDocno</code> objects.
	 * 
	 * @author Jimmy Lin
	 */
	public static class Comparator extends WritableComparator {

		/**
		 * Creates a new <code>Comparator</code> for
		 * <code>TermScoreDocno</code> objects.
		 */
		public Comparator() {
			super(TermScoreDocno.class);
		}

		/**
		 * Optimization hook.
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				String thisTerm = readUTF(b1, s1);
				String thatTerm = readUTF(b2, s2);

				if (thisTerm.equals(thatTerm)) {
					int s1offset = readUnsignedShort(b1, s1);
					int s2offset = readUnsignedShort(b2, s2);

					int thisScore = readUnsignedShort(b1, s1 + 2 + s1offset);
					int thatScore = readUnsignedShort(b2, s2 + 2 + s2offset);

					if (thisScore == thatScore) {
						int thisDocno = readInt(b1, s1 + 4 + s1offset);
						int thatDocno = readInt(b2, s2 + 4 + s2offset);

						if (thisDocno == thatDocno)
							return 0;

						return thisDocno > thatDocno ? 1 : -1;
					}

					return thisScore > thatScore ? -1 : 1;
				}

				return thisTerm.compareTo(thatTerm);
			} catch (Exception e) {
				e.printStackTrace();
			}

			return 0;
		}

		private String readUTF(byte[] bytes, int s) throws IOException {
			int utflen = readUnsignedShort(bytes, s);

			byte[] bytearr = new byte[utflen];
			char[] chararr = new char[utflen];

			int c, char2, char3;
			int count = 0;
			int chararr_count = 0;

			System.arraycopy(bytes, s + 2, bytearr, 0, utflen);
			// in.readFully(bytearr, 0, utflen);

			while (count < utflen) {
				c = (int) bytearr[count] & 0xff;
				if (c > 127)
					break;
				count++;
				chararr[chararr_count++] = (char) c;
			}

			while (count < utflen) {
				c = (int) bytearr[count] & 0xff;
				switch (c >> 4) {
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
					/* 0xxxxxxx */
					count++;
					chararr[chararr_count++] = (char) c;
					break;
				case 12:
				case 13:
					/* 110x xxxx 10xx xxxx */
					count += 2;
					if (count > utflen)
						throw new UTFDataFormatException(
								"malformed input: partial character at end");
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80)
						throw new UTFDataFormatException("malformed input around byte " + count);
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx 10xx xxxx 10xx xxxx */
					count += 3;
					if (count > utflen)
						throw new UTFDataFormatException(
								"malformed input: partial character at end");
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
						throw new UTFDataFormatException("malformed input around byte "
								+ (count - 1));
					chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
					break;
				default:
					/* 10xx xxxx, 1111 xxxx */
					throw new UTFDataFormatException("malformed input around byte " + count);
				}
			}
			// The number of chars produced may be less than utflen
			return new String(chararr, 0, chararr_count);
		}

	}

	static { // register this comparator
		WritableComparator.define(TermScoreDocno.class, new Comparator());
	}
}
