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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * <p>
 * A Hadoop <code>WritableComparable</code> that represents a term and a
 * document that contains that term. These objects serve as intermediate keys in
 * building document-sorted inverted indexes.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class IntTermDocno implements WritableComparable<IntTermDocno> {

	private int mTerm;
	private int mDocno;

	/**
	 * Creates an empty <code>TermDocno</code> object
	 */
	public IntTermDocno() {
	}

	/**
	 * Creates an <code>TermDocno</code> object with a specific term and
	 * docno.
	 */
	public IntTermDocno(int term, int docno) {
		this.mTerm = term;
		this.mDocno = docno;
	}

	/**
	 * Sets the term and docno of this object.
	 */
	public void set(int term, int docno) {
		this.mTerm = term;
		this.mDocno = docno;
	}

	/**
	 * Returns the term in this object.
	 */
	public int getTerm() {
		return mTerm;
	}

	/**
	 * Returns the docno in this object.
	 */
	public int getDocno() {
		return mDocno;
	}

	/**
	 * Deserializes this object.
	 */
	public void readFields(DataInput in) throws IOException {
		mTerm = in.readInt();
		mDocno = in.readInt();
	}

	/**
	 * Serializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(mTerm);
		out.writeInt(mDocno);
	}

	/**
	 * Clones this object.
	 */
	public IntTermDocno clone() {
		return new IntTermDocno(this.getTerm(), this.getDocno());
	}

	/**
	 * Generates a human-readable String representation of this object.
	 */
	public String toString() {
		return "(" + mTerm + ", " + mDocno + ")";
	}

	/**
	 * Defines a sort order. Objects are sorted first by term, and then by
	 * docno.
	 * 
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(IntTermDocno that) {
		if (mTerm == that.getTerm()) {
			if (this.mDocno == that.getDocno())
				return 0;

			return this.mDocno > that.mDocno ? 1 : -1;
		}

		return mTerm < that.getTerm() ? -1 : 1;
	}

	/** Comparator optimized for <code>IntTermDocno</code>. */
	public static class Comparator extends WritableComparator {

		/**
		 * Creates a new Comparator optimized for <code>PairOfInts</code>.
		 */
		public Comparator() {
			super(IntTermDocno.class);
		}

		/**
		 * Optimization hook.
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int thisLeftValue = readInt(b1, s1);
			int thatLeftValue = readInt(b2, s2);

			if (thisLeftValue == thatLeftValue) {
				int thisRightValue = readInt(b1, s1 + 4);
				int thatRightValue = readInt(b2, s2 + 4);

				return (thisRightValue < thatRightValue ? -1
						: (thisRightValue == thatRightValue ? 0 : 1));

			}

			return (thisLeftValue < thatLeftValue ? -1 : (thisLeftValue == thatLeftValue ? 0 : 1));
		}
	}

	static { // register this comparator
		WritableComparator.define(IntTermDocno.class, new Comparator());
	}
}
