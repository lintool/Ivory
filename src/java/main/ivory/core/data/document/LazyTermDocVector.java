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

package ivory.core.data.document;

import ivory.core.index.TermPositions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.WritableUtils;

import uk.ac.gla.terrier.compression.BitInputStream;
import uk.ac.gla.terrier.compression.BitOutputStream;
import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * Implementation of {@link TermDocVector} that lazily decodes term and
 * positional information on demand.
 *
 * @author Tamer Elsayed
 */
public class LazyTermDocVector implements TermDocVector {

  // Term to list of positions
  // notice that this is an ArrayListOfInts, not ArrayList<Integer>
  private Map<String, ArrayListOfInts> termPositionsMap = null;
  private byte[] mRawBytes = null;
  private String[] mTerms = null;
  private int nTerms;
  private static boolean read = false;

  transient private ByteArrayOutputStream mBytesOut = null;
  transient private BitOutputStream mBitsOut = null;

  public LazyTermDocVector() {}

  public LazyTermDocVector(Map<String, ArrayListOfInts> termPositionsMap) {
    this.termPositionsMap = termPositionsMap;
    read = false;
  }

  public void setTermPositionsMap(Map<String, ArrayListOfInts> termPositionsMap) {
    this.termPositionsMap = termPositionsMap;
    read = false;
  }

  public int getNum() {
    return nTerms;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (!read) {
      nTerms = termPositionsMap.size();
      // write # of terms
      WritableUtils.writeVInt(out, nTerms);
      if (nTerms == 0)
        return;

      try {
        mBytesOut = new ByteArrayOutputStream();
        mBitsOut = new BitOutputStream(mBytesOut);

        ArrayListOfInts positions;
        TermPositions tp = new TermPositions();
        String term;

        for (Map.Entry<String, ArrayListOfInts> posting : termPositionsMap.entrySet()) {
          term = posting.getKey();
          positions = posting.getValue();
          tp.set(positions.getArray(), (short) positions.size());

          // write the term
          out.writeUTF(term);
          // write out the tf value
          mBitsOut.writeGamma((short) positions.size());
          // write out the positions
          LazyIntDocVector.writePositions(mBitsOut, tp);
        }
        mBitsOut.padAndFlush();
        mBitsOut.close();
        byte[] bytes = mBytesOut.toByteArray();
        WritableUtils.writeVInt(out, bytes.length);
        out.write(bytes);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error adding postings.");
      } catch (ArithmeticException e) {
        e.printStackTrace();
        throw new RuntimeException("ArithmeticException caught \"" + e.getMessage());
      }

    } else {
      WritableUtils.writeVInt(out, nTerms);
      if (nTerms == 0)
        return;

      for (int i = 0; i < nTerms; i++)
        out.writeUTF(mTerms[i]);

      WritableUtils.writeVInt(out, mRawBytes.length);
      out.write(mRawBytes);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    read = true;
    nTerms = WritableUtils.readVInt(in);
    if (nTerms == 0) {
      mRawBytes = null;
      mTerms = null;
      return;
    }
    mTerms = new String[nTerms];
    for (int i = 0; i < nTerms; i++) {
      mTerms[i] = in.readUTF();
    }
    mRawBytes = new byte[WritableUtils.readVInt(in)];
    in.readFully(mRawBytes);
  }

  @Override
  public String toString() {
    StringBuffer s = new StringBuffer(this.getClass().getName() + "," + nTerms + ","
        + mRawBytes + "," + mTerms + "\n" + "[");
    try {
      Reader r = this.getReader();
      while (r.hasMoreTerms()) {
        String id = r.nextTerm();
        TermPositions pos = new TermPositions();
        r.getPositions(pos);
        s.append("(" + id + ", " + pos.getTf() + ", " + pos + ")");
      }
      s.append("]");
    } catch (Exception e) {
      e.printStackTrace();
    }
    return s.toString();
  }

  @Override
  public Reader getReader() throws IOException {
    return new Reader(nTerms, mRawBytes, mTerms);
  }

  public static class Reader implements TermDocVector.Reader {
    private ByteArrayInputStream mBytesIn;
    private BitInputStream mBitsIn;
    private int p = -1;
    private short mPrevTf = -1;
    private int nTerms;
    private boolean mNeedToReadPositions = false;
    private String[] mTerms = null;

    public Reader(int nTerms, byte[] bytes, String[] terms) throws IOException {
      this.nTerms = nTerms;
      if (nTerms > 0) {
        mBytesIn = new ByteArrayInputStream(bytes);
        mBitsIn = new BitInputStream(mBytesIn);
        mTerms = terms;
      }
    }

    @Override
    public int getNumberOfTerms() {
      return nTerms;
    }

    @Override
    public short getTf() {
      return mPrevTf;
    }

    @Override
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

    @Override
    public String nextTerm() {
      try {
        p++;
        if (mNeedToReadPositions) {
          skipPositions(mPrevTf);
        }
        mNeedToReadPositions = true;
        if (p <= nTerms - 1) {
          mPrevTf = (short) mBitsIn.readGamma();
          return mTerms[p];
        } else
          return null;
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException();
      }
    }

    @Override
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

    @Override
    public boolean getPositions(TermPositions tp) {
      int[] pos = getPositions();

      if (pos == null)
        return false;

      tp.set(pos, (short) pos.length);
      return true;
    }

    @Override
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
}
