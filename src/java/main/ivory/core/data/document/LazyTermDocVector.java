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

import ivory.core.compression.BitInputStream;
import ivory.core.compression.BitOutputStream;
import ivory.core.data.index.TermPositions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.WritableUtils;

import tl.lin.data.array.ArrayListOfInts;

/**
 * Implementation of {@link TermDocVector} that lazily decodes term and
 * positional information on demand.
 *
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class LazyTermDocVector implements TermDocVector {
  private Map<String, ArrayListOfInts> termPositionsMap = null;
  private byte[] rawBytes = null;
  private String[] terms = null;
  private int numTerms;
  private static boolean read = false;

  transient private ByteArrayOutputStream bytesOut = null;
  transient private BitOutputStream bitsOut = null;

  public LazyTermDocVector() {}

  public LazyTermDocVector(Map<String, ArrayListOfInts> termPositionsMap) {
    this.termPositionsMap = termPositionsMap;
    read = false;
  }

  public void setTermPositionsMap(Map<String, ArrayListOfInts> termPositionsMap) {
    this.termPositionsMap = termPositionsMap;
    read = false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (!read) {
      numTerms = termPositionsMap.size();
      // write # of terms
      WritableUtils.writeVInt(out, numTerms);
      if (numTerms == 0)
        return;

      try {
        bytesOut = new ByteArrayOutputStream();
        bitsOut = new BitOutputStream(bytesOut);

        ArrayListOfInts positions;
        TermPositions tp = new TermPositions();
        String term;

        for (Map.Entry<String, ArrayListOfInts> posting : termPositionsMap.entrySet()) {
          term = posting.getKey();
          positions = posting.getValue();
          tp.set(positions.getArray(), (short) positions.size());

          // Write the term.
          out.writeUTF(term);
          // Write out the tf value.
          bitsOut.writeGamma((short) positions.size());
          // Write out the positions.
          LazyIntDocVector.writePositions(bitsOut, tp);
        }
        bitsOut.padAndFlush();
        bitsOut.close();
        byte[] bytes = bytesOut.toByteArray();
        WritableUtils.writeVInt(out, bytes.length);
        out.write(bytes);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error adding postings.");
      } catch (ArithmeticException e) {
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
      }

    } else {
      WritableUtils.writeVInt(out, numTerms);
      if (numTerms == 0)
        return;

      for (int i = 0; i < numTerms; i++)
        out.writeUTF(terms[i]);

      WritableUtils.writeVInt(out, rawBytes.length);
      out.write(rawBytes);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    read = true;
    numTerms = WritableUtils.readVInt(in);
    if (numTerms == 0) {
      rawBytes = null;
      terms = null;
      return;
    }
    terms = new String[numTerms];
    for (int i = 0; i < numTerms; i++) {
      terms[i] = in.readUTF();
    }
    rawBytes = new byte[WritableUtils.readVInt(in)];
    in.readFully(rawBytes);
  }

  @Override
  public String toString() {
    StringBuffer s = new StringBuffer(this.getClass().getName() + "," + numTerms + ","
        + rawBytes + "," + terms + "\n" + "[");
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
    return new Reader(numTerms, rawBytes, terms);
  }

  public static class Reader implements TermDocVector.Reader {
    private ByteArrayInputStream bytesIn;
    private BitInputStream bitsIn;
    private int p = -1;
    private short prevTf = -1;
    private int n;
    private boolean needToReadPositions = false;
    private String[] innerTerms = null;

    public Reader(int nTerms, byte[] bytes, String[] terms) throws IOException {
      this.n = nTerms;
      if (nTerms > 0) {
        bytesIn = new ByteArrayInputStream(bytes);
        bitsIn = new BitInputStream(bytesIn);
        innerTerms = terms;
      }
    }

    @Override
    public int getNumberOfTerms() {
      return n;
    }

    @Override
    public short getTf() {
      return prevTf;
    }

    @Override
    public void reset() {
      try {
        bytesIn.reset();
        bitsIn = new BitInputStream(bytesIn);
        p = -1;
        prevTf = -1;
        needToReadPositions = false;
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error resetting postings.");
      }
    }

    @Override
    public String nextTerm() {
      try {
        p++;
        if (needToReadPositions) {
          skipPositions(prevTf);
        }
        needToReadPositions = true;
        if (p <= n - 1) {
          prevTf = (short) bitsIn.readGamma();
          return innerTerms[p];
        } else {
          return null;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int[] getPositions() {
      int[] pos = null;
      try {
        if (prevTf == 1) {
          pos = new int[1];
          pos[0] = bitsIn.readGamma();
        } else {
          bitsIn.readGamma();
          pos = new int[prevTf];
          pos[0] = bitsIn.readGamma();
          for (int i = 1; i < prevTf; i++) {
            pos[i] = (pos[i - 1] + bitsIn.readGamma());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      needToReadPositions = false;

      return pos;
    }

    @Override
    public boolean getPositions(TermPositions tp) {
      int[] pos = getPositions();

      if (pos == null) {
        return false;
      }

      tp.set(pos, (short) pos.length);
      return true;
    }

    @Override
    public boolean hasMoreTerms() {
      return !(p >= n - 1);
    }

    private void skipPositions(int tf) throws IOException {
      if (tf == 1) {
        bitsIn.readGamma();
      } else {
        bitsIn.skipBits(bitsIn.readGamma());
      }
    }
  }
}
