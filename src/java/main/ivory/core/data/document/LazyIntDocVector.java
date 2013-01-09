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
import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.index.TermPositions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.io.WritableUtils;

/**
 * Implementation of {@link IntDocVector} that lazily decodes term and
 * positional information on demand.
 *
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class LazyIntDocVector implements IntDocVector {
  private SortedMap<Integer, int[]> termPositionsMap = null;
  private byte[] bytes = null;
  private int numTerms;

  private transient ByteArrayOutputStream bytesOut = null;
  private transient BitOutputStream bitsOut = null;

  public LazyIntDocVector() {}

  public LazyIntDocVector(SortedMap<Integer, int[]> termPositionsMap) {
    this.termPositionsMap = termPositionsMap;
  }

  public void setTermPositionsMap(SortedMap<Integer, int[]> termPositionsMap) {
    this.termPositionsMap = termPositionsMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (bytes != null) {
      // This would happen if we're reading in an already-encoded
      // doc vector; if that's the case, simply write out the byte array
      WritableUtils.writeVInt(out, numTerms);
      writeRawBytes(out);
    } else if (termPositionsMap != null) {
      writeTermPositionsMap(out);
    } else {
      throw new RuntimeException("Unable to write LazyIntDocVector!");
    }
  }

  private void writeRawBytes(DataOutput out) {
    try {
      WritableUtils.writeVInt(out, bytes.length);
      out.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException("Error writing LazyIntDocVector raw bytes");
    }
  }

  private void writeTermPositionsMap(DataOutput out) {
    try {
      numTerms = termPositionsMap.size();

      // Write # of terms.
      WritableUtils.writeVInt(out, numTerms);
      if (numTerms == 0)
        return;

      bytesOut = new ByteArrayOutputStream();
      bitsOut = new BitOutputStream(bytesOut);

      Iterator<Map.Entry<Integer, int[]>> it = termPositionsMap.entrySet().iterator();
      Map.Entry<Integer, int[]> posting = it.next();
      int[] positions = posting.getValue();
      TermPositions tp = new TermPositions();
      // Write out the first termid.
      int lastTerm = posting.getKey().intValue();
      bitsOut.writeBinary(32, lastTerm);
      // Write out the tf value.
      bitsOut.writeGamma((short) positions.length);
      tp.set(positions, (short) positions.length);
      // Write out the positions.
      writePositions(bitsOut, tp);

      int curTerm;
      while (it.hasNext()) {
        posting = it.next();
        curTerm = posting.getKey().intValue();
        positions = posting.getValue();
        int tgap = curTerm - lastTerm;
        if (tgap <= 0) {
          throw new RuntimeException("Error: encountered invalid t-gap. termid=" + curTerm);
        }
        // Write out the gap.
        bitsOut.writeGamma(tgap);
        tp.set(positions, (short) positions.length);
        // Write out the tf value.
        bitsOut.writeGamma((short) positions.length);
        // Write out the positions.
        writePositions(bitsOut, tp);
        lastTerm = curTerm;
      }

      bitsOut.padAndFlush();
      bitsOut.close();
      byte[] bytes = bytesOut.toByteArray();
      WritableUtils.writeVInt(out, bytes.length);
      out.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException("Error writing LazyIntDocVector term positions map", e);
    } catch (ArithmeticException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numTerms = WritableUtils.readVInt(in);
    if (numTerms == 0) {
      bytes = null;
      return;
    }
    bytes = new byte[WritableUtils.readVInt(in)];
    in.readFully(bytes);
  }

  // Passing in docno and tf basically for error checking purposes.
  protected static void writePositions(BitOutputStream t, TermPositions p) throws IOException {
    int[] pos = p.getPositions();

    if (p.getTf() == 1) {
      // If tf=1, just write out the single term position.
      t.writeGamma(pos[0]);
    } else {
      // If tf > 1, write out skip information if we want to bypass the
      // positional information during decoding.
      t.writeGamma(p.getEncodedSize());

      // Keep track of where we are in the stream.
      int skip1 = (int) t.getByteOffset() * 8 + t.getBitOffset();

      // Write out first position.
      t.writeGamma(pos[0]);
      // Write out rest of positions using p-gaps (first order positional differences).
      for (int c = 1; c < p.getTf(); c++) {
        int pgap = pos[c] - pos[c - 1];
        if (pos[c] <= 0 || pgap == 0) {
          throw new RuntimeException("Error: invalid term positions. positions=" + p.toString());
        }
        t.writeGamma(pgap);
      }

      // Find out where we are in the stream now.
      int skip2 = (int) t.getByteOffset() * 8 + t.getBitOffset();

      // Verify that the skip information is indeed valid.
      if (skip1 + p.getEncodedSize() != skip2) {
        throw new RuntimeException("Ivalid skip information: skip_pos1=" + skip1
            + ", skip_pos2=" + skip2 + ", size=" + p.getEncodedSize());
      }
    }
  }

  @Override
  public String toString() {
    StringBuffer s = new StringBuffer("[");
    try {
      Reader r = this.getReader();
      while (r.hasMoreTerms()) {
        int id = r.nextTerm();
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

  public String toStringWithTerms(DefaultFrequencySortedDictionary map) {
    StringBuffer s = new StringBuffer("");
    try {
      Reader r = this.getReader();
      while (r.hasMoreTerms()) {
        int id = r.nextTerm();
        TermPositions pos = new TermPositions();
        r.getPositions(pos);
        s.append(String.format("(%d, %d, %s)", map.getTerm(id), pos.getTf(), pos));
      }
      s.append("]");
    } catch (Exception e) {
      e.printStackTrace();
    }
    return s.toString();
  }

  @Override
  public Reader getReader() throws IOException {
    return new Reader(bytes, numTerms);
  }

  public static class Reader implements IntDocVector.Reader {
    private ByteArrayInputStream bytesIn;
    private BitInputStream bitsIn;
    private int p = -1;
    private int prevTermID = -1;
    private short prevTf = -1;
    private int termCnt;
    private boolean needToReadPositions = false;

    public Reader(byte[] bytes, int n) throws IOException {
      this.termCnt = n;
      if (termCnt > 0) {
        bytesIn = new ByteArrayInputStream(bytes);
        bitsIn = new BitInputStream(bytesIn);
      }
    }

    @Override
    public int getNumberOfTerms() {
      return termCnt;
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
        throw new RuntimeException(e);
      }
    }

    @Override
    public int nextTerm() {
      int id = -1;
      try {
        p++;
        if (needToReadPositions) {
          skipPositions(prevTf);
        }
        needToReadPositions = true;
        if (p == 0) {
          prevTermID = bitsIn.readBinary(32);
          prevTf = (short) bitsIn.readGamma();
          return prevTermID;
        } else {
          if (p > termCnt - 1) {
            return -1;
          }
          id = bitsIn.readGamma() + prevTermID;
          prevTermID = id;
          prevTf = (short) bitsIn.readGamma();
          return id;
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException();
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
        throw new RuntimeException("Error reading bits:", e);
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
      return !(p >= termCnt - 1);
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
