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

package ivory.core.data.index;

import ivory.core.compression.BitInputStream;
import ivory.core.compression.BitOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A Hadoop {@code Writable} that encodes the position of term occurrences within a document. Term
 * occurrences are represented as an array of ints, where each int represents a term position. These
 * objects serve as intermediate values in building document-sorted inverted indexes.
 * </p>
 *
 * <p>
 * In serialized form, term positions are represented as first-order differences (i.e., position
 * gaps or <i>p</i>-gaps) using Gamma encoding. As an example, let's say a term has a term frequency
 * of 5, at token positions [3, 53, 58, 90, 101]. Such an object would be encoded as the following
 * sequence of ints: 3, 50, 5, 32, 11, each of which is expressed using Gamma codes. Every int
 * except the first represents the difference between the previous term position and the current
 * term position.
 * </p>
 *
 * @author Jimmy Lin
 */
public class TermPositions implements Writable {
  private int[] positions;
  private byte[] bytes;
  private short tf;
  private int totalBits;

  /**
   * Creates an empty {@code TermPositions} object.
   */
  public TermPositions() {}

  /**
   * Creates a {@code TermPositions} object with initial parameters. Note that the length of the
   * term positions array does not need to be the term frequency; this supports reusing arrays of
   * mismatching sizes.
   *
   * @param pos array of term positions
   * @param tf the term frequency
   */
  public TermPositions(int[] pos, short tf) {
    Preconditions.checkArgument(tf > 0);
    this.positions = Preconditions.checkNotNull(pos);
    this.tf = tf;
  }

  /**
   * Sets the term positions and term frequency of this object. Note that the length of the term
   * positions array does not need to be the term frequency; this supports reusing arrays of
   * mismatching sizes.
   *
   * @param pos array of term positions
   * @param tf the term frequency
   */
  public void set(int[] pos, short tf) {
    Preconditions.checkArgument(tf > 0);
    this.positions = Preconditions.checkNotNull(pos);
    this.tf = tf;
    // Reset so we will recompute encoded size.
    totalBits = 0;
  }

  /**
   * Deserializes this object.
   *
   * @param in data source
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    positions = null;
    bytes = new byte[in.readInt()];
    tf = in.readShort();
    totalBits = in.readInt();
    in.readFully(bytes);

    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    BitInputStream bitStream = new BitInputStream(byteStream);

    positions = new int[tf];
    for (int i = 0; i < tf; i++) {
      if (i == 0) {
        positions[i] = bitStream.readGamma();
      } else {
        positions[i] = (positions[i - 1] + bitStream.readGamma());
      }
    }
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the serialized representation
   */
  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    BitOutputStream t = new BitOutputStream(b);
    for (int i = 0; i < tf; i++) {
      if (i == 0) {
        t.writeGamma(positions[0]);
      } else {
        int pgap = positions[i] - positions[i - 1];
        if (positions[i] <= 0 || pgap == 0) {
          throw new RuntimeException("Error: invalid term positions " + toString());
        }

        t.writeGamma(pgap);
      }
    }

    int bitOffset = t.getBitOffset();
    int byteOffset = (int) t.getByteOffset();
    t.padAndFlush();
    t.close();

    byte[] bytes = b.toByteArray();
    out.writeInt(bytes.length);
    out.writeShort(tf);
    out.writeInt(byteOffset * 8 + bitOffset);
    out.write(bytes);
  }

  /**
   * Serializes this object and returns the raw serialized form in a byte array.
   *
   * @return raw serialized representation
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Factory method for creating {@code TermPositions} objects.
   *
   * @param in source to read from
   * @return newly created {@code TermPositions} object
   * @throws IOException
   */
  public static TermPositions create(DataInput in) throws IOException {
    TermPositions p = new TermPositions();
    p.readFields(in);

    return p;
  }

  /**
   * Factory method for creating {@code TermPositions} objects.
   *
   * @param bytes raw serialized form
   * @return newly created {@code TermPositions} object
   * @throws IOException
   */
  public static TermPositions create(byte[] bytes) throws IOException {
    return TermPositions.create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * Returns the array of term positions.
   * 
   * @return array of term positions
   */
  public int[] getPositions() {
    return positions;
  }

  /**
   * Returns the term frequency.
   * 
   * @return term frequency
   */
  public short getTf() {
    return tf;
  }

  /**
   * Returns the size (in bits) of serialized form of this object.
   *
   * @return size in bits of the serialized object
   */
  public int getEncodedSize() {
    // If this is a newly created object, then we haven't computed the encoded size yet, since this
    // is done as part of the deserialization process... if this is the case, then run through a
    // mock encoding to compute the encoded size.
    if (totalBits == 0) {
      try {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        BitOutputStream t = new BitOutputStream(b);
        for (int i = 0; i < tf; i++) {
          if (i == 0) {
            t.writeGamma(positions[0]);
          } else {
            int pgap = positions[i] - positions[i - 1];
            if (positions[i] <= 0 || pgap == 0) {
              throw new RuntimeException("Error: invalid term positions " + toString());
            }

            t.writeGamma(pgap);
          }
        }

        int bitOffset = t.getBitOffset();
        int byteOffset = (int) t.getByteOffset();
        t.padAndFlush();
        t.close();

        totalBits = byteOffset * 8 + bitOffset;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return totalBits;
  }

  /**
   * Generates a human-readable String representation of this object.
   *
   * @return human-readable String representation of this object
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    for (int i = 0; i < tf; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(positions[i]);
    }
    sb.append("]");

    return sb.toString();
  }

  /**
   * Returns a shallow copy of this object. Note that the underlying int array is not duplicated.
   *
   * @return shallow copy of this object
   */
  @Override
  public TermPositions clone() {
    TermPositions that = new TermPositions();
    that.set(positions, tf);

    return that;
  }
}
