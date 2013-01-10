package ivory.bloomir.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Writable;

/**
 *  Bits is a bit-packet abstraction, containing a number of bits
 *  inside an int array for efficiency
 *
 * @author Nima Asadi
 */
public class Bits implements Writable {
  private static final int UNIT_SIZE = 32;
  private static final int UNIT_SIZE_1 = UNIT_SIZE - 1;
  private static final int UNIT_EXP = 5;
  private int[] bits;
  private int length;

  /**
   * Default constructor to be used by the read() method
   */
  public Bits() {
  }

  /**
   * Copy constructor.
   *
   * @param other Bits object to be copied
   */
  public Bits(Bits other) {
    Preconditions.checkNotNull(other);

    this.length = other.length;
    bits = new int[other.bits.length];
    for(int i = 0; i < bits.length; i++) {
      bits[i] = other.bits[i];
    }
  }

  public Bits(Bits other, int length) {
    this(length);
    Preconditions.checkNotNull(other);

    for(int i = 0; i < bits.length; i++) {
      bits[i] = other.bits[i];
    }
  }

  /**
   * Constructs a Bits vector of a given length
   *
   * @param length Length of the vector.
   */
  public Bits(int length) {
    this(length, false);
  }

  /**
   * Constructs a Bits vector of a given length filled with
   * the given value.
   *
   * @param length Length of the vector
   * @param value Default value for bits
   */
  public Bits(int length, boolean value) {
    Preconditions.checkArgument(length > 0);

    this.length = length;
    int r = length >> UNIT_EXP;  //Divide
    int m = length & (UNIT_SIZE - 1); //Modulus
    if(m != 0) {
      bits = new int[r+1];
    } else {
      bits = new int[r];
    }

    for (int i = 0; i < bits.length; i++) {
      bits[i] = (value == false) ? 0 : 0xffffffff;
    }
  }

  /**
   * Gets the total number of bits (i.e., the length of the vector)
   *
   * @return Length of the vector
   */
  public int size() {
    return length;
  }

  /**
   * Sets the value of a bit.
   *
   * @param p Index of the bit
   * @param on Value of the bit
   */
  public void set(int p, boolean on) {
    Preconditions.checkArgument(p < length);

    if(on) {
      set(p);
    } else {
      clear(p);
    }
  }

  /**
   * Sets a bit to 1
   *
   * @param p Index of the bit
   */
  public void set(int p) {
    Preconditions.checkArgument(p < length);
    bits[p>>UNIT_EXP] |= 1<<(UNIT_SIZE_1 - (p & (UNIT_SIZE_1)));
  }

  /**
   * Clears a bit
   *
   * @param p Index of the bit
   */
  public void clear(int p) {
    Preconditions.checkArgument(p < length);
    bits[p>>UNIT_EXP] &= ~(1<<(UNIT_SIZE_1 - (p&(UNIT_SIZE_1))));
  }

  /**
   * Gets the value of a given bit index
   *
   * @param p Index of the bit
   * @return A boolean value indicating whether a bit is on or off
   */
  public boolean get(int p) {
    Preconditions.checkArgument(p < length);
    return ((bits[p>>UNIT_EXP]>>(UNIT_SIZE_1 - (p&(UNIT_SIZE_1)))) & 1) == 1;
  }

  public int getBits(int e, int b) {
    Preconditions.checkArgument(b < length && b - e < 32);

    if(e == b) {
      if(get(e)) {
        return 1;
      }
      return 0;
    }

    int p_e = e >> UNIT_EXP;
    int r_e = e & UNIT_SIZE_1;
    int p_b = b >> UNIT_EXP;
    int r_b = b & UNIT_SIZE_1;
    int s = UNIT_SIZE_1 - r_b;

    int v = bits[p_e];
    v &= (0xFFFFFFFF >>> r_e);

    if(p_e == p_b) {
      return (v & (0xFFFFFFFF << s)) >>> s;
    } else {
      v = v << (r_b + 1);
      return v | ((bits[p_b] & (0xFFFFFFFF << s)) >>> s);
    }
  }

  /**
   * Clears the vector
   */
  public void clear() {
    for(int i = 0; i < bits.length; i++) {
      bits[i] = 0;
    }
  }

  /**
   * Counts the number of set elements
   *
   * @return Number of elements that are 1
   */
  public int count() {
    int total = 0;
    for(int i = 0; i < bits.length; i++) {
      total += count(bits[i]);
    }
    return total;
  }

  /**
   * Counts the number of set elements in an integer (Popcount algorithm)
   *
   * @param value An integer value
   * @return Number of 1 elements in the given integer value
   */
  private static int count(int value) {
    value = (value & 0x55555555) + ((value & 0xaaaaaaaa) >>> 1);
    value = (value & 0x33333333) + ((value & 0xcccccccc) >>> 2);
    value = (value & 0x0f0f0f0f) + ((value & 0xf0f0f0f0) >>> 4);
    value = (value & 0x00ff00ff) + ((value & 0xff00ff00) >>> 8);
    return (value & 0x0000ffff) + ((value & 0xffff0000) >>> 16);
  }

  /**
   * Retrieves the indexes of the set elements in the vector
   *
   * @return Array of indexes
   */
  public int[] getOnes() {
    int[] documents = new int[count()];
    int pos = 0;
    for(int i = 0; i < size(); i++) {
      if(get(i)) {
        documents[pos++] = i;
      }
    }
    return documents;
  }

  /**
   * Bitwise AND operator
   *
   * @param other A second Bits vector to AND
   */
  public void and(Bits other) {
    Preconditions.checkNotNull(other);
    Preconditions.checkArgument(this.length == other.length);

    for(int i = 0; i < bits.length; i++) {
      bits[i] &= other.bits[i];
    }
  }

  /**
   * Bitwise OR operator
   *
   * @param other A second Bits vector to OR
   */
  public void or(Bits other) {
    Preconditions.checkNotNull(other);
    Preconditions.checkArgument(this.length == other.length);

    for(int i = 0; i < bits.length; i++) {
      bits[i] |= other.bits[i];
    }
  }

  /**
   * Bitwise XOR operator
   *
   * @param other A second Bits vector to XOR
   */
  public void xor(Bits other) {
    Preconditions.checkNotNull(other);
    Preconditions.checkArgument(this.length == other.length);

    for(int i = 0; i < bits.length; i++) {
      bits[i] ^= other.bits[i];
    }
  }

  //Writable interface
  @Override public void readFields(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);

    length = in.readInt();
    bits = new int[in.readInt()];
    for(int i = 0; i < bits.length; i++) {
      bits[i] = in.readInt();
    }
  }

  @Override public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(out);

    out.writeInt(length);
    out.writeInt(bits.length);

    for(int i = 0; i < bits.length; i++){
      out.writeInt(bits[i]);
    }
  }

  @Override public String toString() {
    StringBuilder b = new StringBuilder();
    for(int i = 0; i < length; i++) {
      if(get(i)) {
        b.append('1');
      } else {
        b.append('0');
      }
    }
    return b.toString();
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof Bits);

    Bits other = (Bits) o;
    if(this.length == other.length) {
      int index = 0;
      for(int i = 0; i < this.bits.length; i++) {
        index += (i + 1) * UNIT_SIZE;
        if(index > length) {
          for(int p = i * UNIT_SIZE; p < length; p++) {
            if(get(p) != other.get(p)) {
              return false;
            }
          }
        } else {
          if(this.bits[i] != other.bits[i]) {
            return false;
          }
        }
      }
    } else {
      return false;
    }
    return true;
  }
}
