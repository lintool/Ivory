package ivory.bloomir.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

/**
 * A Bloom filter with an identity hash function (i.e., a bitset)
 *
 * @author Nima Asadi
 */
public class BloomFilterIdentityHash extends Signature {
  private Bits bits;

  private BloomFilterIdentityHash() {
  }

  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   */
  public BloomFilterIdentityHash(int vectorSize) {
    Preconditions.checkArgument(vectorSize > 0);
    bits = new Bits(vectorSize + 1);
  }

  @Override public void add(int key) {
    bits.set(key);
  }

  @Override public boolean membershipTest(int key) {
    return bits.get(key);
  }

  /**
   * Reads and returns an instance of this class.
   *
   * @param in DataInput stream
   * @return An instance of this class.
   */
  public static BloomFilterIdentityHash readInstance(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);
    BloomFilterIdentityHash signature = new BloomFilterIdentityHash();
    signature.readFields(in);
    return signature;
  }

  @Override public void readFields(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);
    bits = new Bits();
    bits.readFields(in);
  }

  @Override public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(out);
    bits.write(out);
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    if(!(o instanceof BloomFilterIdentityHash)) {
      return false;
    }

    BloomFilterIdentityHash other = (BloomFilterIdentityHash) o;
    return this.bits.equals(other.bits);
  }
}
