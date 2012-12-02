package ivory.bloomir.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

/**
 * An implementation of a Bloom filter with simple hash functions.
 *
 * @author Nima Asadi
 */
public class BloomFilterHash extends Signature {
  private static final int SEED = 0x7ed55d16;
  private Bits bits;
  private int vectorSize;
  private int nbHash;

  private BloomFilterHash() {
  }

  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash Number of hash functions.
   */
  public BloomFilterHash(int vectorSize, int nbHash) {
    Preconditions.checkArgument(vectorSize > 0);
    Preconditions.checkArgument(nbHash > 0);

    this.vectorSize = vectorSize;
    this.nbHash = nbHash;
    bits = new Bits(vectorSize);
  }

  @Override public void add(int key) {
    int[] h = hash(key, SEED, nbHash, vectorSize);
    for (int i = 0; i < h.length; i++) {
      bits.set(h[i]);
    }
  }

  @Override public boolean membershipTest(int key) {
    int seed = SEED;
    for(int i = 0; i < nbHash; i++) {
      int h = Math.abs(hash(key, seed) % vectorSize);
      if(!bits.get(h)) {
        return false;
      }
      seed = h;
    }
    return true;
  }

  /**
   * Computes a number of hash values for a given input.
   *
   * @param key Key to be hashed.
   * @param seed Seed to the hash algorithm
   * @param nbHash Number of hash values to compute
   * @param v Size of the underlying bitset
   * @return An array of hash values
   */
  private static int[] hash(int key, int seed, int nbHash, int v) {
    int[] h = new int[nbHash];
    int s = seed;
    for(int i = 0; i < h.length; i++) {
      h[i] = Math.abs(hash(key, s) % v);
      s = h[i];
    }
    return h;
  }

  /**
   * Jenkin's integer hash function
   *
   * @param key Key to be hashed
   * @param seed Seed to the hash algorithm
   * @return The hash value of the given input
   */
  private static int hash(int a, int seed) {
    a = (a+seed) + (a<<12);
    a = (a^0xc761c23c) ^ (a>>>19);
    a = (a+0x165667b1) + (a<<5);
    a = (a+0xd3a2646c) ^ (a<<9);
    a = (a+0xfd7046c5) + (a<<3);
    return (a^0xb55a4f09) ^ (a>>>16);
  }

  /**
   * Reads and returns an instance of this class from the given input
   *
   * @param in DataInput stream
   * @return A BloomFilterHash object
   */
  public static BloomFilterHash readInstance(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);

    BloomFilterHash signature = new BloomFilterHash();
    signature.readFields(in);
    return signature;
  }

  @Override public void readFields(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);

    vectorSize = in.readInt();
    nbHash = in.readInt();

    bits = new Bits();
    bits.readFields(in);
  }

  @Override public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(out);

    out.writeInt(vectorSize);
    out.writeInt(nbHash);
    bits.write(out);
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    if(!(o instanceof BloomFilterHash)) {
      return false;
    }

    BloomFilterHash other = (BloomFilterHash) o;
    if(this.vectorSize != other.vectorSize) {
      return false;
    }
    if(this.nbHash != other.nbHash) {
      return false;
    }
    return this.bits.equals(other.bits);
  }
}
