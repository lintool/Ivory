package ivory.lsh.data;
import org.apache.hadoop.io.WritableComparable;

import tl.lin.data.array.ArrayListOfIntsWritable;


/**
 *  Abstract class for signatures.
 * @author ferhanture
 *
 */
@SuppressWarnings("unchecked")
public abstract class Signature implements WritableComparable {

  public Signature() {
    super();
  }

  public Signature(int size) {
  }

  public Signature(Signature p) {
  }

  public abstract int size();

  public abstract String toString();

  public abstract Signature getSubSignature(int start, int end);
  public abstract Signature getSubSignature(int start, int end, Signature subSign);

  /**
   * @param signature
   * @return compute hamming distance between this object and signature.
   */
  public abstract int hammingDistance(Signature signature);

  /**
   * @param signature
   * @param threshold
   * @return compute hamming distance between this object and signature. early terminate if distance exceeds threshold.
   */
  public abstract int hammingDistance(Signature signature, int threshold);

  /**
   * @param permutation
   * permute this object w.r.t permutation object and save permuted signature in permSign
   */
  public abstract void perm(ArrayListOfIntsWritable permutation, Signature permSign);

  /**
   * @param permutation
   * @return permute this object w.r.t permutation object and return permuted signature
   */
  public abstract Signature perm(ArrayListOfIntsWritable permutation);

  @SuppressWarnings("unchecked")
  public static Signature createSignature(Class subClass, int size){
    if(subClass.equals(NBitSignature.class) || subClass.equals(SixtyFourBitSignature.class)){
      return new NBitSignature(size);
    }else if(subClass.equals(MinhashSignature.class)){
      return new MinhashSignature(size);
    }else{
      throw new RuntimeException("Unidentified class");
    }
  }

  public int getLongestPrefix(Signature signature) {
    return 0;
  }

}

