package ivory.lsh.data;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;


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

  public abstract int hammingDistance(Signature signature);

  public abstract int hammingDistance(Signature signature, int threshold);

  public abstract void perm(ArrayListOfIntsWritable arrayListOfIntsWritable, Signature permSign);
  public abstract Signature perm(ArrayListOfIntsWritable arrayListOfIntsWritable);

  @SuppressWarnings("unchecked")
  public static Signature createSubSignature(Class subClass, int size){
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

