package ivory.lsh.data;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * 
 * 	A version of BitsSignature specifically tuned for 64 bits. Uses an array of 8 bytes instead of the Bits object.
 * 
 * @see ivory.lsh.data.NBitSignature
 * 
 * @author ferhanture
 *
 */
public class MinhashSignature extends Signature{//implements WritableComparable<BitsSignature64> {
  private ArrayListOfIntsWritable terms;

  /**
   * Create a BitsSignature object with the specified number of bits, all initially set to 0.
   */
  public MinhashSignature(){
    super();
    terms = new ArrayListOfIntsWritable();
  }

  public MinhashSignature(ArrayListOfIntsWritable b){
    terms = new ArrayListOfIntsWritable(b);
  }

  public MinhashSignature(MinhashSignature other){
    this(other.terms);
  }

  public MinhashSignature(int numTerms){			//need this constructor for general purposes.
    super();
    terms = new ArrayListOfIntsWritable(numTerms);
  }


  public void setTerms(ArrayListOfIntsWritable terms) {
    this.terms = terms;
  }

  /**
   * @param b
   * 	first index to be included in sub-signature
   * @param e
   * 	last index to be included in sub-signature
   * @return
   * 	return a new BitsSignature object, containing the bits of this object from <code>start</code> to <code>end</code>
   */
  public MinhashSignature getSubSignature(int b, int e){
    MinhashSignature sub = new MinhashSignature();
    sub.setTerms(sub(terms, b, e));
    return sub;
  }

  public MinhashSignature getSubSignature(int b, int e, Signature subSign){
    MinhashSignature sub = (MinhashSignature) subSign;
    sub.setTerms(sub(terms, b, e));
    return sub;
  }

  public void readFields(DataInput in) throws IOException {
    //		bits = new byte[NUM_BYTES];
    terms.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    terms.write(out);
  }

  public int get(int i) {
    return terms.get(i);
  }

  public void add(int elt) {
    terms.add(elt);
  }

  public void set(int pos, int elt) {
    terms.set(pos, elt);
  }

  /**
   * @return number of minhash terms.
   * 
   * @see ivory.lsh.data.Signature#size()
   */
  public int size() {
    return terms.size();
  }

  @Override
  public boolean equals(Object o){
    if(o==null){
      return false;
    }
    MinhashSignature other = (MinhashSignature) o;
    int myBit, otherBit;
    for(int i=0;i<size();i++){
      myBit = get(i);
      otherBit = other.get(i);
      if(myBit != otherBit){
        return false;
      }
    }
    return true;
  }

  /*public int compareTo(Object obj) {
		BitsSignature64 other = (BitsSignature64) obj;
		if(size()!=other.size()){
			throw new RuntimeException("Cannot compare different sized signatures!");
		}
		for(int i=0;i<size();i++){
			if(!get(i) && other.get(i)){
				return -1;
			}else if(get(i) && !other.get(i)){
				return 1;
			}
		}
		return 0;	
	}*/

  public int compareTo(MinhashSignature other) {
    ///BitsSignature64 other = (BitsSignature64) obj;
    /*if(size()!=other.size()){
			throw new RuntimeException("Cannot compare different sized signatures!");
		}*/
    int myBit, otherBit;
    for(int i=0;i<size();i++){
      myBit = get(i);
      otherBit = other.get(i);
      if(myBit < otherBit){
        return -1;
      }else if(myBit > otherBit){
        return 1;
      }
    }
    return 0;
  }

  @Override
  public int hashCode(){
    int h = terms.toString().hashCode();
    if(h>0){
      return h ^ Integer.MAX_VALUE;
    }else{
      return h ^ Integer.MIN_VALUE;			
    }
  }


  @Override
  public int hammingDistance(Signature signature, int threshold){
    MinhashSignature s2 = (MinhashSignature) signature;
    ArrayListOfIntsWritable l1 = this.terms;
    ArrayListOfIntsWritable l2 = s2.terms;

    int count=0;
    for(int i=0;i<l1.size();i++){

      int i1 = l1.get(i), i2=l2.get(i);
      if(i1!=i2){
        count++;
        if(count>threshold){
          return count;
        }
      }
    }
    return count;
  }

  @Override
  public int hammingDistance(Signature signature) {
    return this.hammingDistance(signature, this.size());
  }

  public String toString(){	
    String s="";
    if(terms.size()==0){
      return s;
    }
    for(int i=0;i<terms.size()-1;i++){
      s+=get(i)+",";
    }
    s+=get(terms.size()-1);
    return s;
  }

  @Override
  public MinhashSignature perm(ArrayListOfIntsWritable permutation) {
    MinhashSignature permuted = new MinhashSignature(this.size());
    int[] perms = permutation.getArray();
    int[] t = terms.getArray();
    for(int i=0;i<size();i++){
      //permuted.add(get(perms[i]));
      permuted.add(t[perms[i]]);
    }

    return permuted;
  }


  public void perm(ArrayListOfIntsWritable permutation, Signature permSign) {
    MinhashSignature permuted = (MinhashSignature) permSign;
    int[] perms = permutation.getArray();
    int[] t = terms.getArray();
    permuted.clear();
    for(int i=0;i<size();i++){
      permuted.add(t[perms[i]]);
    }
  }

  public void clear() {
    terms.clear();
  }

  public int compareTo(Object o) {
    MinhashSignature other = (MinhashSignature) o;

    return this.compareTo(other);
  }

  public boolean containsTerm(int i) {
    return terms.contains(i);
  }

  /**
   * @param start
   * 	first index to be included in sub-list
   * @param end
   * 	last index to be included in sub-list
   * @return
   * 	return a new ArrayListOfIntsWritable object, containing the ints of this object from <code>start</code> to <code>end</code>
   */
  public ArrayListOfIntsWritable sub(ArrayListOfIntsWritable lst, int start, int end) {
    ArrayListOfIntsWritable sublst = new ArrayListOfIntsWritable(end-start+1);
    for(int i=start;i<=end;i++){
      sublst.add(lst.get(i));
    }
    return sublst;
  }

}
