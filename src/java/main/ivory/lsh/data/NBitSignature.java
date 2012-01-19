package ivory.lsh.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 *
 *   A space and time-efficient implementation of Signature.
 *  This class is basically a WritableComparable wrapper around the Bits class with many functionalities such as computing hamming distance very efficiently (using bitwise operations).
 *
 * @author ferhanture
 *
 */
public class NBitSignature extends Signature {
  Bits bits;

  public NBitSignature(){
    super();
  }

  /**
   * Create a BitsSignature object with the specified number of bits, all initially set to 0.
   *
   * @param size
   *     number of bits
   */
  public NBitSignature(int size){
    bits = new Bits(0, size);
  }

  public NBitSignature(Bits b){
    bits = new Bits(0, b.length);
    for(int i=0;i<size();i++){
      bits.setBit(i, b.getBit(i));
    }
  }

  public NBitSignature(NBitSignature other){
    this(other.bits);
  }

  public NBitSignature(byte[] bits, int len) {
    this.bits = new Bits(bits, len);
  }

  public NBitSignature append(Signature signature) {
    Bits b = ((NBitSignature)signature).bits.append(bits);
    NBitSignature s = new NBitSignature(b);
    return s;
  }

  /**
   * @param start
   *   first index to be included in sub-signature
   * @param end
   *   last index to be included in sub-signature
   * @return
   *   return a new BitsSignature object, containing the bits of this object from <code>start</code> to <code>end</code>
   */
  @Override
  public NBitSignature getSubSignature(int start, int end){
    NBitSignature sub = new NBitSignature(end-start+1);
    Bits subBits = bits.extract(end, start);
    sub.bits = subBits;

    //    for(int i=0;i<(end-start+1);i++){
    //      sub.set(i, subBits.getBit(i)==1);
    //    }
    return sub;
  }

  @Override
  public NBitSignature getSubSignature(int start, int end, Signature subSign){
    NBitSignature sub = (NBitSignature) subSign;
    Bits subBits = bits.extract(end, start);
    sub.bits = subBits;

    return sub;
  }

  public void readFields(DataInput in) throws IOException {
    int numBits = in.readInt();
    int numBytes = in.readInt();
    byte[] bytearray = new byte[numBytes];

    for(int i=0;i<numBytes;i++){
      bytearray[i]=in.readByte();
    }

    bits = new Bits(bytearray, numBits);
  }

  public void write(DataOutput out) throws IOException {
    int numBits = size(), numBytes = bits.bits.length;

    //    System.out.println("# bits: "+size());
    //    System.out.println("# bytes: "+bits.bits.length);

    out.writeInt(numBits);
    out.writeInt(numBytes);

    for(int i=0;i<numBytes;i++){
      out.writeByte(bits.bits[i]);
    }
  }

  public boolean get(int i) {
    return bits.getBit(i)==1;
  }

  public void set(int i, boolean sign) {
    if(sign){
      bits.setBit(i, 1);
    }else{
      bits.setBit(i, 0);
    }
  }

  @Override
  public int size() {
    return bits.length;
  }

  @Override
  public boolean equals(Object o){
    if(o==null){
      return false;
    }
    NBitSignature other = (NBitSignature) o;

    if(other.size()!=this.size()){
      return false;
    }

    for(int i=0;i<other.bits.length;i++){
      if((!other.get(i) && this.get(i))|| (other.get(i) && !this.get(i))){
        return false;
      }
    }
    return true;
  }

  public int compareTo(Object obj) {
    NBitSignature other = (NBitSignature) obj;
    return this.compareTo(other);
  }

  public int compareTo(NBitSignature other) {
    ///BitsSignature64 other = (BitsSignature64) obj;
    /*if(size()!=other.size()){
      throw new RuntimeException("Cannot compare different sized signatures!");
    }*/
    boolean myBit, otherBit;
    for(int i=0;i<size();i++){
      myBit = get(i);
      otherBit = other.get(i);
      if(!myBit && otherBit){
        return -1;
      }else if(myBit && !otherBit){
        return 1;
      }
    }
    return 0;
  }

  @Override
  public int hashCode(){
    int h = bits.toString().hashCode();
    if(h>0){
      return h ^ Integer.MAX_VALUE;
    }else{
      return h ^ Integer.MIN_VALUE;
    }
  }

  public int countSetBits(){
    int cnt = 0;
    for(int i=0;i<this.size();i++){
      if(this.get(i)){
        cnt++;
      }
    }
    return cnt;
  }

  public static int countSetBits2(int n){
    int count = 0;
    while (n!=0)  {
      count++ ;
      n &= (n - 1) ;
    }
    return count;
  }


  public String toString(){
    String s = "";
    for(int i=0;i<size();i++){
      s+=(get(i) ? "1" : "0");
    }
    return s;
  }

  public float cosine(NBitSignature s2){
    float dist = 0;
    for(int i=0;i<size();i++){
      if((this.get(i) && s2.get(i)) || (!this.get(i) && !s2.get(i))){

      }else{
        dist++;
      }
    }
    return (float) Math.cos(Math.PI*(dist/size()));
  }

  public Signature perm(ArrayListOfIntsWritable permutation) {
    NBitSignature newSign = new NBitSignature(size());
    int[] perms = permutation.getArray();
    for(int j=0;j<size();j++){
      newSign.set(j,bits.getBit(perms[j])==1);
    }
    return newSign;
  }

  public void perm(ArrayListOfIntsWritable permutation, Signature newSign) {
    NBitSignature newS = (NBitSignature) newSign;
    int[] perms = permutation.getArray();
    for(int j=0;j<size();j++){
      newS.set(j, bits.getBit(perms[j])==1);
    }
  }

  @Override
  public int hammingDistance(Signature s) {
    NBitSignature s2 = (NBitSignature) s;
    return this.hammingDistance(s2, bits.length);
  }

  @Override
  public int hammingDistance(Signature signature, int threshold) {
    byte[] bb1 = bits.bits;
    NBitSignature s2 = (NBitSignature) signature;
    byte[] bb2 = s2.bits.bits;

    int count=0;

    /*byte by byte*/
    for(int i=0;i<bb1.length;i++){
      byte n = (byte) (bb1[i] ^ bb2[i]);    //xor two bytes
      while (n!=0)  {          //count number of 1s in the xor
        count++;
        n &= (n - 1);
      }
      if(count>threshold){
        return count;
      }
    }
    return count;
  }

  public void and(NBitSignature other) {
    byte[] obb = other.bits.bits;

    for(int i = 0; i < obb.length; i++) {
      bits.bits[i] &= obb[i];
    }
  }

  public void or(NBitSignature other) {
    byte[] obb = other.bits.bits;

    for(int i = 0; i < obb.length; i++) {
      bits.bits[i] |= obb[i];
    }
  }

  @Override
  public int getLongestPrefix(Signature s) {
    NBitSignature signature = (NBitSignature) s;
    int cnt = 0;
    for(int i=0; i<size(); i++){
      if(signature.get(i) == this.get(i)){
        cnt++;
      }else{
        return cnt;
      }
    }
    return cnt;
  }

  //  public BitsSignature permute(int[] permutation){
  //    Bits permBits = new Bits(0,size());
  //    int b=0;
  //    while(b<size()-1){
  //      Bits bits = this.bits;
  //      int seq = bits.getBits(b, b+31);
  //      int permSeq = perm(seq, permutation, 32);
  //      permBits.setBits(b, b+31, permSeq);
  //      b+=32;
  //    }
  //    return new BitsSignature(permBits);
  //  }

  //  public static long perm(long b, final int subs[], int blockSize) {
  //    long sbytes = 0;
  //    for (int i = 0; i < subs.length; i++) {
  //      int shifts = subs[i];
  //      //using a mask of intialized with one
  //      long mask = 1;
  //
  //      mask = (mask << (blockSize - shifts));
  //      long bit = (mask & b);
  //      if (bit != 0) { //if the bit is one
  //        mask = 1;
  //        mask =  (mask << (blockSize - i - 1));
  //        sbytes = (sbytes | mask);
  //      }
  //    }
  //
  //    return sbytes;
  //  }

}
