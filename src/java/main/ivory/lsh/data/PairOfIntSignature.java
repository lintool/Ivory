package ivory.lsh.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("unchecked")
public abstract class PairOfIntSignature implements WritableComparable {

  public int permNo;
  Signature signature; 

  public PairOfIntSignature() {
    super();
    //		signature = new BitsSignature();
  }

  public PairOfIntSignature(int i, Signature permutedSign) {
    permNo = i;
    signature = permutedSign;
  }

  public abstract void readFields(DataInput in);

  public void write(DataOutput out) throws IOException {
    out.writeInt(permNo);
    if(signature!=null){
      signature.write(out);
    }
  }

  //	public int hashCode(){
  //		return permNo;
  //	}

  public abstract int compareTo(Object other);

  public boolean equals(Object other){
    PairOfIntSignature p = (PairOfIntSignature) other;

    return (p.getInt()==getInt() && p.getSignature().equals(this.getSignature()));
  }

  public Signature getSignature() {
    return signature;
  }

  public int getInt() {
    return permNo;
  }

  public void setInt(int n){
    permNo = n;
  }

  public void setSignature(Signature s){
    signature = s;
  }

  public String toString(){
    return "(" + permNo + "," + signature + ")";
  }

}
