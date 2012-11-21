package ivory.lsh.data;

import java.io.DataInput;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PairOfIntNBitSignature extends PairOfIntSignature {
  private static final Logger logger = Logger.getLogger(PairOfIntNBitSignature.class);
  {
    logger.setLevel(Level.WARN);
  }

  public PairOfIntNBitSignature() {
    super();
  }

  public PairOfIntNBitSignature(int i, Signature permutedSign) {
    permNo = i;
    signature = permutedSign;
  }

  @Override
  public void readFields(DataInput in)  {
    signature = new NBitSignature();

    try {
      permNo = in.readInt();
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new RuntimeException("Could not read permNo in PairOfIntSignature");
    }

    try {
      signature.readFields(in);
    } catch (IOException e) {
      logger.warn("NULL SIGNATURE!");
      signature = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Object other) {
    PairOfIntNBitSignature p = (PairOfIntNBitSignature) other;

    int c = signature.compareTo(p.signature);
    if(c==0){
      return (permNo<p.permNo) ? -1 : ((permNo>p.permNo) ? 1 :0);
    }else{
      return c;
    }
  }

  public boolean equals(Object other){
    PairOfIntNBitSignature p = (PairOfIntNBitSignature) other;

    return (p.getInt()==getInt() && ((NBitSignature)p.getSignature()).equals(this.getSignature()));
  }

  @Override
  public int hashCode(){
    int h = signature.hashCode()+permNo;
    if(h>0){
      return h ^ Integer.MAX_VALUE;
    }else{
      return h ^ Integer.MIN_VALUE;			
    }
  }
}
