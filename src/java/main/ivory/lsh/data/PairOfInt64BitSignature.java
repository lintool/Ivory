package ivory.lsh.data;

import java.io.DataInput;
import java.io.IOException;

public class PairOfInt64BitSignature extends PairOfIntSignature {

	public PairOfInt64BitSignature() {
		super();
	}
	
	public PairOfInt64BitSignature(int i, Signature permutedSign) {
		permNo = i;
		signature = permutedSign;	
	}
	
	@Override
	public void readFields(DataInput in)  {
		signature = new SixtyFourBitSignature();

		try {
			permNo = in.readInt();
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new RuntimeException("Could not read permNo in PairOfIntSignature");
		}
		
		try {
			signature.readFields(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(Object other) {
		PairOfInt64BitSignature p = (PairOfInt64BitSignature) other;

		int c = signature.compareTo(p.signature);
		if(c==0){
			return -1;
		}else{
			return c;
		}
	}
}
