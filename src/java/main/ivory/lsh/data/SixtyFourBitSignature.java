package ivory.lsh.data;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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
public class SixtyFourBitSignature extends Signature{
	private byte[] bits;
	static int NUM_BITS = 64;
	static int NUM_BYTES = 8;

	/**
	 * Create a BitsSignature object with the specified number of bits, all initially set to 0.
	 * 
	 * @param size
	 * 		number of bits
	 */
	public SixtyFourBitSignature(){
		super();
		setBits(new byte[NUM_BYTES]);
	}

	public SixtyFourBitSignature(byte[] b){
		setBits(new byte[NUM_BYTES]);
		for(int i=0;i<b.length;i++){
			getBits()[i]= b[i];
		}
		//		bits = b;
	}

	public SixtyFourBitSignature(SixtyFourBitSignature other){
		this(other.getBits());
	}
	
	public SixtyFourBitSignature(int numBits){			//need this constructor for general purposes.
		super();
		if(numBits!=NUM_BITS){
			throw new RuntimeException("Wrong number of bits!");
		}
		setBits(new byte[NUM_BYTES]);
	}


	public void setBits(byte[] bits) {
		this.bits = bits;
	}

	/**
	 * @param start
	 * 	first index to be included in sub-signature
	 * @param end
	 * 	last index to be included in sub-signature
	 * @return
	 * 	return a new BitsSignature object, containing the bits of this object from <code>start</code> to <code>end</code>
	 */
	public NBitSignature getSubSignature(int b, int e){
		NBitSignature sub = new NBitSignature();
		int l = 1 + ((b < e) ? e-b : b-e);
		int r = l/8, m = l%8;
		byte[] q = new byte[(m==0) ? r : r+1];

		if (b <= e) // not reversed
			for (int i=0; i<l; ++i) {
				int p = b+i;
				if ((getBits()[p/8] & (1<<(p%8))) != 0) q[i/8] |= 1<<(i%8);
			}
		else // reversed
			for (int i=0; i<l; ++i) {
				int p = b-i;
				if ((getBits()[p/8] & (1<<(p%8))) != 0) q[i/8] |= 1<<(i%8);
			}
		sub.bits = new Bits(q,e-b+1);
		return sub;
	}

	public NBitSignature getSubSignature(int b, int e, Signature subSign){
		NBitSignature sub = (NBitSignature) subSign;
		int l = 1 + ((b < e) ? e-b : b-e);
		int r = l/8, m = l%8;
		byte[] q = new byte[(m==0) ? r : r+1];

		if (b <= e) // not reversed
			for (int i=0; i<l; ++i) {
				int p = b+i;
				if ((getBits()[p/8] & (1<<(p%8))) != 0) q[i/8] |= 1<<(i%8);
			}
		else // reversed
			for (int i=0; i<l; ++i) {
				int p = b-i;
				if ((getBits()[p/8] & (1<<(p%8))) != 0) q[i/8] |= 1<<(i%8);
			}
		sub.bits = new Bits(q,e-b+1);
		return sub;
	}

	public void readFields(DataInput in) throws IOException {
//		bits = new byte[NUM_BYTES];

		for(int i=0;i<NUM_BYTES;i++){
			getBits()[i]=in.readByte();
		}
	}

	public void write(DataOutput out) throws IOException {
		for(int i=0;i<NUM_BYTES;i++){
			out.writeByte(getBits()[i]);
		}
	}

	public boolean get(int i) {
		return (((getBits()[i/8]>>(7-i%8)) & 1)==1);
	}

	public void set(int i, boolean sign) {
		if (!sign) getBits()[i/8] &= ~(1<<(7-i%8)); // clear
		else getBits()[i/8] |= 1<<(7-i%8); // set
	}

	public int size() {
		return NUM_BITS;
	}

	@Override
	public boolean equals(Object o){
		if(o==null){
			return false;
		}
		SixtyFourBitSignature other = (SixtyFourBitSignature) o;

		if(other.size()!=this.size()){
			return false;
		}

		for(int i=0;i<other.getBits().length;i++){
			if((!other.get(i) && this.get(i))|| (other.get(i) && !this.get(i))){
				return false;
			}
		}
		return true;
	}

	public int compareTo(Object obj) {
		SixtyFourBitSignature other = (SixtyFourBitSignature) obj;
		return this.compareTo(other);	
	}
	
	public int compareTo(SixtyFourBitSignature other) {
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
		int h = getBits().toString().hashCode();
		if(h>0){
			return h ^ Integer.MAX_VALUE;
		}else{
			return h ^ Integer.MIN_VALUE;			
		}
	}

	@Override
	public int hammingDistance(Signature signature, int threshold){
		//		float thr = (float) ((Math.acos(PWSimProbMethod.COSINE_TRESHOLD)/Math.PI)*this.size());
		SixtyFourBitSignature s2 = (SixtyFourBitSignature) signature;
		byte[] bb1 = this.getBits();
		byte[] bb2 = s2.getBits();

		int count=0;

		/*byte by byte*/
		for(int i=0;i<bb1.length;i++){

			byte i1 = bb1[i], i2=bb2[i];
			byte n = (byte) (i1 ^ i2);
			while (n!=0)  {
				count++;
				n &= (n - 1) ;
				if(count>threshold){
					return count;
				}
			}
		}
		return count;
	}
	
	@Override
	public int hammingDistance(Signature s){
		return this.hammingDistance(s, NUM_BITS);
	}

	public String toString(){
		String s = "";
		for(int i=0;i<size();i++){
			s+=(get(i) ? "1" : "0");
		}
		return s;
	}

	@Override
	public Signature perm(ArrayListOfIntsWritable p) {
		int[] subs = p.getArray();
		long b = 0;	//byte array represented as a long
		ByteArrayInputStream bis = new ByteArrayInputStream(getBits());
		DataInputStream in = new DataInputStream(bis);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);

		try {
			b = in.readLong();
			long sbytes = 0;
			for (int i = 0; i < NUM_BITS; i++) {
				int shifts = subs[i];
				//using a mask of intialized with one 
				long mask = 1; 
				mask = (mask << (NUM_BITS - shifts - 1));
				long bit = (mask & b);
				if (bit != 0) { //if the bit is one
					mask = 1;
					mask =  (mask << (NUM_BITS - i - 1));
					sbytes = (sbytes | mask);
				}
			}
			dos.writeLong(sbytes);
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] permutedBytes =  bos.toByteArray();

		return new SixtyFourBitSignature(permutedBytes);
	}
	
//	@Override
//	public void perm(ArrayListOfIntsWritable p, BitsSignature permSign, DataInputStream in, ByteArrayOutputStream bos, DataOutputStream dos) throws IOException {
//		int[] subs = p.getArray();
//		long b = 0;	//byte array represented as a long
//		in.reset();
//		in.read(bits);
////		ByteArrayOutputStream bos = new ByteArrayOutputStream();
////		DataOutputStream dos = new DataOutputStream(bos);
//
//		try {
//			b = in.readLong();
//			long sbytes = 0;
//			for (int i = 0; i < NUM_BITS; i++) {
//				int shifts = subs[i];
//				//using a mask of intialized with one 
//				long mask = 1; 
//				mask = (mask << (NUM_BITS - shifts - 1));
//				long bit = (mask & b);
//				if (bit != 0) { //if the bit is one
//					mask = 1;
//					mask =  (mask << (NUM_BITS - i - 1));
//					sbytes = (sbytes | mask);
//				}
//			}
//			dos.writeLong(sbytes);
//			dos.flush();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		byte[] permutedBytes =  bos.toByteArray();
//		bos.reset();
//		
//		((SixtyFourBitSignature) permSign).setBits(permutedBytes);
//	}

	@Override
	public void perm(ArrayListOfIntsWritable p, Signature permSign) {
		int[] subs = p.getArray();
		long b = 0;	//byte array represented as a long
		ByteArrayInputStream bis = new ByteArrayInputStream(getBits());
		DataInputStream in = new DataInputStream(bis);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);

		try {
			b = in.readLong();
			long sbytes = 0;
			for (int i = 0; i < NUM_BITS; i++) {
				int shifts = subs[i];
				//using a mask of intialized with one 
				long mask = 1; 
				mask = (mask << (NUM_BITS - shifts - 1));
				long bit = (mask & b);
				if (bit != 0) { //if the bit is one
					mask = 1;
					mask =  (mask << (NUM_BITS - i - 1));
					sbytes = (sbytes | mask);
				}
			}
			dos.writeLong(sbytes);
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		byte[] permutedBytes =  bos.toByteArray();

		((SixtyFourBitSignature)permSign).setBits(permutedBytes);		
	}

	public byte[] getBits() {
		return bits;
	}
}
