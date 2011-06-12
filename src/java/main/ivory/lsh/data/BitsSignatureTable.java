/**
 * 
 */
package ivory.lsh.data;

/**
 * @author Tamer
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.Writable;


public class BitsSignatureTable implements Writable{
	Signature[] signaturesArray = null;
	int[] docNosArray = null;

	int nSignatures = 0;

	public BitsSignatureTable (){
	}

	public void set(Signature[] signs, int[] nos, int nElements){
		if(signs.length == 0) throw new RuntimeException("invalid size of signatures: "+signs.length);
		if(nos.length == 0) throw new RuntimeException("invalid size of docnos: "+nos.length);
		if(signs.length != nos.length) 
			throw new RuntimeException("size mismatch: "+signs.length+"\t"+nos.length);
		if(nElements <= 0 || nElements > signs.length) throw new RuntimeException("invalid size: "+nElements);
		signaturesArray = signs;
		docNosArray = nos;	
		nSignatures = nElements;
	}

	public void add(SixtyFourBitSignature sign, int docno){
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		
		String className = in.readUTF();
		try {
			Class c = Class.forName(className);
			nSignatures = in.readInt();
			if(signaturesArray== null || signaturesArray.length < nSignatures)
				signaturesArray = (Signature[]) Array.newInstance(c, nSignatures);
			for(int  i = 0; i < nSignatures; i++){
				signaturesArray[i] = (Signature) c.newInstance();
				signaturesArray[i].readFields(in);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Signature class not found");
		}
		
		if(docNosArray== null || docNosArray.length < nSignatures)
			docNosArray = new int[nSignatures];
		for(int  i = 0; i < nSignatures; i++){
			docNosArray[i] = in.readInt();
		}
	}
	
	/*public void readFields(DataInput in) throws IOException {
		
		nSignatures = in.readInt();
		if(signaturesArray== null || signaturesArray.length < nSignatures)
			signaturesArray = new SixtyFourBitSignature[nSignatures];
		for(int  i = 0; i < nSignatures; i++){
			signaturesArray[i] = new SixtyFourBitSignature();
			signaturesArray[i].readFields(in);
		}
		if(docNosArray== null || docNosArray.length < nSignatures)
			docNosArray = new int[nSignatures];
		for(int  i = 0; i < nSignatures; i++){
			docNosArray[i] = in.readInt();
		}
	}*/

	public void clear(){
		nSignatures = 0;
	}

	public void free(){
		nSignatures = 0;
		signaturesArray = null;
		docNosArray = null;
	}

	public void write(DataOutput out) throws IOException {
		if(signaturesArray == null)
			throw new RuntimeException("invalid NULL signaturesArray!");
		if(docNosArray == null)
			throw new RuntimeException("invalid NULL docNosArray!");
		
		out.writeUTF(signaturesArray[0].getClass().getCanonicalName());
		
		out.writeInt(nSignatures);
		for(int i = 0; i < nSignatures; i++) signaturesArray[i].write(out);
		for(int i = 0; i < nSignatures; i++) out.writeInt(docNosArray[i]);
	}

	public Signature[] getSignatures(){
		return signaturesArray;
	}

	public int[] getDocNos(){
		return docNosArray;
	}

	public int getNumOfSignatures(){
		return nSignatures;
	}
	
	public String toString(){
		String s ="";
		for(int i=0;i<nSignatures;i++){
			s+="("+docNosArray[i]+","+signaturesArray[i]+");";
		}
		return s;
	}
}
