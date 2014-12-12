package ivory.lsh.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.map.HMapStFW;

public class WikiDocInfo implements Writable {
	int langID;
	ArrayListWritable<HMapStFW> vectors;
	ArrayListWritable<Text> sentences;

	public WikiDocInfo() {
		super();
	}
	
	public WikiDocInfo(int i1, ArrayListWritable<Text> t, ArrayListWritable<HMapStFW> v){//, ArrayListOfIntsWritable l) {
		langID = i1;
		vectors = v;
		sentences = t;
	}
	
	public void readFields(DataInput in)  {
		vectors = new ArrayListWritable<HMapStFW>();
		sentences = new ArrayListWritable<Text>();

		try {
			langID = in.readInt();
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new RuntimeException("Could not read integer in WikiDocInfo");
		}
		
		try {
			vectors.readFields(in);
			sentences.readFields(in);
		} catch (IOException e) {
			throw new RuntimeException("Could not read vectors/sentlengths in WikiDocInfo");
		}
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(langID);
		vectors.write(out);
		sentences.write(out);
	}
	
	public boolean equals(Object other){
		WikiDocInfo p = (WikiDocInfo) other;

		return (p.getLangID()==getLangID() && (p.getVectors()).equals(this.getVectors())  && (p.getSentences()).equals(this.getSentences()));
	}
	
	public ArrayListWritable<HMapStFW> getVectors() {
		return vectors;
	}

	public ArrayListWritable<Text> getSentences() {
		return sentences;
	}
	
	public int getLangID() {
		return langID;
	}

	public void set(int n1, ArrayListWritable<HMapStFW> vectors, ArrayListWritable<Text> sentences) {
		this.langID = n1;		
		this.vectors = vectors;
		this.sentences = sentences;
	}


}
