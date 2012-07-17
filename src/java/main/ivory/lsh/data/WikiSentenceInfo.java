package ivory.lsh.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.map.HMapSFW;

public class WikiSentenceInfo implements Writable {
  int langID;
  Text sentence;
  HMapSFW vector;

  public WikiSentenceInfo() {
    super();
  }

  public WikiSentenceInfo(int i1, Text t, HMapSFW v){
    langID = i1;
    sentence = t;
    vector = v;
  }

  public void readFields(DataInput in)  {
    try {
      langID = in.readInt();
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new RuntimeException("Could not read integer in WikiSentenceInfo");
    }

    try {
      sentence = new Text();
      sentence.readFields(in);
      vector = new HMapSFW();
      vector.readFields(in);
    } catch (IOException e) {
      throw new RuntimeException("Could not read vectors/sentences in WikiSentenceInfo");
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(langID);
    sentence.write(out);
    vector.write(out);
  }

  public boolean equals(Object other){
    WikiSentenceInfo p = (WikiSentenceInfo) other;

    return (p.getLangID()==getLangID() && (p.getSentence()).equals(this.getSentence()) && (p.getVector()).equals(this.getVector()));
  }

  public Text getSentence() {
    return sentence;
  }

  public HMapSFW getVector() {
    return vector;
  }

  public int getLangID() {
    return langID;
  }

  public void set(int n1, Text sentence, HMapSFW vector) {
    this.langID = n1;		
    this.sentence = sentence;
    this.vector = vector;
  }


}
