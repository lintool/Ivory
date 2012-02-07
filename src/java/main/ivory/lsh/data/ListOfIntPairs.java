package ivory.lsh.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.array.ArrayListOfInts;

public class ListOfIntPairs implements Writable {

  List<PairOfInts> lst; 

  public ListOfIntPairs() {
    super();
    lst = new ArrayList<PairOfInts>();
  }

  public void readFields(DataInput in) throws IOException{
    int size = in.readInt();
    for(int i=0; i<size; i++){
      PairOfInts elt = new PairOfInts();
      elt.readFields(in);
      lst.add(elt);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(lst.size());
    for(PairOfInts pair : lst){
      pair.write(out);
    }
  }

  public boolean equals(Object other){
    ListOfIntPairs p = (ListOfIntPairs) other;

    return lst.equals(p);
  }

  public void addPair(int i1, int i2){
    lst.add(new PairOfInts(i1, i2));
  }

  public String toString(){
    return lst.toString();
  }

  public void clear() {
    lst.clear();
  }

  public boolean isEmpty() {
    return lst.isEmpty();
  }

  public void setEIds(ArrayListOfInts l) {
    for(PairOfInts p : lst){
      l.add(p.getLeftElement());
    }
  }

  public void setFIds(ArrayListOfInts l) {
    for(PairOfInts p : lst){
      l.add(p.getRightElement());
    }
  }

}
