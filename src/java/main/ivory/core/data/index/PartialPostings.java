package ivory.core.data.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.util.array.ArrayListOfInts;

public class PartialPostings implements Writable {

  ArrayListOfInts docnos = new ArrayListOfInts();
  ArrayList<int[]> positions = new ArrayList<int[]>();

  public void add(int docno, int[] tp) {
    docnos.add(docno);
    positions.add(tp);
  }

  public void add(PartialPostings termDocPosList) {
    termDocPosList.docnos.trimToSize();
    for (int i : termDocPosList.docnos.getArray()) {
      docnos.add(i);
    }
    for (int[] tp : termDocPosList.positions) {
      positions.add(tp);
    }
  }

  public int size() {
    return positions.size();
  }

  public int[] getDocnos() {
    return docnos.getArray();
  }

  public int[][] getPositions() {
    int[][] p = new int[positions.size()][];
    positions.toArray(p);
    return p;
  }

  public void readFields(DataInput in) throws IOException {
    docnos.clear();
    positions.clear();
    int n = in.readInt();

    for (int i = 0; i < n; i++)
      docnos.add(in.readInt());

    int[] p;
    short m;
    for (int i = 0; i < n; i++) {
      m = in.readShort();
      p = new int[m];
      for (int j = 0; j < m; j++)
        p[j] = WritableUtils.readVInt(in);
      positions.add(p);
    }
  }

  public void write(DataOutput out) throws IOException {
    docnos.trimToSize();
    out.writeInt(docnos.size());
    for (int i : docnos.getArray())
      out.writeInt(i);
    for (int[] tp : positions) {
      out.writeShort((short) tp.length);
      for (int i : tp)
        WritableUtils.writeVInt(out, i);
    }

  }

  public void clear() {
    docnos.clear();
    positions.clear();
  }

}
