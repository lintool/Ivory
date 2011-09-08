package ivory.core.data.dictionary;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public class FrontCodedDictionary implements Writable, LexicographicallySortedDictionary {

  private FrontCodedStringList stringList;
  private ShiftAddXorSignedStringMap dict;

  public FrontCodedDictionary() {
  }

  @Override
  public int getId(String term) {
    return (int) dict.getLong(term);
  }

  @Override
  public String getTerm(int id) {
    return stringList.get(id).toString();
  }

  @Override
  public int size() {
    return stringList.size();
  }

  @Override
  public Iterator<String> iterator() {
    return null;
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    final int size = in.readInt();

    stringList = new FrontCodedStringList(new Iterator<String>() {
      int cnt = 0;

      @Override
      public boolean hasNext() {
        return cnt < size;
      }

      @Override
      public String next() {
        String s = null;
        try {
          s = in.readUTF();
        } catch (IOException e) {
          e.printStackTrace();
        }
        cnt++;
        return s;
      }

      @Override
      public void remove() {
        // TODO Auto-generated method stub
      }

    }, 8, true);

    dict = new ShiftAddXorSignedStringMap(stringList.iterator(),
        new TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence>(stringList,
            TransformationStrategies.prefixFreeIso()));

  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  /**
   * Simple demo program for looking up terms and term ids.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [index-path]");
      System.exit(-1);
    }

    String indexPath = args[0];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    FrontCodedDictionary dictionary = new FrontCodedDictionary();
    dictionary.readFields(fs.open(new Path(indexPath)));

    int nTerms = dictionary.size();
    System.out.println("nTerms: " + nTerms);

    System.out.println(" \"term word\" to lookup termid; \"termid 234\" to lookup term");
    String cmd = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("lookup > ");
    while ((cmd = stdin.readLine()) != null) {

      String[] tokens = cmd.split("\\s+");

      if (tokens.length != 2) {
        System.out.println("Error: unrecognized command!");
        System.out.print("lookup > ");

        continue;
      }

      if (tokens[0].equals("termid")) {
        int termid;
        try {
          termid = Integer.parseInt(tokens[1]);
        } catch (Exception e) {
          System.out.println("Error: invalid termid!");
          System.out.print("lookup > ");

          continue;
        }

        System.out.println("termid=" + termid + ", term=" + dictionary.getTerm(termid));
      } else if (tokens[0].equals("term")) {
        String term = tokens[1];

        System.out.println("term=" + term + ", termid=" + dictionary.getId(term));
      } else {
        System.out.println("Error: unrecognized command!");
        System.out.print("lookup > ");
        continue;
      }

      System.out.print("lookup > ");
    }
  }
}
