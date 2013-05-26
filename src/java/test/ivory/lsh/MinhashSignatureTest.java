package ivory.lsh;

import static org.junit.Assert.assertTrue;
import ivory.lsh.data.MinhashSignature;
import ivory.lsh.data.PermutationByBit;
import java.io.IOException;
import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;
import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class MinhashSignatureTest {
  private static final String TMP_FILENAME1 = "tmp1.out";

  private static int D = 20, vocabSize = 1000;

  private MinhashSignature getRandomSignature() {
    MinhashSignature s = new MinhashSignature(D);
    for (int i = 0; i < D; i++) {
    	int elt = (int) (Math.random() * vocabSize);
    	s.add(elt);
    }
    return s;
  }

  @Test
  public void testPermute() throws IOException {
    PermutationByBit p = new PermutationByBit(D);

    MinhashSignature s = getRandomSignature();
    System.out.println(s);

    int loopcnt = 0;
    MinhashSignature permutedS = new MinhashSignature(D);
    while (loopcnt++ < 10) {
      ArrayListOfIntsWritable a = p.nextPermutation();
      s.perm(a, permutedS);
      for (int i = 0; i < s.size(); i++) {
        assertTrue(permutedS.containsTerm(s.get(i)));
      }
      assertTrue(permutedS.size() == s.size());
      System.out.println(permutedS);
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    MinhashSignature s = getRandomSignature();
    MinhashSignature s2 = getRandomSignature();

    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();

    try {
      fs = FileSystem.get(conf);
      w = SequenceFile.createWriter(fs, conf, new Path(TMP_FILENAME1),
          IntWritable.class, MinhashSignature.class);
      w.append(new IntWritable(1), s);
      w.append(new IntWritable(2), s2);
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    List<PairOfWritables<WritableComparable<IntWritable>, Writable>> listOfKeysPairs =
        SequenceFileUtils.readFile(new Path(TMP_FILENAME1));
    FileSystem.get(conf).delete(new Path(TMP_FILENAME1), true);

    MinhashSignature read1 = (MinhashSignature) listOfKeysPairs.get(0).getRightElement();
    MinhashSignature read2 = (MinhashSignature) listOfKeysPairs.get(1).getRightElement();

    assertTrue(read1.toString().equals(s.toString()));
    assertTrue(read2.toString().equals(s2.toString()));

    System.out.println(read1.toString());
    System.out.println(read2.toString());
  }

  public void testSignatureSizeOnDisk() throws IOException {
    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();

    try {
      MinhashSignature s = getRandomSignature();
      fs = FileSystem.get(conf);
      w = SequenceFile.createWriter(fs, conf, new Path("test2"),
          IntWritable.class, MinhashSignature.class);
      for (int i = 0; i < 1000000; i++) {
        w.append(new IntWritable(1), s);
      }
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testBasic() {
    MinhashSignature s = new MinhashSignature(D);

    s.add(1);
    s.add(2);
    assertTrue(s.get(0) == 1);
    assertTrue(s.get(1) == 2);

    s.set(0, 3);
    assertTrue(s.get(0) == 3);
  }

  @Test
  public void testHammingDistance() {
    MinhashSignature s1 = new MinhashSignature(D);
    MinhashSignature s2 = new MinhashSignature(D);
    s1.add(1);
    s1.add(2);
    s1.add(3);
    s1.add(4);
    s1.add(5);

    s2.add(3);
    s2.add(2);
    s2.add(5);
    s2.add(4);
    s2.add(1);

    assertTrue(s1.hammingDistance(s2) == 3);
    assertTrue(s1.hammingDistance(s2, 2) == 3);
    assertTrue(s1.hammingDistance(s2, 5) == 3);

    for (int i = 0; i < 1000; i++) {
      s1.hammingDistance(s2);
    }
    System.out.println(s1.hammingDistance(s2));

  }

  @Test
  public void testCompare() {
    MinhashSignature s1 = new MinhashSignature(D);
    MinhashSignature s2 = new MinhashSignature(D);

    s1.add(1);
    s1.add(2);
    s1.add(3);
    s1.add(4);
    s1.add(5);

    s2.add(3);
    s2.add(2);
    s2.add(5);
    s2.add(4);
    s2.add(1);

    assertTrue(s1.compareTo(s2) + "", s1.compareTo(s2) < 0);
    assertTrue(s2.compareTo(s1) + "", s2.compareTo(s1) > 0);
    assertTrue(s1.compareTo(s1) + "", s1.compareTo(s1) == 0);
    assertTrue(s2.compareTo(s2) + "", s2.compareTo(s2) == 0);

  }

  @Test
  public void testSubSignature() {
    for (int i = 0; i < 100; i++) {

      MinhashSignature s = getRandomSignature();

      System.out.println(s);

      MinhashSignature slide = s.getSubSignature(0, D / 2);
      MinhashSignature slide2 = s.getSubSignature(D / 2 + 1, D - 1);
      System.out.println(slide + "," + slide2);

      assertTrue(s.toString().equals(slide.toString() + "," + slide2.toString()));
    }
    System.out.println("done");
  }
  
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(MinhashSignatureTest.class);
  }
}
