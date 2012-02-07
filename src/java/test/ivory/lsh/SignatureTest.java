package ivory.lsh;

import static org.junit.Assert.assertTrue;
import ivory.lsh.data.Bits;
import ivory.lsh.data.MinhashSignature;
import ivory.lsh.data.NBitSignature;
import ivory.lsh.data.PermutationByBit;
import ivory.lsh.data.PermutationByBlock;
import ivory.lsh.data.Signature;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

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

public class SignatureTest {
  private static final String TMP_FILENAME1 = "tmp1.out";
  private static int D = 20;

  private NBitSignature getRandomSignature() {
    NBitSignature s = new NBitSignature(D);
    for (int i = 0; i < s.size(); i++) {
      s.set(i, (Math.random() > 0.5 ? true : false));
    }
    return s;
  }

  private MinhashSignature getRandomMinhashSignature() {
    MinhashSignature s = new MinhashSignature(D);
    for (int i = 0; i < s.size(); i++) {
      s.add((int) (Math.random() * 39000));
    }
    return s;
  }

  @Test
  public void testPermuteBit() throws IOException {
    PermutationByBit p = new PermutationByBit(D);

    NBitSignature s = getRandomSignature();

    int cntBits = s.countSetBits();

    int loopcnt = 0;
    NBitSignature permutedS = new NBitSignature(D);
    while (loopcnt++ < 100) {
      ArrayListOfIntsWritable a = p.nextPermutation();
      s.perm(a, permutedS);
      System.out.println(permutedS.countSetBits());
      assertTrue(cntBits == permutedS.countSetBits());
      // System.out.println(permutedS);
    }
  }

  @Test
  public void testPermuteBlk() throws IOException {
    PermutationByBlock p = new PermutationByBlock(64);
    int tempD = D;
    D = 64;
    NBitSignature s = getRandomSignature();
    System.out.println(s + "\n========================");

    int cntBits = s.countSetBits();
    System.out.println(s.countSetBits());

    int loopcnt = 0;
    NBitSignature permutedS = new NBitSignature(D);
    while (true) {
      try {
        ArrayListOfIntsWritable a = p.nextPermutation();
        s.perm(a, permutedS);
        System.out.println((loopcnt++) + "\n" + permutedS);
        assertTrue(cntBits == permutedS.countSetBits());
        // System.out.println(permutedS);
      } catch (RuntimeException e) {
        D = tempD;
        return;
      }
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    NBitSignature s = getRandomSignature();
    NBitSignature s2 = getRandomSignature();

    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();

    fs = FileSystem.get(conf);
    w = SequenceFile.createWriter(fs, conf, new Path(TMP_FILENAME1),
          IntWritable.class, NBitSignature.class);
    w.append(new IntWritable(1), s);
    w.append(new IntWritable(2), s2);
    w.close();

    List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs =
        SequenceFileUtils.readFile(new Path(TMP_FILENAME1));
    fs.delete(new Path(TMP_FILENAME1), true);

    NBitSignature read1 = (NBitSignature) listOfKeysPairs.get(0).getRightElement();
    NBitSignature read2 = (NBitSignature) listOfKeysPairs.get(1).getRightElement();

    assertTrue(read1.toString().equals(s.toString()));
    assertTrue(read2.toString().equals(s2.toString()));

    // System.out.println(read1.toString());
    // System.out.println(read2.toString());
  }

  public void testSignatureSizeOnDisk() throws IOException {
    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();

    fs = FileSystem.get(conf);
    w = SequenceFile.createWriter(fs, conf, new Path(TMP_FILENAME1),
          IntWritable.class, NBitSignature.class);
    for (int i = 0; i < 100; i++) {
      NBitSignature s = new NBitSignature(64);
      w.append(new IntWritable(1), s);
    }
    w.close();

    fs.delete(new Path(TMP_FILENAME1), true);
  }

  public void testWrite() throws IOException {
    NBitSignature s = new NBitSignature(1000);

    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();

    try {
      fs = FileSystem.get(conf);
      w = SequenceFile.createWriter(fs, conf, new Path("test"),
          IntWritable.class, NBitSignature.class);
      int i = 0;
      while (i++ < 100) {
        if (i % 100000 == 0)
          System.out.println(i + "=" + s);
        w.append(new IntWritable(i), s);
      }
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testBasic() {
    NBitSignature s = new NBitSignature(D);
    s.set(0, true);
    s.set(1, false);
    assertTrue(s.get(0));
    assertTrue(!s.get(1));

    s.set(0, false);
    assertTrue(!s.get(0));
  }

  @Test
  public void testHammingDistance() {
    Bits b1 = new Bits("11100011010111101010");// 11000001011100011110001111100000101001011110111111010111101100101111101100110001110000101000011010001000001001111010111001101001100100011100101010111100000110001101001001010000100001111000000110011110001010101110001001001111010111010111100011001111111000110110000010001100111000001010010000110000000010100101110011001110111011110011010001000110110100111000001001111000111110111111100101011000010110000110100101001101000001110001110110010101000101001001000000000110110101001011111010111010101010001010010000111000001100100101110001111111011100001101011101000011110011111111010001001100001100111110000010111110101010000000101100011110011000010100001011101011001111111011011110010011110101000010001110011100111100101000110111010100011111101000100111010111111100001110110001011010110000000011100000001011101100000001100110010100100000011111001110010101111100100100000110010100010011110111001101110010111010001101110110001010000000000101010010011111010110000110110111100101110010100111");
    NBitSignature s1 = new NBitSignature(b1);
    Bits b2 = new Bits("11101111111111110000");// 01110010110000000101011111000000101001011111111000000110011010111100100111110000010010010101001101101000100100010111111101101001000010010101111110111100010110000100100000010101100010001000100110110110001001100100000111000100010111000101101001001011110001110011000000000010100100001011001110100010001100000100100000111110111110101011000101000111111010001000001101011110100001110110110111110100000010011100000100101101000010110001100110110111000001001000011110000100011001010100100011111110101010000100110000100010000000100101110100011111101110101001011001100100010011111011000110101000010101110000101111100100101001011111110110100110011010000111111001001010100111100111000100000000010011000010101110001101011110110000110011010111000110100100000111001111110101011010110110001000111011111010100010011001100110001000110110111100011101011111001111100101000000010000010111011011011101101100100110111010101111010101110010111011101100011111110010110110010010101010000101101000111100010110");
    NBitSignature s2 = new NBitSignature(b2);

    assertTrue(7 == s1.hammingDistance(s2));
    assertTrue(7 == s2.hammingDistance(s1));
  }

  public void benchmark() {
    int origD = this.D;
    this.D = 1000;
    NBitSignature s1 = this.getRandomSignature();
    NBitSignature s2 = this.getRandomSignature();
    long time = System.currentTimeMillis();
    int i = 0;
    while (i++ < 15000000) {
      s1.hammingDistance(s2, 400);
    }
    System.out.println("Bit signatures finished in " + (System.currentTimeMillis() - time) / 1000.0);

    this.D = 31;
    MinhashSignature m1 = this.getRandomMinhashSignature();
    MinhashSignature m2 = this.getRandomMinhashSignature();
    time = System.currentTimeMillis();
    i = 0;
    while (i++ < 3000000) {
      m1.hammingDistance(m2);
    }
    System.out.println("Minhash signatures finished in " + (System.currentTimeMillis() - time)
        / 1000.0);

    this.D = origD;
  }

  @Test
  public void testCompare() {
    Bits b1 = new Bits("01111111111111111111111111111110");
    Signature s1 = new NBitSignature(b1);
    Bits b2 = new Bits("10000000000000000000000000000001");
    Signature s2 = new NBitSignature(b2);

    assertTrue(s1.compareTo(s2) + "", s1.compareTo(s2) < 0);
    assertTrue(s2.compareTo(s1) + "", s2.compareTo(s1) > 0);
    assertTrue(s1.compareTo(s1) + "", s1.compareTo(s1) == 0);
    assertTrue(s2.compareTo(s2) + "", s2.compareTo(s2) == 0);

  }

  @Test
  public void testSort() {
    // 00000000
    NBitSignature s1 = new NBitSignature(64);
    for (int i = 0; i < 64; i++) {
      s1.set(i, false);
    }

    // 10000000
    NBitSignature s2 = new NBitSignature(64);
    s2.set(0, true);

    // 01000000
    NBitSignature s3 = new NBitSignature(64);
    s3.set(1, true);

    // 000100000
    NBitSignature s4 = new NBitSignature(64);
    s4.set(3, true);

    PriorityQueue<NBitSignature> sorted = new PriorityQueue<NBitSignature>();
    sorted.add(s1);
    sorted.add(s2);
    sorted.add(s3);
    sorted.add(s4);

    assertTrue(sorted.poll() == s1);
    assertTrue(sorted.poll() == s4);
    assertTrue(sorted.poll() == s3);
    assertTrue(sorted.poll() == s2);

  }

  // @Test
  // public void testGetSlide(){
  // BitsSignature s = new BitsSignature(10);
  // BitsSignature s2 = new BitsSignature(10);
  //
  // Slide slide = s.getSlide(0, 5);
  // Slide slide2 = s.getSlide(0, 5);
  //
  // System.out.println(slide);
  // System.out.println(slide.hashCode());
  // System.out.println(slide2);
  // System.out.println(slide2.hashCode());
  //
  //		
  // System.out.println(-299566668 % 100);
  // System.out.println(Integer.MIN_VALUE);
  //		
  //		
  // int i=0, j=0;
  // float sum=0;
  // while(i<100000000){
  // while(j<100000000){
  // float f1 = (float) Math.random();
  // float f2 = (float) Math.random();
  // float f = (f1*f2);
  // sum = sum + f;
  // j++;
  // }
  // i++;
  // }
  //
  //		
  // }

  @Test
  public void testSubSignature() {
    PermutationByBit p = new PermutationByBit(D);
    ArrayListOfIntsWritable a = p.nextPermutation();

    for (int i = 0; i < 100000; i++) {

      NBitSignature s = getRandomSignature();

      // System.out.println(s);

      NBitSignature slide = s.getSubSignature(0, D / 2);
      NBitSignature slide2 = s.getSubSignature(D / 2 + 1, D - 1);
      // System.out.println(slide+""+slide2);

      assertTrue(s.toString().equals(slide.toString() + slide2.toString()));
    }
    System.out.println("done");

    // System.out.println(slide);
    // System.out.println(slide2);

  }
  // @Test
  // public void testExtract(){
  // BitsSignature s = new BitsSignature(10);
  // s.set(2, true);
  // // s.set(4, true);
  //
  // BitsSignature sub = s.getSubSignature(0, 4);
  // System.out.println(sub);
  // System.out.println(s);
  //
  // }
}
