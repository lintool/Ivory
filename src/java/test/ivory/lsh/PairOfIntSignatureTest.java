package ivory.lsh;

import ivory.lsh.data.NBitSignature;
import ivory.lsh.data.PairOfIntNBitSignature;
import ivory.lsh.data.PairOfIntSignature;
import java.io.IOException;
import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.io.SequenceFileUtils;

public class PairOfIntSignatureTest {
  private static final String TMP_FILENAME1 = "PairOfIntSignatureTest.out";

  @Test
  public void testReadWrite() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    // write
    SequenceFile.Writer w = SequenceFile.createWriter(fs, conf,
        new Path(TMP_FILENAME1), IntWritable.class, PairOfIntNBitSignature.class);

    PairOfIntNBitSignature p1 = new PairOfIntNBitSignature(1, null);
    PairOfIntNBitSignature p2 = new PairOfIntNBitSignature(2, new NBitSignature(100));
    PairOfIntNBitSignature p3 = new PairOfIntNBitSignature(3, null);

    w.append(new IntWritable(1), p1);
    w.append(new IntWritable(2), p2);
    w.append(new IntWritable(3), p3);
    w.close();

    // read
    List<PairOfWritables<Writable, Writable>> listOfKeysPairs =
        SequenceFileUtils.readFile(new Path(TMP_FILENAME1), fs);

    // delete
    FileSystem.get(new Configuration()).delete(new Path(TMP_FILENAME1), true);

    PairOfIntSignature a1 = (PairOfIntSignature) listOfKeysPairs.get(0).getRightElement();
    PairOfIntSignature a2 = (PairOfIntSignature) listOfKeysPairs.get(1).getRightElement();
    PairOfIntSignature a3 = (PairOfIntSignature) listOfKeysPairs.get(2).getRightElement();

    System.out.println(a1);
    System.out.println(a2);
    System.out.println(a3);

  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PairOfIntSignatureTest.class);
  }
}
