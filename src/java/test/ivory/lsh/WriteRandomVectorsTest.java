package ivory.lsh;

import static org.junit.Assert.assertTrue;
import ivory.lsh.data.FloatAsBytesWritable;
import ivory.lsh.projection.WriteRandomVectors;

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

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.pair.PairOfWritables;
import edu.umd.cloud9.io.SequenceFileUtils;

public class WriteRandomVectorsTest {
  private static final String TMP_FILENAME1 = "tmp1.out";
  private static final String TMP_FILENAME2 = "tmp2.out";

  @Test
  public void testBasic() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new Configuration());
    
    SequenceFile.Writer writer1 = SequenceFile.createWriter(fs, conf,
          new Path(TMP_FILENAME1), IntWritable.class, ArrayListOfFloatsWritable.class);
    SequenceFile.Writer writer2 = SequenceFile.createWriter(fs, conf,
          new Path(TMP_FILENAME2), IntWritable.class, FloatAsBytesWritable.class);

    ArrayListOfFloatsWritable a1 = WriteRandomVectors.generateUnitRandomVector(100);
    FloatAsBytesWritable a2 = WriteRandomVectors.generateUnitRandomVectorAsBytes(100);

    writer1.append(new IntWritable(1), a1);
    writer1.close();

    writer2.append(new IntWritable(1), a2);
    writer2.close();

    List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs1 =
      SequenceFileUtils.readFile(new Path(TMP_FILENAME1));
    fs.delete(new Path(TMP_FILENAME1), true);

    List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs2 =
      SequenceFileUtils.readFile(new Path(TMP_FILENAME2));
    fs.delete(new Path(TMP_FILENAME2), true);

    FloatAsBytesWritable b2 = (FloatAsBytesWritable) listOfKeysPairs2.get(0).getRightElement();
    ArrayListOfFloatsWritable b1 = (ArrayListOfFloatsWritable) listOfKeysPairs1.get(0)
        .getRightElement();

    for (int i = 0; i < 100; i++) {
      float f = b1.get(i);
      float g = a1.get(i);
      assertTrue(f == g);
    }
    for (int i = 0; i < 100; i++) {
      float f2 = b2.getAsFloat(i);
      float g2 = a2.getAsFloat(i);
      assertTrue(f2 == g2);
    }
  }
  
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(WriteRandomVectorsTest.class);
  }
}
