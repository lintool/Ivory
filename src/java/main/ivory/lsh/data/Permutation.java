package ivory.lsh.data;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

@SuppressWarnings("deprecation")
public abstract class Permutation {

  public abstract ArrayListOfIntsWritable nextPermutation();

  public static void writeToFile(Permutation p, int numPerms, FileSystem fs, JobConf job,
      String fileName) throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, new Path(fileName),
        IntWritable.class, ArrayListOfIntsWritable.class);

    for (int j = 0; j < numPerms; j++) {
      ArrayListOfIntsWritable perm = p.nextPermutation();
      writer.append(new IntWritable(j), perm);
      // sLogger.debug(j +":"+perm);
    }
    writer.close();
  }
}
