package ivory.lsh.projection;

import ivory.core.RetrievalEnvironment;
import ivory.lsh.data.FloatAsBytesWritable;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.util.PowerTool;

/**
 * @author ferhanture
 * 
 *         This class is a Hadoop task to write randomly generated vectors to a SequenceFile
 * 
 */
@SuppressWarnings("deprecation")
public class WriteRandomVectors extends PowerTool {

  public static final String[] RequiredParameters = {};
  private static final Logger sLogger = Logger.getLogger(WriteRandomVectors.class);

  public WriteRandomVectors(Configuration conf) {
    super(conf);
  }

  /**
   * @author ferhanture
   * 
   *         Identity mapper that passes all work to Reducer. Enables multiple Reducers to write
   *         random vectors simultaneously.
   */
  public static class MyMapper0 extends MapReduceBase implements
      Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void map(IntWritable key, IntWritable value,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      output.collect(key, value);
    }

  }

  /**
   * @author ferhanture
   * 
   *         (K=size of each random vector) needs to be set manually, as the number of terms (i.e.,
   *         vocabulary size) Ideally, this should be passed through the Configuration object, but
   *         that is not checked right now.
   * 
   *         The size of a file is the number of random vectors to be written at a single file.
   *         Multiple files are used if necessary. The parameter FILESIZE should be set through the
   *         ComputeSignatures class' SIZE_OF_FILE field. FILESIZE should be the maximum number of
   *         random vectors such that the size of each file is small enough to fit into memory
   */
  public static class MyReducer0 extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, FloatAsBytesWritable> {

    static int D, K;
    FloatAsBytesWritable v;
    IntWritable keyInt = new IntWritable();

    public void configure(JobConf conf) {
      // sLogger.setLevel(Level.DEBUG);
      D = conf.getInt("D", -1);
      K = conf.getInt("K", -1);
    }

    public void reduce(IntWritable key, Iterator<IntWritable> values,
        OutputCollector<IntWritable, FloatAsBytesWritable> output, Reporter reporter)
        throws IOException {
      for (int i = 0; i < D; i++) {
        int index = (D * key.get()) + i; // just some guaranteed-to-be-unique-for-each-reducer
                                         // number
        v = generateUnitRandomVectorAsBytes(K);
        sLogger.debug("vector " + index + " = " + v.size() + "\n--->" + v.get(0) + "," + v.get(1));
        keyInt.set(index);
        output.collect(keyInt, v);
      }
    }
  }

  @Override
  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  @Override
  public int runTool() throws Exception {
    int D, K;
    D = getConf().getInt("Ivory.NumOfBits", -1);
    String indexPath = getConf().get("Ivory.IndexPath");

    JobConf job = new JobConf(getConf(), WriteRandomVectors.class);
    FileSystem fs = FileSystem.get(job);
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    K = (int) env.readCollectionTermCount();
    job.setJobName("WriteRandomVectors");

    if (D <= 0 || K <= 0) {
      throw new RuntimeException("parameters not read properly");
    }

    // Set parameters
    String inputPath = indexPath + "/files";
    String outputPath = PwsimEnvironment.getFileNameWithPars(indexPath, "RandomVectors");

    if (fs.exists(new Path(outputPath))) {
      sLogger.info("Random vectors output path already exists! Quitting...");
      return 0;
    }
    int numMappers = 1;
    int numReducers = 1;

    SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, new Path(inputPath),
        IntWritable.class, IntWritable.class);
    for (int i = 0; i < numReducers; i++) {
      writer.append(new IntWritable(i), new IntWritable(i));
    }
    writer.close();

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("K", K);
    job.setInt("D", D);

    sLogger.info("Random vectors...");
    sLogger.info("Total number of vectors: " + D);
    sLogger.info("Vector size: " + K);
    sLogger.info("InputPath: " + inputPath);
    sLogger.info("outputPath: " + outputPath);

    job.setNumMapTasks(numMappers);
    job.setNumReduceTasks(numReducers);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatAsBytesWritable.class);
    job.setMapperClass(MyMapper0.class);
    job.setReducerClass(MyReducer0.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    return 0;
  }

  /**
   * 
   * This piece of code is based on a publicly available Java code by Kevin Wayne, as referenced
   * below.
   * 
   * @param numSamples number of elements in each random vector
   * @return a random vector of numSamples random float values
   */
  public static ArrayListOfFloatsWritable generateUnitRandomVector(int numSamples) {
    /*************************************************************************
     * Author: Kevin Wayne Date: 8/20/04 Compilation: javac StdGaussian.java Execution: java
     * StdGaussian
     **************************************************************************/
    double r, x, y;
    ArrayListOfFloatsWritable vector = new ArrayListOfFloatsWritable(numSamples);
    vector.setSize(numSamples);

    double normalizationFactor = 0;
    for (int i = 0; i < numSamples; i++) {

      // find a uniform random point (x, y) inside unit circle
      do {
        x = 2.0 * Math.random() - 1.0;
        y = 2.0 * Math.random() - 1.0;
        r = x * x + y * y;
      } while (r > 1 || r == 0); // loop executed 4 / pi = 1.273.. times on average
      // http://en.wikipedia.org/wiki/Box-Muller_transform

      // apply the Box-Muller formula to get standard Gaussian z
      double f = (x * Math.sqrt(-2.0 * Math.log(r) / r));
      normalizationFactor += Math.pow(f, 2.0);
      vector.set(i, (float) f);
    }

    /* normalize vector */
    normalizationFactor = Math.sqrt(normalizationFactor);
    for (int i = 0; i < vector.size(); i++) {
      float val = vector.get(i);
      float newf = (float) (val / normalizationFactor);
      vector.set(i, newf);
    }
    return vector;
  }

  public static FloatAsBytesWritable generateUnitRandomVectorAsBytes(int numSamples) {
    double r, x, y;
    ArrayListOfFloatsWritable vector = new ArrayListOfFloatsWritable(numSamples);
    vector.setSize(numSamples);

    byte[] bytes = new byte[numSamples];
    float max = Float.MIN_VALUE;
    float min = Float.MAX_VALUE;

    for (int i = 0; i < numSamples; i++) {

      // find a uniform random point (x, y) inside unit circle
      do {
        x = 2.0 * Math.random() - 1.0;
        y = 2.0 * Math.random() - 1.0;
        r = x * x + y * y;
      } while (r > 1 || r == 0); // loop executed 4 / pi = 1.273.. times on average
      // http://en.wikipedia.org/wiki/Box-Muller_transform

      // apply the Box-Muller formula to get standard Gaussian z
      float f = (float) (x * Math.sqrt(-2.0 * Math.log(r) / r));
      vector.set(i, f);
      if (f > 0 && f > max) {
        max = f;
      } else if (f < 0 && f < min) {
        min = f;
      }

    }

    // System.out.println(max);
    // System.out.println(min);

    /* normalize vector */
    for (int i = 0; i < vector.size(); i++) {
      float val = vector.get(i);
      float normalized2one = 0.0f;
      // map values to [-1,1] range
      if (val > 0) {
        normalized2one = val / max;
      } else if (val < 0) {
        normalized2one = val / Math.abs(min);
      }
      // System.out.println("normalized to [-1,1]: "+val + "=>" + normalized2one);

      // quantize float to byte
      int byteInt = (int) (normalized2one * (Byte.MAX_VALUE + 1));

      byte b;
      if (byteInt > Byte.MAX_VALUE) {
        b = (byte) Byte.MAX_VALUE;
      } else {
        b = (byte) byteInt;
      }

      // store quantized byte value
      // System.out.println("quantized: "+normalized2one + "=>" + b);
      bytes[i] = b;
    }
    FloatAsBytesWritable vector2 = new FloatAsBytesWritable(bytes, max, min);

    // debugging
    // float sum = 0;
    // for(int i=0;i<numSamples;i++){
    // float f1 = vector.get(i);
    // float f2 = vector2.getAsFloat(i);
    // System.out.println(f1+" "+f2+" = "+Math.abs(f1-f2));
    // sum+=Math.pow(f1-f2,2);
    // }
    // System.out.println(Math.sqrt(sum));

    return vector2;
  }

}
