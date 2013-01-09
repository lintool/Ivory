package ivory.lsh.projection;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.lsh.data.FloatAsBytesWritable;
import ivory.lsh.data.NBitSignature;
import ivory.lsh.driver.PwsimEnvironment;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapIF;

/**
 * A Hadoop task to compute signatures from document vectors.
 * 
 * usage: [index-path] [num-of-bits] [type-of-computation] ([batch-size])
 * 
 * @author ferhanture
 * 
 * 
 */
public class ComputeSignaturesRandom extends PowerTool {

  public ComputeSignaturesRandom(Configuration conf) {
    super(conf);
  }

  public static final String[] RequiredParameters = {};
  private static final Logger sLogger = Logger.getLogger(ComputeSignaturesRandom.class);

  static {
    sLogger.setLevel(Level.INFO);
  }

  protected static enum Maps {
    ALL, ONES, ZEROS, EMPTY
  };


  /**
   * Convert int doc vectors into NBitSignature objects using LSH.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, WeightedIntDocVector, IntWritable, NBitSignature> {

    static Path[] localFiles;
    static List<Writable> randomUnitVectors;
    static int D;
    static NBitSignature signature;
    static float[] dotProductThresholds;

    private String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }

    @SuppressWarnings("deprecation")
    public void configure(JobConf job) {
      // sLogger.setLevel(Level.DEBUG);
      D = job.getInt("Ivory.NumOfBits", -1);
      if (D == -1) {
        throw new RuntimeException("Could not read parameters!");
      }

      String inCacheFile = job.get("InCache");
      try {
        inCacheFile = getFilename(inCacheFile);
        localFiles = DistributedCache.getLocalCacheFiles(job);
        for (Path localFile : localFiles) {
          sLogger.info("in cache " + localFile);
          if (localFile.toString().contains(inCacheFile)) {            
            randomUnitVectors = SequenceFileUtils.readValues(localFile, FileSystem.getLocal(job));
          }
        }
        if (randomUnitVectors == null)  throw new RuntimeException("File not found in local cache: " + inCacheFile);
      } catch (Exception e) {
        throw new RuntimeException("Error reading random vectors!\n" + e.getMessage());
      }
      signature = new NBitSignature(D);
    }

    static public double dotProduct(HMapIFW docvector, FloatAsBytesWritable vector) {
      float s = 0;

      for (MapIF.Entry f : docvector.entrySet()) {
        int indexOfTerm = f.getKey();
        s += f.getValue() * vector.getAsFloat(indexOfTerm - 1);
      }
      return s;
    }

    static public double dotProduct(HMapIFW docvector, ArrayListOfFloatsWritable vector) {
      float s = 0;

      for (MapIF.Entry f : docvector.entrySet()) {
        int indexOfTerm = f.getKey();
        s += f.getValue() * vector.get(indexOfTerm - 1);
      }
      return s;
    }

    public void map(IntWritable docno, WeightedIntDocVector docvectorIn,
        OutputCollector<IntWritable, NBitSignature> output, Reporter reporter) throws IOException {
      HMapIFW docvector = docvectorIn.getWeightedTerms();
      FloatAsBytesWritable value;

      for (int i = 0; i < randomUnitVectors.size(); i++) {
        value = (FloatAsBytesWritable) randomUnitVectors.get(i);
        double dprod = dotProduct(docvector, value);
        boolean sign = (dprod >= (dotProductThresholds == null ? 0 : dotProductThresholds[i])) ? true
            : false;
        signature.set(i, sign);
      }
      sLogger.debug("Doc vector " + docvector + " mapped to \nBitsSignature: " + docno + "\n"
          + signature);
      output.collect(docno, signature);
    }
  }

  @SuppressWarnings("deprecation")
  public int runTool() throws Exception {
    int D = getConf().getInt("Ivory.NumOfBits", -1);
    int numBatchFiles = getConf().getInt("NumBatch", 0);
    boolean isBatch = (numBatchFiles != 0);
    String dir = getConf().get("Ivory.IndexPath");
    if (D < 0 || numBatchFiles < 0) {
      throw new RuntimeException("Parameters not read properly! Quitting...");
    }
    JobConf job = new JobConf(getConf(), ComputeSignaturesRandom.class);
    FileSystem fs = FileSystem.get(job);

    RetrievalEnvironment re = new RetrievalEnvironment(dir, fs);
    job.setJobName("ComputeSignatures_random_D=" + D + ":" + re.readCollectionName());

    String inputPath = PwsimEnvironment.getIntDocvectorsFile(dir, fs);
    String outputPath = PwsimEnvironment.getSignaturesDir(dir, D, "random"); 
    String randomVectorFile = PwsimEnvironment.getRandomVectorsDir(dir, D) + "/part-00000";

    job.set("InCache", randomVectorFile);
    DistributedCache.addCacheFile(new URI(randomVectorFile), job);

    int numMappers = 300;
    if (fs.exists(new Path(outputPath))) {
      sLogger.info("Signatures output path already exists! Quitting...");
      return 0;
    }

    sLogger.info("Computing signatures...");
    sLogger.info("Type of computation: Random");
    sLogger.info("Total number of bits: " + D);
    sLogger.info("randomVectorFile: " + randomVectorFile);
    sLogger.info("InputPath: " + inputPath);
    sLogger.info("outputPath: " + outputPath);
    sLogger.info("Batch?: " + isBatch);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("mapred.task.timeout", 6000000);

    job.setNumMapTasks(numMappers);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NBitSignature.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(NBitSignature.class);
    job.setMapperClass(MyMapper.class);

    job.setOutputFormat(SequenceFileOutputFormat.class);

    if (isBatch) {
      job.setNumReduceTasks(numBatchFiles);
      job.setReducerClass(IdentityReducer.class);
    } else {
      job.setNumReduceTasks(0);
    }
    long startTime = System.currentTimeMillis();
    JobClient.runJob(job);
    System.out.println("Job finished in " + (System.currentTimeMillis() - startTime)
        + " milliseconds");

    return 0;
  }

  @Override
  public String[] getRequiredParameters() {
    return new String[] {};
  }

}
