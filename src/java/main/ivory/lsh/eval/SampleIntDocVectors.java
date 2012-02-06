package ivory.lsh.eval;

import ivory.core.data.document.WeightedIntDocVector;
import ivory.lsh.eval.SampleSignatures.mapoutput;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.map.HMapII;

/**
 * <p>
 * A program that samples from a collection of key,value pairs according to a given frequency.
 * </p>
 * 
 * <ul>
 * <li>[input] path to the collection
 * <li>[output-dir] path of the output file containing sample
 * <li>[num-mappers] number of mappers to run
 * <li>[sample-frequency] if entered N, then every Nth <key,value> pair is sampled. N=1 is
 * equivalent to sampling everything.
 * </ul>
 * 
 * <p>
 * User needs to modify the source file to change the key and value class type. Change input and
 * output class type of the mapper, and modify the 3 static fields accordingly.
 * </p>
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar ivory.jar ivory.util.SampleDocVectors 
 * /umd-lin/fture/pwsim/medline/wt-int-doc-vectors 
 * /umd-lin/fture/pwsim/medline/wt-int-doc-vectors-sample 
 * 100
 * </pre>
 * 
 * <p>
 * If there is a text file containing docnos to be sampled (one docno per line), this should be
 * specified as the fifth and last argument. In this case, the sample frequency argument can be
 * anything since it will be ignored.
 * </p>
 * 
 * </blockquote>
 * 
 * 
 * usage: [input] [output-dir] [number-of-mappers] [sample-freq] ([sample-docnos-path])
 * 
 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class SampleIntDocVectors extends Configured implements Tool {
  @SuppressWarnings("unchecked")
  static Class keyClass = IntWritable.class, valueClass = WeightedIntDocVector.class,
      inputFormat = SequenceFileInputFormat.class;

  private static final Logger sLogger = Logger.getLogger(SampleIntDocVectors.class);

  private static int printUsage() {
    System.out
        .println("usage: [input] [output-dir] [number-of-mappers] [sample-freq] ([sample-docnos-path])");
    return -1;
  }

  public SampleIntDocVectors() {

  }

  private static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, WeightedIntDocVector, IntWritable, WeightedIntDocVector> {
    static int sampleFreq;
    HMapII samplesMap = null;
    static Path[] localFiles;

    public void configure(JobConf conf) {
      sampleFreq = conf.getInt("SampleFrequency", -1);

      // try to get local cache
      try {
        localFiles = DistributedCache.getLocalCacheFiles(conf);
      } catch (Exception e) {
        throw new RuntimeException("Error reading doc vectors!");
      }

      // if cache is non-empty, a docnos file has been entered
      if (localFiles != null) {
        sLogger.setLevel(Level.INFO);
        samplesMap = new HMapII();
        try {
          LineReader reader = new LineReader(FileSystem.getLocal(conf).open(localFiles[0]));
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            int docno = Integer.parseInt(t.toString());
            samplesMap.put(docno, 1);
          }
          reader.close();
        } catch (IOException e1) {
        }
        sLogger.info(samplesMap);
      }
    }

    public void map(IntWritable key, WeightedIntDocVector val,
        OutputCollector<IntWritable, WeightedIntDocVector> output, Reporter reporter)
        throws IOException {
      if (samplesMap != null) {
        if (samplesMap.containsKey(key.get())) {
          reporter.incrCounter(mapoutput.count, 1);
          output.collect(key, val);
        }
      } else {
        int randInt = (int) (Math.random() * sampleFreq); // integer in [0,sampleFrq)
        if (randInt == 0) {
          output.collect(key, val);
        }
      }
    }
  }

  public static class MyReducer extends MapReduceBase implements
      Reducer<IntWritable, WeightedIntDocVector, IntWritable, WeightedIntDocVector> {

    @Override
    public void reduce(IntWritable key, Iterator<WeightedIntDocVector> values,
        OutputCollector<IntWritable, WeightedIntDocVector> output, Reporter reporter)
        throws IOException {
      output.collect(key, values.next());
    }
  }

  @SuppressWarnings("unchecked")
  public int run(String[] args) throws Exception {
    boolean isLocal = false;
    if (args.length != 4 && args.length != 5 && !isLocal) {
      printUsage();
      return -1;
    }
    String inputPath = args[0];
    String outputPath = args[1];
    int N = Integer.parseInt(args[2]);
    int sampleFreq = Integer.parseInt(args[3]);

    JobConf job = new JobConf(SampleIntDocVectors.class);
    FileSystem fs;

    if (isLocal) {
      sLogger.info("Running local...");
      job.set("mapred.job.tracker", "local");
      job.set("fs.default.name", "file:///");
      fs = FileSystem.getLocal(job);
    } else {
      fs = FileSystem.get(job);
    }
    job.setJobName(getClass().getName());

    // if sample docnos path provided,
    if (args.length == 5) {
      sampleFreq = -1; // ignore sample frequency
      DistributedCache.addCacheFile(new URI(args[4]), job); // sample doc vectors in file
    }

    int numMappers = N;
    int numReducers = 1;

    if (!fs.exists(new Path(inputPath))) {
      throw new RuntimeException("Error, input path does not exist!");
    }

    sLogger.setLevel(Level.INFO);

    fs.delete(new Path(outputPath), true);
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 100);
    job.setInt("mapred.reduce.max.attempts", 100);
    job.setInt("mapred.task.timeout", 600000000);
    job.setInt("SampleFrequency", sampleFreq);

    sLogger.info("Running job " + job.getJobName());
    sLogger.info("Input directory: " + inputPath);
    sLogger.info("Output directory: " + outputPath);
    sLogger.info("Number of mappers: " + N);
    sLogger.info("Sample frequency: " + sampleFreq);

    job.setNumMapTasks(numMappers);
    job.setNumReduceTasks(numReducers);
    job.setInputFormat(inputFormat);
    job.setMapOutputKeyClass(keyClass);
    job.setMapOutputValueClass(valueClass);
    job.setOutputKeyClass(keyClass);
    job.setOutputValueClass(valueClass);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new SampleIntDocVectors(), args);
    return;
  }
}
