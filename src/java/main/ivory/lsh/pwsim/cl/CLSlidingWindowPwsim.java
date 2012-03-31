package ivory.lsh.pwsim.cl;

import ivory.lsh.data.BitsSignatureTable;
import ivory.lsh.data.Signature;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.io.pair.PairOfInts;

/**
 * Implementation of sliding window algorithm for cross-lingual pairwise similarity (see Ture et al,
 * SIGIR'11 for details and pseudocode)
 * 
 * @author ferhanture
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class CLSlidingWindowPwsim extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(CLSlidingWindowPwsim.class);

  static enum mapoutput {
    count, PROCESSEDPAIRS, EMITTEDPAIRS, PrefixSum
  }

  private static int printUsage() {
    System.out
        .println("usage: [input-path] [output-path] [window-size] [max-distance] ([sample-docnos])");
    return -1;
  }

  public CLSlidingWindowPwsim() {
    super();
  }

  public static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, BitsSignatureTable, PairOfInts, IntWritable> {
    static int slidingWindowSize, maxDist;

    Signature[] signatures = null;
    int[] docNos = null;
    Path[] localFiles;
    HMapIIW samplesMap = null;
    int hammingDistance;
    PairOfInts outKey = new PairOfInts();
    IntWritable outValue = new IntWritable();
    int nSignatures = -1;

    public void configure(JobConf conf) {
      slidingWindowSize = conf.getInt("Ivory.SlidingWindowSize", -1);
      maxDist = conf.getInt("Ivory.MaxHammingDistance", -1);

      // sLogger.setLevel(Level.DEBUG);
      sLogger.setLevel(Level.INFO);

      sLogger.info("configure");
      sLogger.info(maxDist);
      sLogger.info(slidingWindowSize);

      // read doc ids of sample into vectors
      try {
        localFiles = DistributedCache.getLocalCacheFiles(conf);
      } catch (Exception e) {
        throw new RuntimeException("Error reading doc vectors!");
      }

      if (localFiles != null && localFiles.length > 0) {
        samplesMap = new HMapIIW();
        try {
          LineReader reader = new LineReader(FileSystem.get(conf).open(new Path(conf.get("Ivory.SampleFile"))));
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            int docno = Integer.parseInt(t.toString());
            sLogger.info(docno + " --> sample");
            samplesMap.put(docno, 1);
          }
          reader.close();
        } catch (IOException e1) {
        }
        sLogger.info(samplesMap.size() + " sampled");
      } else {
        sLogger.info("samples file does not exist");
      }
    }

    public void map(IntWritable permNo, BitsSignatureTable signatureTable,
        OutputCollector<PairOfInts, IntWritable> output, Reporter reporter) throws IOException {
      signatures = signatureTable.getSignatures();
      docNos = signatureTable.getDocNos();
      nSignatures = signatureTable.getNumOfSignatures();
      for (int i = 0; i < nSignatures; i++) {
        if ((docNos[i] > 1000000000 && samplesMap == null)
            || (samplesMap != null && samplesMap.containsKey(docNos[i]))) {
          for (int j = i - 1; j > i - slidingWindowSize && j >= 0; j--) {
            if (docNos[j] > 1000000000) {
              continue;
            }
            int prefix = signatures[i].getLongestPrefix(signatures[j]);
            reporter.incrCounter(mapoutput.PrefixSum, prefix);
            reporter.incrCounter(mapoutput.PROCESSEDPAIRS, 1);
            hammingDistance = signatures[i].hammingDistance(signatures[j], maxDist);
            sLogger.debug(hammingDistance);
            if (hammingDistance <= maxDist) {
              reporter.incrCounter(mapoutput.EMITTEDPAIRS, 1);

              // If filtering results by a sample set (i.e., samplesMap!=null), change output format
              // outValue.set(docNos[i]);
              // outKey.set(hammingDistance, docNos[j]); //pair format: english docno first, then
              // german docno
              // output.collect(outValue, outKey);

              outValue.set(hammingDistance);
              outKey.set(docNos[j], docNos[i]); // pair format: english docno first, then german
                                                // docno
              output.collect(outKey, outValue);
            }
          }
          for (int j = i + 1; j < i + slidingWindowSize && j < nSignatures; j++) {
            if (docNos[j] > 1000000000) {
              continue;
            }
            int prefix = signatures[i].getLongestPrefix(signatures[j]);
            reporter.incrCounter(mapoutput.PrefixSum, prefix);
            reporter.incrCounter(mapoutput.PROCESSEDPAIRS, 1);
            hammingDistance = signatures[i].hammingDistance(signatures[j], maxDist);
            sLogger.debug(hammingDistance);
            if (hammingDistance <= maxDist) {
              reporter.incrCounter(mapoutput.EMITTEDPAIRS, 1);

              // If filtering results by a sample set (i.e., samplesMap!=null), change output format
              // outValue.set(docNos[i]);
              // outKey.set(hammingDistance, docNos[j]);
              // output.collect(outValue, outKey);

              outValue.set(hammingDistance);
              outKey.set(docNos[j], docNos[i]);
              output.collect(outKey, outValue);
            }
          }
        }
      }
    }

  }

  // Use this Reducer class when the output of Mapper is (PairOfInts,IntWritable)
  // same as IdentityReducer? the goal is to get rid of duplicate key-value pairs
  public static class MyReducer extends MapReduceBase implements
      Reducer<PairOfInts, IntWritable, PairOfInts, IntWritable> {
    IntWritable outValue = new IntWritable();
    HashMap<String, Integer> map = new HashMap<String, Integer>();

    public void reduce(PairOfInts key, Iterator<IntWritable> val,
        OutputCollector<PairOfInts, IntWritable> output, Reporter reporter) throws IOException {
      output.collect(key, val.next());
    }
  }

  // Use this Reducer class when the output of Mapper is (IntWritable,PairOfInts)
  // the goal is to output top N similar pairs for each key document
  public static class MyReducerTopN extends MapReduceBase implements
      Reducer<IntWritable, PairOfInts, IntWritable, PairOfInts> {
    int numResults;
    TreeSet<PairOfInts> list = new TreeSet<PairOfInts>();

    public void configure(JobConf conf) {
      numResults = conf.getInt("Ivory.NumResults", -1);
      sLogger.info("numResults");
    }

    public void reduce(IntWritable key, Iterator<PairOfInts> values,
        OutputCollector<IntWritable, PairOfInts> output, Reporter reporter) throws IOException {
      list.clear();
      while (values.hasNext()) {
        PairOfInts p = values.next();
        list.add(new PairOfInts(p.getLeftElement(), p.getRightElement()));
        reporter.incrCounter(mapoutput.count, 1);
      }
      int cntr = 0;
      while (!list.isEmpty() && cntr < numResults) {
        output.collect(key, list.pollFirst());
        cntr++;
      }
    }
  }

  public static final String[] RequiredParameters = { "Ivory.NumMapTasks", "Ivory.NumReduceTasks",
      "Ivory.CollectionName", "Ivory.NumOfPermutations", "Ivory.SlidingWindowSize",
      "Ivory.MaxHammingDistance", };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public int run(String[] args) throws Exception {
    if (args.length != 4 && args.length != 5) {
      printUsage();
    }

    JobConf job = new JobConf(getConf(), CLSlidingWindowPwsim.class);
    FileSystem fs = FileSystem.get(job);
    if (!PwsimEnvironment.cluster) {
      job.set("mapred.job.tracker", "local");
      job.set("fs.default.name", "file:///");
      fs = FileSystem.getLocal(job);
    }
    String inputPath = args[0]; // PwsimEnvironment.getFileNameWithPars(dir, "Tables");
    String outputPath = args[1]; // PwsimEnvironment.getFileNameWithPars(dir, "PWSimCollection");
    int numMappers = job.getInt("Ivory.NumMapTasks", 100);
    int numReducers = job.getInt("Ivory.NumReduceTasks", 1);
    int windowSize = Integer.parseInt(args[2]);
    int maxDist = Integer.parseInt(args[3]);
    int numResults = job.getInt("Ivory.NumResults", -1);

    if (fs.exists(new Path(outputPath))) {
      sLogger.info("SlidingWindowPwsim output already exists! Quitting...\nPath: " + outputPath);
      return 0;
    }

    String samplesFile = "";
    if (args.length == 5) {
      samplesFile = args[4];
      DistributedCache.addCacheFile(new URI(samplesFile), job); // sample doc vectors in file
    }

    String collectionName = job.get("Ivory.CollectionName");
    job.setJobName("SlidingWindowPwsim:" + collectionName + args[0].replaceFirst("tables", "")
        + "_B=" + windowSize + "_" + numResults);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.task.timeout", 60000000);
    job.setInt("Ivory.SlidingWindowSize", windowSize);
    job.setInt("Ivory.MaxHammingDistance", maxDist);
    job.set("Ivory.SampleFile", samplesFile);
    job.setNumMapTasks(numMappers);
    job.setNumReduceTasks(numReducers);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(PairOfInts.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(MyMapper.class);
    if (numResults == -1) {
      job.setReducerClass(MyReducer.class);
    } else {
      job.setReducerClass(MyReducerTopN.class);
    }
    if (samplesFile.equals("")) { // if sample file is provided, output should be text.
      job.setOutputFormat(SequenceFileOutputFormat.class);
    } else {
      job.setOutputFormat(TextOutputFormat.class);
      sLogger.info("text output");
    }

    sLogger.info("Running job " + job.getJobName() + "...");
    sLogger.info("Input path: " + inputPath);
    sLogger.info("Output path: " + outputPath);
    sLogger.info("Window size: " + windowSize);
    sLogger.info("Threshold: " + maxDist);
    sLogger.info("Sample file?: " + !samplesFile.equals(""));
    sLogger.info("Number of results: " + (numResults == -1 ? "all" : numResults));

    long startTime = System.currentTimeMillis();
    RunningJob j = JobClient.runJob(job);
    System.out.println("Job finished in " + (System.currentTimeMillis() - startTime)
        + " milliseconds");
    Counters counters = j.getCounters();
    long processed = (long) counters.findCounter(mapoutput.PROCESSEDPAIRS).getCounter();
    long prefixsum = (long) counters.findCounter(mapoutput.PrefixSum).getCounter();
    System.out.println("Avg prefix length = " + (prefixsum / (float) processed));

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CLSlidingWindowPwsim(), args);
    return;
  }

}
