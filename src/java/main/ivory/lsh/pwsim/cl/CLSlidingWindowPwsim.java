package ivory.lsh.pwsim.cl;

import ivory.lsh.data.BitsSignatureTable;
import ivory.lsh.data.Signature;
import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import tl.lin.data.map.HMapIIW;
import tl.lin.data.pair.PairOfInts;

/**
 * Implementation of sliding window algorithm for cross-lingual pairwise similarity (see Ture et al,
 * SIGIR'11 for details and pseudocode)
 * 
 * @author ferhanture
 * 
 * 
 */
public class CLSlidingWindowPwsim extends Configured implements Tool {

  private static final Logger sLogger = Logger.getLogger(CLSlidingWindowPwsim.class);

  static enum Pairs {
    Processed, Emitted, PrefixSum
  }

  public static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, BitsSignatureTable, PairOfInts, IntWritable> {
    static int slidingWindowSize, maxDist;

    private Signature[] signatures = null;
    private int[] docNos = null;
    private HMapIIW samplesMap = null;
    private int hammingDistance;
    private PairOfInts outKey = new PairOfInts();
    private IntWritable outValue = new IntWritable();
    private int nSignatures = -1;

    private String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }

    public void configure(JobConf conf) {
      sLogger.setLevel(Level.INFO);

      slidingWindowSize = conf.getInt("Ivory.SlidingWindowSize", -1);
      maxDist = conf.getInt("Ivory.MaxHammingDistance", -1);

      // read doc ids of sample into vectors
      String samplesFile = conf.get("Ivory.SampleFile"); 
      if (samplesFile != null) {
        try {
          samplesMap = readSamplesFromCache(getFilename(samplesFile), conf);
        } catch (NumberFormatException e) {
          e.printStackTrace();
          throw new RuntimeException("Incorrect format in " + samplesFile);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("I/O error in " + samplesFile);
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Error reading sample file: " + samplesFile);
        }
      }

    }

    private HMapIIW readSamplesFromCache(String samplesFile, JobConf conf) throws IOException {
      Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
      HMapIIW samplesMap = null;
      for (Path localFile : localFiles) {
        if (localFile.toString().contains(samplesFile)) {
          samplesMap = new HMapIIW();
          LineReader reader = new LineReader(FileSystem.getLocal(conf).open(localFile));
          Text t = new Text();
          while (reader.readLine(t) != 0) {
            int docno = Integer.parseInt(t.toString());
            sLogger.info(docno + " --> sample");
            samplesMap.put(docno, 1);
          }
          reader.close();
          sLogger.info(samplesMap.size() + " sampled");
        }
      }
      if (samplesMap == null) throw new RuntimeException("Not found in local cache: " + samplesFile);
      return samplesMap;
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
            reporter.incrCounter(Pairs.PrefixSum, prefix);
            reporter.incrCounter(Pairs.Processed, 1);
            hammingDistance = signatures[i].hammingDistance(signatures[j], maxDist);
            sLogger.debug(hammingDistance);
            if (hammingDistance <= maxDist) {
              reporter.incrCounter(Pairs.Emitted, 1);

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
            reporter.incrCounter(Pairs.PrefixSum, prefix);
            reporter.incrCounter(Pairs.Processed, 1);
            hammingDistance = signatures[i].hammingDistance(signatures[j], maxDist);
            sLogger.debug(hammingDistance);
            if (hammingDistance <= maxDist) {
              reporter.incrCounter(Pairs.Emitted, 1);

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
      reporter.incrCounter(Pairs.Emitted, 1);
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
      }
      int cntr = 0;
      while (!list.isEmpty() && cntr < numResults) {
        output.collect(key, list.pollFirst());
        cntr++;
      }
    }
  }

  public int run(String[] args) throws Exception {
    if ( parseArgs(args) < 0 ) {
      printUsage();
      return -1;
    }

    JobConf job = new JobConf(getConf(), CLSlidingWindowPwsim.class);
    FileSystem fs = FileSystem.get(job);
    inputPath = inputPath == null ? PwsimEnvironment.getTablesDir(workDir, fs, signatureType, numOfBits, chunkOverlapSize, numOfPermutations) : inputPath;
    outputPath = outputPath == null ? PwsimEnvironment.getPwsimDir(workDir, signatureType, maxDist, numOfBits, numOfPermutations, windowSize) : outputPath;

    if (fs.exists(new Path(outputPath))) {
      sLogger.info("SlidingWindowPwsim output already exists! Quitting...\nPath: " + outputPath);
      return 0;
    }

    if (sampleDocnosFile != null) {
      DistributedCache.addCacheFile(new URI(sampleDocnosFile), job);
      job.set("Ivory.SampleFile", sampleDocnosFile);
    }

    String collectionName = job.get("Ivory.CollectionName");
    job.setJobName("SlidingWindowPwsim:" + collectionName + workDir.replaceFirst("tables", "")
        + "_B=" + windowSize + "_" + numResults);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    FileOutputFormat.setCompressOutput(job, false);

    job.setJarByClass(CLSlidingWindowPwsim.class);

    job.set("mapreduce.map.java.opts", "-Xmx2000m");
    job.setInt("mapred.task.timeout", 60000000);
    job.setInt("Ivory.SlidingWindowSize", windowSize);
    job.setInt("Ivory.MaxHammingDistance", maxDist);
    job.setNumMapTasks(100);
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
    if (sampleDocnosFile == null) { // if sample file is provided, output should be text.
      job.setOutputFormat(SequenceFileOutputFormat.class);
    } else {
      job.setOutputFormat(TextOutputFormat.class);
    }

    sLogger.info("Running job " + job.getJobName() + "...");
    sLogger.info("Input path: " + workDir);
    sLogger.info("Output path: " + outputPath);
    sLogger.info("Window size: " + windowSize);
    sLogger.info("Threshold: " + maxDist);
    sLogger.info("Sample file?: " + ((sampleDocnosFile != null) ? sampleDocnosFile : "none"));
    sLogger.info("Number of results: " + (numResults == -1 ? "all" : numResults));

    long startTime = System.currentTimeMillis();
    RunningJob j = JobClient.runJob(job);
    System.out.println("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    Counters counters = j.getCounters();
    long processed = (long) counters.findCounter(Pairs.Processed).getCounter();
    long prefixsum = (long) counters.findCounter(Pairs.PrefixSum).getCounter();
    System.out.println("Avg prefix length = " + (prefixsum / (float) processed));

    return 0;
  }

  private static final String WORKDIR_PATH_OPTION = "index";
  private static final String INPUT_PATH_OPTION = "input";
  private static final String OUTPUT_PATH_OPTION = "output";
  private static final String THRESHOLD_OPTION = "T";
  private static final String WINDOWSIZE_OPTION = "B";
  private static final String SIGNLENG_OPTION = "num_bits";
  private static final String NUMPERMS_OPTION = "Q";
  private static final String OVERLAPSIZE_OPTION = "overlap";
  private static final String SIGNTYPE_OPTION = "type";
  private static final String SAMPLEDOCNOS_OPTION = "docnos";
  private static final String NUMREDUCERS_OPTION = "reduce";
  private static final String TOPN_OPTION = "topN";
  private static final String LIBJARS_OPTION = "libjars";
  private Options options;
  private int numOfPermutations, chunkOverlapSize, numReducers, windowSize, maxDist, numResults, numOfBits;
  private String signatureType, sampleDocnosFile, workDir, inputPath, outputPath;

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
    //      System.out.println("usage: [input-path] [output-path] [window-size] [max-distance] ([sample-docnos])");
    //    return -1;
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to index directory").withArgName("path").hasArg().create(WORKDIR_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to input (permuted tables)").withArgName("path").hasArg().create(INPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output").withArgName("path").hasArg().create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("hamming distance threshold for similar pairs").withArgName("threshold").hasArg().isRequired().create(THRESHOLD_OPTION));
    options.addOption(OptionBuilder.withDescription("only keep pairs that match these docnos").withArgName("path to sample docnos file").hasArg().create(SAMPLEDOCNOS_OPTION));
    options.addOption(OptionBuilder.withDescription("number of reducers").withArgName("number").hasArg().create(NUMREDUCERS_OPTION));
    options.addOption(OptionBuilder.withDescription("length of signature").withArgName("number of bits").hasArg().create(SIGNLENG_OPTION));
    options.addOption(OptionBuilder.withDescription("sliding window size").withArgName("window").hasArg().isRequired().create(WINDOWSIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("type of signature").withArgName("random|minhash|simhash").hasArg().create(SIGNTYPE_OPTION));
    options.addOption(OptionBuilder.withDescription("number of permutations (tables)").withArgName("permutations").hasArg().create(NUMPERMS_OPTION));    
    options.addOption(OptionBuilder.withDescription("size of overlap between chunks (default: window size)").withArgName("overlap size").hasArg().create(OVERLAPSIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("keep only N results for each source document").withArgName("N").hasArg().create(TOPN_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    workDir = cmdline.hasOption(WORKDIR_PATH_OPTION) ? cmdline.getOptionValue(WORKDIR_PATH_OPTION) : null;
    inputPath = cmdline.hasOption(INPUT_PATH_OPTION) ? cmdline.getOptionValue(INPUT_PATH_OPTION) : null;
    outputPath = cmdline.hasOption(OUTPUT_PATH_OPTION) ? cmdline.getOptionValue(OUTPUT_PATH_OPTION) : null;
    numOfBits = cmdline.hasOption(SIGNLENG_OPTION) ? Integer.parseInt(cmdline.getOptionValue(SIGNLENG_OPTION)) : -1;
    signatureType = cmdline.hasOption(SIGNTYPE_OPTION) ? cmdline.getOptionValue(SIGNTYPE_OPTION) : null;
    numOfPermutations = cmdline.hasOption(NUMPERMS_OPTION) ? Integer.parseInt(cmdline.getOptionValue(NUMPERMS_OPTION)) : -1;
    chunkOverlapSize = cmdline.hasOption(OVERLAPSIZE_OPTION) ? Integer.parseInt(cmdline.getOptionValue(OVERLAPSIZE_OPTION)) : -1;      

    // either work dir or input+output should be specified
    if (!((workDir != null && numOfBits > 0 && numOfPermutations > 0 && chunkOverlapSize > 0 && signatureType != null) || (inputPath != null && outputPath != null))) {
      System.err.println("Either options -" + WORKDIR_PATH_OPTION + " and -" + SIGNLENG_OPTION + " and -" + SIGNTYPE_OPTION + " and -" + 
          NUMPERMS_OPTION + " and -" + OVERLAPSIZE_OPTION + " or options -" + INPUT_PATH_OPTION + " and -" + OUTPUT_PATH_OPTION + "should be specified!");
      return -1;
    }
    numReducers = cmdline.hasOption(NUMREDUCERS_OPTION) ? Integer.parseInt(cmdline.getOptionValue(NUMREDUCERS_OPTION)) : 100;
    windowSize = Integer.parseInt(cmdline.getOptionValue(WINDOWSIZE_OPTION));
    maxDist = Integer.parseInt(cmdline.getOptionValue(THRESHOLD_OPTION));
    numResults = cmdline.hasOption(TOPN_OPTION) ? Integer.parseInt(cmdline.getOptionValue(TOPN_OPTION)) : -1;
    sampleDocnosFile = cmdline.hasOption(SAMPLEDOCNOS_OPTION) ? cmdline.getOptionValue(SAMPLEDOCNOS_OPTION) : null;

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CLSlidingWindowPwsim(), args);
  }
}
