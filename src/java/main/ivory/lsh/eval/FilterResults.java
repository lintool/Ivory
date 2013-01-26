package ivory.lsh.eval;

import ivory.lsh.driver.PwsimEnvironment;

import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.io.pair.PairOfInts;

@SuppressWarnings("deprecation")
public class FilterResults extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(FilterResults.class);

  static enum mapoutput {
    count
  };

  public FilterResults() {
    super();
  }

  private static HMapIIW readSamplesFromCache(String samplesFile, JobConf conf) throws IOException {
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
  
  private static String getFilename(String s) {
    return s.substring(s.lastIndexOf("/") + 1);
  }

  /**
   * Filter results that are not from sample and/or have distance more than specified in option
   * Ivory.MaxHammingDistance. Reducer selects closest N pairs for each sample foreign-language
   * document.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperTopN extends MapReduceBase implements
  Mapper<PairOfInts, IntWritable, IntWritable, PairOfInts> {

    static Path[] localFiles;
    HMapIIW samplesMap = null;
    int maxDist;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      maxDist = job.getInt("Ivory.MaxHammingDistance", -1);

      // read sample docnos
      String samplesFile = job.get("Ivory.SampleFile"); 
      try {
        samplesMap = readSamplesFromCache(getFilename(samplesFile), job);
      } catch (NumberFormatException e) {
        e.printStackTrace();
        throw new RuntimeException("Incorrect format in " + samplesFile);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("I/O error in " + samplesFile);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error reading sample file " + samplesFile);
      }
    }

    public void map(PairOfInts key, IntWritable value,
        OutputCollector<IntWritable, PairOfInts> output, Reporter reporter) throws IOException {

      int leftKey = key.getLeftElement(); // english docno
      int rightKey = key.getRightElement(); // german docno

      sLogger.debug(rightKey);
      if (samplesMap == null || samplesMap.containsKey(rightKey)) {
        if (maxDist == -1 || value.get() <= maxDist) {
          output.collect(new IntWritable(rightKey), new PairOfInts(value.get(), leftKey));

          // symmetric implementation. change when not desired.
          // output.collect(new IntWritable(leftKey), new PairOfInts(value.get(),rightKey));
        }
      }

    }
  }

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

  public static class MyMapper extends MapReduceBase implements
  Mapper<PairOfInts, IntWritable, PairOfInts, IntWritable> {

    static Path[] localFiles;
    HMapIIW samplesMap = null;
    int maxDist;
    IntWritable outValue = new IntWritable();
    PairOfInts outKey = new PairOfInts();

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      maxDist = job.getInt("Ivory.MaxHammingDistance", -1);

      // read sample docnos
      String samplesFile = job.get("Ivory.SampleFile"); 
      try {
        samplesMap = readSamplesFromCache(getFilename(samplesFile), job);
      } catch (NumberFormatException e) {
        e.printStackTrace();
        throw new RuntimeException("Incorrect format in " + samplesFile);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("I/O error in " + samplesFile);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error reading sample file " + samplesFile);
      }
    }

    public void map(PairOfInts key, IntWritable value,
        OutputCollector<PairOfInts, IntWritable> output, Reporter reporter) throws IOException {

      int leftKey = key.getLeftElement(); // english docno
      int rightKey = key.getRightElement(); // german docno

      if (samplesMap == null || samplesMap.containsKey(rightKey)) {
        if (maxDist == -1 || value.get() <= maxDist) {
          outKey.set(leftKey, rightKey);
          outValue.set(value.get());
          output.collect(outKey, outValue);
        }
      }
    }
  }

  // @author: ferhanture
  // I wrote this to be used on a text dataset. needs some fixing.
  // public static class MyReducerAltOutput extends MapReduceBase implements
  // Reducer<IntWritable, PairOfInts, Text, Text> {
  // int numResults;
  // TreeSet<PairOfInts> list = new TreeSet<PairOfInts>();
  // private DocnoMapping mDocMapping;
  //
  // public void configure(JobConf conf){
  // numResults = conf.getInt("Ivory.NumResults", -1);
  // sLogger.info("numResults");
  // mDocMapping = new TextDocnoMapping();
  // try {
  // mDocMapping.loadMapping(new Path("/user/fture/doug/docno-mapping.dat"), FileSystem.get(conf));
  // } catch (IOException e) {
  // e.printStackTrace();
  // }
  // }
  //
  // public void reduce(IntWritable key, Iterator<PairOfInts> values,
  // OutputCollector<Text, Text> output, Reporter reporter)
  // throws IOException {
  // list.clear();
  // while(values.hasNext()){
  // PairOfInts p = values.next();
  // list.add(new PairOfInts(p.getLeftElement(),p.getRightElement()));
  // reporter.incrCounter(mapoutput.count, 1);
  // }
  // int cntr = 0;
  // while(!list.isEmpty() && cntr<numResults){
  // PairOfInts nextClosest = list.pollFirst();
  // String keyDocid = mDocMapping.getDocid(key.get());
  // String valueDocid = mDocMapping.getDocid(nextClosest.getRightElement());
  // int dist= nextClosest.getLeftElement();
  // output.collect(new Text(keyDocid), new Text(valueDocid+"\t"+dist));
  // cntr++;
  // }
  // }
  //
  // }

  public int run(String[] args) throws Exception {
    if ( parseArgs(args) < 0 ) {
      printUsage();
      return -1;
    }
    JobConf job = new JobConf(getConf(), FilterResults.class);

    job.setInt("Ivory.MaxHammingDistance", maxDist);
    job.setInt("Ivory.NumResults", numResults);

    job.setJobName("FilterResults_sample=" + getFilename(sampleDocnosFile) + "_top=" + (numResults > 0 ? numResults : "all"));
    FileSystem fs = FileSystem.get(job);

    inputPath = (inputPath == null) ? PwsimEnvironment.getPwsimDir(workDir, signatureType, maxDist, numOfBits, numOfPermutations, windowSize) : inputPath;
    outputPath = (outputPath == null) ? PwsimEnvironment.getFilteredPwsimDir(workDir, signatureType, maxDist, numOfBits, numOfPermutations, windowSize, sampleDocnosFile, numResults) : outputPath;

    int numMappers = 300;
    int numReducers = 1;

    if (fs.exists(new Path(outputPath))) {
      sLogger.info("FilteredPwsim output already exists! Quitting...");
      return 0;
    }

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("mapred.task.timeout", 6000000);

    job.set("Ivory.SampleFile", sampleDocnosFile);
    DistributedCache.addCacheFile(new URI(sampleDocnosFile), job); // sample docnos in file

    sLogger.info("Running job " + job.getJobName());
    sLogger.info("Input directory: " + inputPath);
    sLogger.info("Output directory: " + outputPath);
    sLogger.info("Samples file: " + sampleDocnosFile);

    if (numResults > 0) {
      sLogger.info("Number of results = " + numResults);
      job.setMapperClass(MyMapperTopN.class);
      job.setReducerClass(MyReducerTopN.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(PairOfInts.class);
    } else {
      sLogger.info("Number of results = all");
      job.setMapperClass(MyMapper.class);
      job.setReducerClass(IdentityReducer.class);
      job.setMapOutputKeyClass(PairOfInts.class);
      job.setMapOutputValueClass(IntWritable.class);
    }

    job.setJarByClass(FilterResults.class);
    job.setNumMapTasks(numMappers);
    job.setNumReduceTasks(numReducers);
    job.setInputFormat(SequenceFileInputFormat.class);

    JobClient.runJob(job);

    return 0;
  }

  private static final String WORKDIR_PATH_OPTION = "index";
  private static final String INPUT_PATH_OPTION = "input";
  private static final String OUTPUT_PATH_OPTION = "output"; 
  private static final String THRESHOLD_OPTION = "T";
  private static final String SAMPLEDOCNOS_OPTION = "docnos";
  private static final String WINDOWSIZE_OPTION = "B";
  private static final String SIGNLENG_OPTION = "num_bits";
  private static final String NUMPERMS_OPTION = "Q";
  private static final String OVERLAPSIZE_OPTION = "overlap";
  private static final String SIGNTYPE_OPTION = "type";
  private static final String TOPN_OPTION = "topN";
  private static final String LIBJARS_OPTION = "libjars";

  private Options options;
  private int numOfPermutations, windowSize, maxDist, numResults, numOfBits;
  private String signatureType, sampleDocnosFile, workDir, inputPath, outputPath;

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to index directory").withArgName("path").hasArg().create(WORKDIR_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to source-language index directory").withArgName("path").hasArg().create(INPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to target-language index directory").withArgName("path").hasArg().create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("only keep pairs that match these docnos").withArgName("path to sample docnos file").hasArg().isRequired().create(SAMPLEDOCNOS_OPTION));
    options.addOption(OptionBuilder.withDescription("hamming distance threshold for similar pairs").withArgName("threshold").hasArg().create(THRESHOLD_OPTION));
    options.addOption(OptionBuilder.withDescription("keep only N results for each source document").withArgName("N").hasArg().create(TOPN_OPTION));
    options.addOption(OptionBuilder.withDescription("length of signature").withArgName("number of bits").hasArg().create(SIGNLENG_OPTION));
    options.addOption(OptionBuilder.withDescription("sliding window size").withArgName("window").hasArg().create(WINDOWSIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("type of signature").withArgName("random|minhash|simhash").hasArg().create(SIGNTYPE_OPTION));
    options.addOption(OptionBuilder.withDescription("number of permutations (tables)").withArgName("permutations").hasArg().create(NUMPERMS_OPTION));    
    options.addOption(OptionBuilder.withDescription("size of overlap between chunks (default: window size)").withArgName("overlap size").hasArg().create(OVERLAPSIZE_OPTION));
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
    maxDist = cmdline.hasOption(THRESHOLD_OPTION) ? Integer.parseInt(cmdline.getOptionValue(THRESHOLD_OPTION)) : -1;
    windowSize = cmdline.hasOption(WINDOWSIZE_OPTION) ? Integer.parseInt(cmdline.getOptionValue(WINDOWSIZE_OPTION)) : -1;
    
    // either provide --input and --output, or enter all parameters so the program will determine these paths
    if (!((workDir != null && numOfBits > 0 && numOfPermutations > 0 && windowSize > 0 && signatureType != null && maxDist > 0) 
        || (inputPath != null && outputPath != null))) {
      System.err.println("Either options -" + WORKDIR_PATH_OPTION + " and -" + SIGNLENG_OPTION + " and -" + SIGNTYPE_OPTION + " and -" + 
          NUMPERMS_OPTION + " and -" + OVERLAPSIZE_OPTION + " or options -" + INPUT_PATH_OPTION + " and -" + OUTPUT_PATH_OPTION + "should be specified!");
      return -1;
    }
    try {
      PwsimEnvironment.getPwsimDir(workDir, signatureType, maxDist, numOfBits, numOfPermutations, windowSize);
    } catch (IOException e) {
      System.err.println("Error with path names: " + e.getMessage());
      return -1;
    }
    sampleDocnosFile = cmdline.getOptionValue(SAMPLEDOCNOS_OPTION);
    numResults = cmdline.hasOption(TOPN_OPTION) ? Integer.parseInt(cmdline.getOptionValue(TOPN_OPTION)) : -1;

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FilterResults(), args);
  }
}
