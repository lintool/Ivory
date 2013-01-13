package ivory.lsh.pwsim;

import ivory.core.RetrievalEnvironment;
import ivory.lsh.data.BitsSignatureTable;
import ivory.lsh.data.PairOfIntSignature;
import ivory.lsh.data.Permutation;
import ivory.lsh.data.PermutationByBit;
import ivory.lsh.data.Signature;
import ivory.lsh.driver.PwsimEnvironment;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

@SuppressWarnings("deprecation")
public class GenerateChunkedPermutedTables extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(GenerateChunkedPermutedTables.class);

  static {
    sLogger.setLevel(Level.WARN);
  }

  static enum Count {
    Signatures, Chunks, SignaturesInChunks
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
  }

  /**
   * @author ferhanture
   * 
   *         Maps each signature to Q random permutations, usign the permuters stored in local cache
   *         file <docno,signature> --> <(permno,signature),docno>
   */

  @SuppressWarnings("unchecked")
  public static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, Signature, PairOfIntSignature, IntWritable> {

    static Path[] localFiles;
    static List<Writable> randomPermutations;
    static int numOfPermutations, numOfBits;
    static Signature permutedSignature;
    static Constructor pairConstructor;
    static PairOfIntSignature pair;

    private String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }

    public void configure(JobConf job) {
      numOfPermutations = job.getInt("Ivory.NumOfPermutations", -1);
      numOfBits = job.getInt("Ivory.NumOfBits", -1);
      Class signatureClass = null;

      try {
        Class pairClass = Class.forName(job.get("Ivory.PairClass"));
        pairConstructor = pairClass.getConstructor(int.class, Signature.class);

        signatureClass = Class.forName(job.get("Ivory.SignatureClass"));
        Constructor intConstructor = signatureClass.getConstructor(int.class);
        permutedSignature = (Signature) intConstructor.newInstance(numOfBits);
        pair = (PairOfIntSignature) pairConstructor.newInstance(0, permutedSignature);
      } catch (Exception e) {
        throw new RuntimeException("config exception: \n" + e.toString());
      }

      sLogger.debug("Reading permutations file....");
      String randomPermsFile = job.get("Ivory.RandomPermsFile");
      try {
        randomPermsFile = getFilename(randomPermsFile);
        localFiles = DistributedCache.getLocalCacheFiles(job);
        for (Path localFile : localFiles) {
          if (localFile.toString().contains(randomPermsFile)) {
            randomPermutations = SequenceFileUtils.readValues(localFile, FileSystem.getLocal(job));
          }
        }
        if (randomPermutations == null) throw new RuntimeException("Not found in local cache: " + randomPermsFile);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error reading random permutations " + randomPermsFile);
      }
      sLogger.debug("Done reading file.");

    }

    public void map(IntWritable docno, Signature signature,
        OutputCollector<PairOfIntSignature, IntWritable> output, Reporter reporter)
    throws IOException {
      // Map each signature to Q random permutations
      for (int i = 0; i < numOfPermutations; i++) {
        signature.perm((ArrayListOfIntsWritable) randomPermutations.get(i), permutedSignature);
        pair.setInt(i);
        pair.setSignature(permutedSignature);
        output.collect(pair, docno);
      }
      reporter.incrCounter(Count.Signatures, 1);
    }
  }

  public static class MyPartitioner implements Partitioner<PairOfIntSignature, IntWritable> {
    // partition with permutation number only
    public int getPartition(PairOfIntSignature key, IntWritable value, int numReducers) {
      return key.getInt() % numReducers;
    }

    public void configure(JobConf conf) {
    }
  }

  public static class MyReducer extends MapReduceBase implements
  Reducer<PairOfIntSignature, IntWritable, IntWritable, BitsSignatureTable> {
    static int permNo = -1;
    Signature[] signatures = null;
    int[] docNos = null;
    int curTableSize = 0;
    int overlapSize = -1;
    int chunckSize = -1;

    public void configure(JobConf conf) {
      // sLogger.setLevel(Level.DEBUG);
      overlapSize = conf.getInt("Ivory.OverlapSize", -1);
      chunckSize = conf.getInt("Ivory.ChunckSize", -1);
      if (overlapSize >= chunckSize)
        throw new RuntimeException("Invalid Ivory.OverlapSize(" + 
            overlapSize + ") or Ivory.ChunkSize(" + chunckSize + ")");
      signatures = new Signature[chunckSize];
      docNos = new int[chunckSize];
    }

    BitsSignatureTable table = new BitsSignatureTable();
    IntWritable outKey = new IntWritable();
    OutputCollector<IntWritable, BitsSignatureTable> mOutput = null;
    PairOfIntSignature lastKey = null;
    Reporter mReporter = null;

    public void reduce(PairOfIntSignature key, Iterator<IntWritable> val,
        OutputCollector<IntWritable, BitsSignatureTable> output, Reporter reporter)
    throws IOException {
      mReporter = reporter;
      mOutput = output;
      lastKey = key;
      while (val.hasNext()) {
        docNos[curTableSize] = val.next().get();
        signatures[curTableSize] = key.getSignature();
        curTableSize++;
        if (curTableSize == chunckSize) {
          table.set(signatures, docNos, curTableSize);
          outKey.set(key.getInt());
          output.collect(outKey, table);
          reporter.incrCounter(Count.SignaturesInChunks, table.getNumOfSignatures());
          reporter.incrCounter(Count.Chunks, 1);
          shiftOverlap();
        }
      }
    }

    private void shiftOverlap() {
      if (overlapSize >= curTableSize)
        return;

      // overlapSize < curTableSize ==> shift up
      int j = 0;
      for (int i = curTableSize - overlapSize; i < curTableSize; i++) {
        signatures[j] = signatures[i];
        docNos[j] = docNos[i];
        j++;
      }
      curTableSize = j;
    }

    @Override
    public void close() throws IOException {
      if (curTableSize == 0 || mOutput == null)
        return;
      table.set(signatures, docNos, curTableSize);
      outKey.set(lastKey.getInt());
      mOutput.collect(outKey, table);
      mReporter.incrCounter(Count.SignaturesInChunks, table.getNumOfSignatures());
      mReporter.incrCounter(Count.Chunks, 1);

      // sLogger.debug(nSignaturesRead);
    }

  }

  // create Q permutation functions and write them to file
  public static String createPermutations(FileSystem fs, JobConf job, String rootPath, int numBits,
      int numOfPermutations) throws Exception {

    String randomPermFile = PwsimEnvironment.getPermutationsFile(rootPath, fs, numBits, numOfPermutations);
    if (fs.exists(new Path(randomPermFile))) {
      sLogger.info("Random permutations output path already exists!");
      return randomPermFile;
    }

    SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, new Path(randomPermFile),
        IntWritable.class, ArrayListOfIntsWritable.class);
    Permutation p = new PermutationByBit(numBits);
    for (int i = 0; i < numOfPermutations; i++) {
      ArrayListOfIntsWritable perm = p.nextPermutation();
      writer.append(new IntWritable(i), perm);
      sLogger.debug(i + ":" + perm);
    }
    writer.close();
    sLogger.info("Random permutations written.");
    return randomPermFile;
  }

  public int run(String[] args) throws Exception {
    sLogger.setLevel(Level.INFO);
    JobConf job = new JobConf(getConf(), GenerateChunkedPermutedTables.class);
    if ( parseArgs(args, job) < 0 ) {
      printUsage();
      System.exit(-1);
    }

    FileSystem fs = FileSystem.get(job);

    job.setJobName(this.getClass().getCanonicalName() + "_" + numOfPermutations + "_" + signatureType + "_" + numOfBits);
    PwsimEnvironment.setClassTypes(signatureType, job);

    if (fs.exists(new Path(outputPath))) {
      sLogger.info("Permuted tables already exist! Quitting...");
      return 0;
    }

    // create Q permutation functions and write them to file
    String randomPermFile = createPermutations(fs, job, workDir, numOfBits, numOfPermutations);
    job.set("Ivory.RandomPermsFile", randomPermFile);
    DistributedCache.addCacheFile(new URI(randomPermFile), job);

    FileInputFormat.addInputPath(job, new Path(trgInputPath));
    if (srcInputPath != null) {
      FileInputFormat.addInputPath(job, new Path(srcInputPath));
    }

    Path[] paths = FileInputFormat.getInputPaths(job);
    for (Path path : paths) {
      sLogger.info("Added input: " + path);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInt("Ivory.NumOfPermutations", numOfPermutations);
    job.setInt("Ivory.NumOfBits", numOfBits);
    job.setInt("Ivory.OverlapSize", chunkOverlapSize);
    job.setInt("Ivory.ChunckSize", chunkSize);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.task.timeout", 600000000);
    job.setJarByClass(GenerateChunkedPermutedTables.class);
    job.setNumMapTasks(100);
    job.setNumReduceTasks(numOfPermutations);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(Class.forName(job.get("Ivory.PairClass")));
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BitsSignatureTable.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    sLogger.info("Running job " + job.getJobName() + "...");
    sLogger.info("Output path: " + outputPath);
    sLogger.info("Number of bits/signature(D): " + numOfBits);
    sLogger.info("Number of permutations(Q): " + numOfPermutations);
    sLogger.info("Overlap size: " + chunkOverlapSize);
    sLogger.info("Chunk size: " + chunkSize);

    long startTime = System.currentTimeMillis();
    JobClient.runJob(job);
    System.out.println("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    return 0;
  }

  private Options options;
  private int numOfPermutations, numOfBits, chunkOverlapSize, slidingWindowSize, chunkSize;
  private String signatureType, trgInputPath, srcInputPath, outputPath, workDir, srcWorkDir;

  private static final String SOURCE_INPUT_OPTION = "sourceindex";
  private static final String INPUT_OPTION = "index";
  private static final String SIGNLENG_OPTION = "num_bits";
  private static final String SIGNTYPE_OPTION = "type";
  private static final String NUMPERMS_OPTION = "Q";
  private static final String OVERLAPSIZE_OPTION = "overlap";
  private static final String LIBJARS_OPTION = "libjars";
  private static final String WINDOWSIZE_OPTION = "B";

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args, JobConf conf) throws Exception {
    FileSystem fs = FileSystem.get(conf);

    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to source-language index directory").withArgName("path").hasArg().create(SOURCE_INPUT_OPTION));
    options.addOption(OptionBuilder.withDescription("path to (target-language) index directory").withArgName("path").hasArg().isRequired().create(INPUT_OPTION));
    options.addOption(OptionBuilder.withDescription("length of signature").withArgName("number of bits").hasArg().isRequired().create(SIGNLENG_OPTION));
    options.addOption(OptionBuilder.withDescription("type of signature").withArgName("random|minhash|simhash").hasArg().isRequired().create(SIGNTYPE_OPTION));
    options.addOption(OptionBuilder.withDescription("sliding window size").withArgName("window").hasArg().create(WINDOWSIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("number of permutations (tables)").withArgName("permutations").hasArg().isRequired().create(NUMPERMS_OPTION));    
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

    srcWorkDir = cmdline.hasOption(SOURCE_INPUT_OPTION) ? cmdline.getOptionValue(SOURCE_INPUT_OPTION) : null;
    workDir = cmdline.getOptionValue(INPUT_OPTION);
    numOfBits = Integer.parseInt(cmdline.getOptionValue(SIGNLENG_OPTION));
    signatureType = cmdline.getOptionValue(SIGNTYPE_OPTION);
    numOfPermutations = Integer.parseInt(cmdline.getOptionValue(NUMPERMS_OPTION));
    slidingWindowSize = cmdline.hasOption(WINDOWSIZE_OPTION) ? Integer.parseInt(cmdline.getOptionValue(WINDOWSIZE_OPTION)) : 0;
    srcInputPath = PwsimEnvironment.getSignaturesDir(srcWorkDir, numOfBits, signatureType);
    trgInputPath = PwsimEnvironment.getSignaturesDir(workDir, numOfBits, signatureType);
    if (cmdline.hasOption(OVERLAPSIZE_OPTION)){
      chunkOverlapSize = Integer.parseInt(cmdline.getOptionValue(OVERLAPSIZE_OPTION));      
    }else {
      if (slidingWindowSize == 0) {
        throw new RuntimeException("Either provide option --" + WINDOWSIZE_OPTION + " or --" + OVERLAPSIZE_OPTION);
      }else {
        chunkOverlapSize = slidingWindowSize;
      }
    }
    outputPath = PwsimEnvironment.getTablesDir(workDir, fs, signatureType, numOfBits, chunkOverlapSize, numOfPermutations);

    RetrievalEnvironment targetEnv = new RetrievalEnvironment(workDir, fs);
    RetrievalEnvironment srcEnv = new RetrievalEnvironment(srcWorkDir, fs);
    int collSize = targetEnv.readCollectionDocumentCount() + srcEnv.readCollectionDocumentCount();
    chunkSize = collSize / 10;
    if (chunkSize < 100000) { 
      chunkSize = 100000;
    } else if (chunkSize > 2000000) {
      chunkSize = 2000000;
    }

    if (numOfPermutations < 0 || numOfBits < 0 || chunkOverlapSize < slidingWindowSize) {
      return -1;
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GenerateChunkedPermutedTables(), args);
    return;
  }
}
