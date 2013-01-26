package ivory.lsh.eval;

import ivory.core.data.document.WeightedIntDocVector;
import ivory.core.util.CLIRUtils;
import ivory.lsh.data.Signature;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;

/**
 * A class to extract the similarity list of each sample document, either by performing dot product
 * between the doc vectors or finding hamming distance between signatures.
 * 
 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class BruteForcePwsim extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(BruteForcePwsim.class);

  static enum Pairs {
    Total, Emitted, DEBUG, DEBUG2, Total2
  };

  static enum Sample {
    Size
  };

  /**
   * For every document in the sample, find all other docs that have cosine similarity higher than
   * some given threshold.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperDocVectors extends MapReduceBase implements
  Mapper<IntWritable, WeightedIntDocVector, IntWritable, PairOfFloatInt> {

    @SuppressWarnings("unchecked")
    static List<PairOfWritables<WritableComparable, Writable>> vectors;
    float threshold;

    private String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
    }

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      threshold = job.getFloat("Ivory.CosineThreshold", -1);
      sLogger.info("Threshold = " + threshold);

      String sampleFile = job.get("Ivory.SampleFile");


      // read doc ids of sample into vectors
      try {
        sampleFile = getFilename(sampleFile);
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        for (Path localFile : localFiles) {
          if (localFile.toString().contains(sampleFile)) {
            vectors = SequenceFileUtils.readFile(localFile, FileSystem.getLocal(job));
          }
        }
        if (vectors == null) throw new RuntimeException("Sample file not found at " + sampleFile);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error reading doc vectors from " + sampleFile);
      }
      sLogger.info("Read " + vectors.size() + " sample doc vectors");
    }

    public void map(IntWritable docno, WeightedIntDocVector docvector,
        OutputCollector<IntWritable, PairOfFloatInt> output, Reporter reporter) throws IOException {
      for (int i = 0; i < vectors.size(); i++) {
        IntWritable sampleDocno = (IntWritable) vectors.get(i).getLeftElement();

        WeightedIntDocVector fromSample = (WeightedIntDocVector) vectors.get(i).getRightElement();
        float cs = CLIRUtils.cosine(docvector.getWeightedTerms(), fromSample.getWeightedTerms());

        if (cs >= threshold) {
          output.collect(new IntWritable(sampleDocno.get()), new PairOfFloatInt(cs, docno.get()));
        }
      }
    }
  }

  /**
   * For every document in the sample, find all other docs that have cosine similarity higher than
   * some given threshold.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperTermDocVectors extends MapReduceBase implements
  Mapper<IntWritable, HMapSFW, IntWritable, PairOfFloatInt> {

    @SuppressWarnings("unchecked")
    static List<PairOfWritables<WritableComparable, Writable>> vectors;
    float threshold;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      threshold = job.getFloat("Ivory.CosineThreshold", -1);
      sLogger.info("Threshold = " + threshold);

      String sampleFile = job.get("Ivory.SampleFile");
      // read doc ids of sample into vectors
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        for (Path localFile : localFiles) {
          if (localFile.toString().contains(sampleFile)) {
            vectors = SequenceFileUtils.readFile(localFile, FileSystem.getLocal(job));
          }
        }
        if (vectors == null) throw new RuntimeException("Sample file not found at " + sampleFile);
      } catch (Exception e) {
        throw new RuntimeException("Error reading doc vectors from " + sampleFile);
      }
      sLogger.info("Read " + vectors.size() + " sample doc vectors");
    }

    public void map(IntWritable docno, HMapSFW docvector,
        OutputCollector<IntWritable, PairOfFloatInt> output, Reporter reporter) throws IOException {
      for (int i = 0; i < vectors.size(); i++) {
        reporter.incrCounter(Pairs.Total, 1);
        IntWritable sampleDocno = (IntWritable) vectors.get(i).getLeftElement();
        HMapSFW fromSample = (HMapSFW) vectors.get(i).getRightElement();

        float cs = CLIRUtils.cosine(docvector, fromSample);       
        if (cs >= threshold) {
          sLogger.debug(sampleDocno + "," + fromSample + "\n" + fromSample.length());
          sLogger.debug(docno + "," + docvector + "\n" + docvector.length());
          sLogger.debug(cs);
          reporter.incrCounter(Pairs.Emitted, 1);
          output.collect(new IntWritable(sampleDocno.get()), new PairOfFloatInt(cs, docno.get()));
        }
      }
    }
  }

  /**
   * For every document in the sample, find all other docs that are closer than some given hamming
   * distance.
   * 
   * @author ferhanture
   * 
   */
  public static class MyMapperSignature extends MapReduceBase implements
  Mapper<IntWritable, Signature, IntWritable, PairOfFloatInt> {

    @SuppressWarnings("unchecked")
    static List<PairOfWritables<WritableComparable, Writable>> signatures;
    int maxDist;

    public void configure(JobConf job) {
      sLogger.setLevel(Level.INFO);
      maxDist = (int) job.getFloat("Ivory.MaxHammingDistance", -1);
      sLogger.info("Threshold = " + maxDist);

      String sampleFile = job.get("Ivory.SampleFile");
      // read doc ids of sample into vectors
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        for (Path localFile : localFiles) {
          if (localFile.toString().contains(sampleFile)) {
            signatures = SequenceFileUtils.readFile(localFile, FileSystem.getLocal(job));
          }
        }
        if (signatures == null) throw new RuntimeException("Sample file not found at " + sampleFile);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error reading sample signatures!");
      }
      sLogger.info(signatures.size());
    }

    public void map(IntWritable docno, Signature signature,
        OutputCollector<IntWritable, PairOfFloatInt> output, Reporter reporter) throws IOException {
      for (int i = 0; i < signatures.size(); i++) {
        reporter.incrCounter(Pairs.Total, 1);
        IntWritable sampleDocno = (IntWritable) signatures.get(i).getLeftElement();
        Signature fromSample = (Signature) signatures.get(i).getRightElement();
        int dist = signature.hammingDistance(fromSample, maxDist);
        // if((sampleDocno.get()==1000009022 && docno.get()==189034) ||
        // (sampleDocno.get()==1000170828 && docno.get()==2898431)){
        // reporter.incrCounter(Pairs.DEBUG, 1);
        // sLogger.info(sampleDocno.get()+","+docno.get()+"="+dist);
        // sLogger.info(fromSample);
        // sLogger.info(signature);
        // }else{
        // continue;
        // }

        if (dist <= maxDist) {
          output
          .collect(new IntWritable(sampleDocno.get()), new PairOfFloatInt(-dist, docno.get()));
          reporter.incrCounter(Pairs.Emitted, 1);
        }
      }
    }
  }

  /**
   * This reducer reduces the number of pairs per sample document to a given number
   * (Ivory.NumResults).
   * 
   * @author ferhanture
   * 
   */
  public static class MyReducer extends MapReduceBase implements
  Reducer<IntWritable, PairOfFloatInt, PairOfInts, Text> {
    int numResults;
    TreeSet<PairOfFloatInt> list = new TreeSet<PairOfFloatInt>();
    PairOfInts keyOut = new PairOfInts();
    Text valOut = new Text();
    NumberFormat nf;

    public void configure(JobConf conf) {
      sLogger.setLevel(Level.INFO);
      numResults = conf.getInt("Ivory.NumResults", Integer.MAX_VALUE);
      nf = NumberFormat.getInstance();
      nf.setMaximumFractionDigits(3);
      nf.setMinimumFractionDigits(3);
    }

    public void reduce(IntWritable key, Iterator<PairOfFloatInt> values,
        OutputCollector<PairOfInts, Text> output, Reporter reporter) throws IOException {
      list.clear();
      while (values.hasNext()) {
        PairOfFloatInt p = values.next();
        if (!list.add(new PairOfFloatInt(p.getLeftElement(), p.getRightElement()))) {
          sLogger.debug("Not added: " + p);
        } else {
          sLogger.debug("Added: " + p);
        }
        reporter.incrCounter(Pairs.Total, 1);
      }
      sLogger.debug(list.size());
      int cntr = 0;
      while (!list.isEmpty() && cntr < numResults) {
        PairOfFloatInt pair = list.pollLast();
        sLogger.debug("output " + cntr + "=" + pair);

        keyOut.set(pair.getRightElement(), key.get()); // first english docno, then foreign language
        // docno
        valOut.set(nf.format(pair.getLeftElement()));
        output.collect(keyOut, valOut);
        cntr++;
      }
    }

  }

  public int run(String[] args) throws Exception {
    if ( parseArgs(args) < 0 ) {
      return printUsage();
    }

    JobConf job = new JobConf(getConf(), BruteForcePwsim.class);

    FileSystem fs = FileSystem.get(job);

    fs.delete(new Path(outputPath), true);

    int numMappers = 100;
    int numReducers = 1;

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 10);
    job.setInt("mapred.reduce.max.attempts", 10);
    job.setInt("mapred.task.timeout", 6000000);

    job.setNumMapTasks(numMappers);
    job.setNumReduceTasks(numReducers);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfFloatInt.class);
    job.setOutputKeyClass(PairOfInts.class);
    job.setOutputValueClass(FloatWritable.class);

    job.set("Ivory.SampleFile", sampleFile);
    DistributedCache.addCacheFile(new URI(sampleFile), job);

    if (inputType.contains("signature")) {
      job.setMapperClass(MyMapperSignature.class);
      job.setFloat("Ivory.MaxHammingDistance", threshold);
    } else if (inputType.contains("vector")) {
      if (inputType.contains("term")) {
        job.setMapperClass(MyMapperTermDocVectors.class);
      } else {
        job.setMapperClass(MyMapperDocVectors.class);
      }
      job.setFloat("Ivory.CosineThreshold", threshold);
    }
    job.setJobName("BruteForcePwsim_type=" + inputType + "_cosine=" + threshold + "_top=" + (numResults > 0 ? numResults : "all"));

    if (numResults > 0) {
      job.setInt("Ivory.NumResults", numResults);
    }
    job.setReducerClass(MyReducer.class);

    sLogger.info("Running job " + job.getJobName());

    JobClient.runJob(job);

    return 0;
  }


  private static final String INPUT_PATH_OPTION = "input";
  private static final String OUTPUT_PATH_OPTION = "output"; 
  private static final String INPTYPE_OPTION = "type";
  private static final String THRESHOLD_OPTION = "cosineT";
  private static final String SAMPLE_OPTION = "sample";
  private static final String TOPN_OPTION = "topN";
  private static final String LIBJARS_OPTION = "libjars";
  private Options options;
  private float threshold;
  private int numResults;
  private String sampleFile, inputPath, outputPath, inputType;


  private int printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
    return -1;
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to input doc vectors or signatures").withArgName("path").hasArg().isRequired().create(INPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output directory").withArgName("path").hasArg().isRequired().create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("cosine similarity threshold when type=*docvector, hamming distance threshold when type=signature").withArgName("threshold").hasArg().isRequired().create(THRESHOLD_OPTION));
    options.addOption(OptionBuilder.withDescription("path to file with sample doc vectors or signatures").withArgName("path").hasArg().isRequired().create(SAMPLE_OPTION));
    options.addOption(OptionBuilder.withDescription("type of input").withArgName("signature|intdocvector|termdocvector").hasArg().isRequired().create(INPTYPE_OPTION));
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

    inputPath = cmdline.getOptionValue(INPUT_PATH_OPTION);
    outputPath = cmdline.getOptionValue(OUTPUT_PATH_OPTION);
    threshold = Float.parseFloat(cmdline.getOptionValue(THRESHOLD_OPTION));
    sampleFile = cmdline.getOptionValue(SAMPLE_OPTION);
    inputType = cmdline.getOptionValue(INPTYPE_OPTION);
    numResults = cmdline.hasOption(TOPN_OPTION) ? Integer.parseInt(cmdline.getOptionValue(TOPN_OPTION)) : -1;

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BruteForcePwsim(), args);
    return;
  }

}
