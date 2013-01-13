package ivory.lsh.eval;

import ivory.core.RetrievalEnvironment;
import ivory.core.data.document.WeightedIntDocVector;
import ivory.lsh.driver.PwsimEnvironment;
import ivory.lsh.eval.SampleSignatures.mapoutput;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.Map.Entry;

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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.io.map.HMapSFW;
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
 * /umd-lin/fture/pwsim/medline/wt-term-doc-vectors 
 * /umd-lin/fture/pwsim/medline/wt-term-doc-vectors-sample 
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
public class SampleTermDocVectors extends Configured implements Tool {
  @SuppressWarnings("unchecked")
  static Class keyClass = IntWritable.class, valueClass = HMapSFW.class,
  inputFormat = SequenceFileInputFormat.class;

  private static final Logger sLogger = Logger.getLogger(SampleTermDocVectors.class);

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
  }


  private static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, HMapSFW, IntWritable, HMapSFW> {
    private int sampleFreq;
    private HMapII samplesMap = null;

    private String getFilename(String s) {
      return s.substring(s.lastIndexOf("/") + 1);
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

    public void configure(JobConf conf) {
      sLogger.setLevel(Level.INFO);

      sampleFreq = conf.getInt("SampleFrequency", -1);

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
          throw new RuntimeException("Error reading sample file!");
        }
      }
    }

    public void map(IntWritable key, HMapSFW val,
        OutputCollector<IntWritable, HMapSFW> output, Reporter reporter)
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
  Reducer<IntWritable, HMapSFW, IntWritable, HMapSFW> {

    @Override
    public void reduce(IntWritable key, Iterator<HMapSFW> values,
        OutputCollector<IntWritable, HMapSFW> output, Reporter reporter)
    throws IOException {
      output.collect(key, values.next());
    }
  }

  @SuppressWarnings("unchecked")
  public int run(String[] args) throws Exception {
    sLogger.setLevel(Level.INFO);

    if ( parseArgs(args) < 0 ) {
      printUsage();
      System.exit(-1);
    }

    JobConf job = new JobConf(getConf(), SampleTermDocVectors.class);
    FileSystem fs = FileSystem.get(job);

    inputPath = (inputPath == null) ? PwsimEnvironment.getTermDocvectorsFile(workDir, fs) : inputPath;
    outputPath = (outputPath == null) ? PwsimEnvironment.getTermDocvectorsFile(workDir, fs, sampleSize) : outputPath;
    
    if (!fs.exists(new Path(inputPath))) {
      throw new RuntimeException("Error, input path does not exist!");
    }

    job.setJobName(getClass().getName());

    // if sample docnos path provided and frequency not provided
    if (sampleDocnosFile != null && fs.exists(new Path(sampleDocnosFile))) {
      job.set("Ivory.SampleFile", sampleDocnosFile);
      DistributedCache.addCacheFile(new URI(sampleDocnosFile), job);
    } else if (sampleSize != -1) {
      RetrievalEnvironment env = new RetrievalEnvironment(workDir, fs);
      int collectionSize = env.readCollectionDocumentCount();
      sampleFreq = collectionSize / (float) sampleSize; 
      job.setInt("SampleFrequency", (int) sampleFreq);
    } else {
      throw new RuntimeException("Either provide sample frequency with " +
          "option -" + SAMPLESIZE_OPTION+ " or existing sample docnos with option -" + SAMPLEDOCNOS_OPTION);
    }

    int numMappers = 100;
    int numReducers = 1;

    if (!fs.exists(new Path(inputPath))) {
      throw new RuntimeException("Error, input path does not exist!");
    }

    sLogger.setLevel(Level.INFO);

    fs.delete(new Path(outputPath), true);
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.setJarByClass(SampleTermDocVectors.class);
    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 100);
    job.setInt("mapred.reduce.max.attempts", 100);
    job.setInt("mapred.task.timeout", 600000000);

    sLogger.info("Running job " + job.getJobName());
    sLogger.info("Input directory: " + inputPath);
    sLogger.info("Output directory: " + outputPath);
    sLogger.info("Sample frequency: " + sampleFreq);
    sLogger.info("Sample docnos: " + job.get("Ivory.SampleFile"));
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

    if (sampleDocnosFile != null && !fs.exists(new Path(sampleDocnosFile))) {
      sLogger.info("Extracting sample docnos from sampled vectors...");
      SortedMap<WritableComparable, Writable> docno2DocVectors;
      try{
        docno2DocVectors = SequenceFileUtils.readFileIntoMap(new Path(outputPath+"/part-00000"));
        FSDataOutputStream out = fs.create(new Path(sampleDocnosFile));
        for(Entry<WritableComparable, Writable> entry : docno2DocVectors.entrySet()){
          int docno = ((IntWritable) entry.getKey()).get();
          out.writeBytes(docno+"\n");
        }
        out.close();
      } catch (Exception e) {
        throw new RuntimeException(e.toString());
      }
    }
    
    return 0;
  }
  private Options options;
  private String sampleDocnosFile, inputPath, outputPath, workDir;
  private int sampleSize;
  private float sampleFreq;

  private static final String WORKDIR_PATH_OPTION = "index";
  private static final String INPUT_PATH_OPTION = "input";
  private static final String OUTPUT_PATH_OPTION = "output"; 
  private static final String SAMPLEDOCNOS_OPTION = "docnos";
  private static final String SAMPLESIZE_OPTION = "size";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to directory with weighted term doc vectors").withArgName("path").hasArg().isRequired().create(WORKDIR_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to weighted term doc vectors").withArgName("path").hasArg().create(INPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to sampled weighted term doc vectors").withArgName("path").hasArg().create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("only keep pairs that match these docnos").withArgName("path to sample docnos file").hasArg().create(SAMPLEDOCNOS_OPTION));
    options.addOption(OptionBuilder.withDescription("sample a document with probability = number-of-docs/N").withArgName("N").hasArg().create(SAMPLESIZE_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    workDir = cmdline.getOptionValue(WORKDIR_PATH_OPTION);
    inputPath = cmdline.hasOption(INPUT_PATH_OPTION) ? cmdline.getOptionValue(INPUT_PATH_OPTION) : null;
    outputPath = cmdline.hasOption(OUTPUT_PATH_OPTION) ? cmdline.getOptionValue(OUTPUT_PATH_OPTION) : null;
    sampleSize = cmdline.hasOption(SAMPLESIZE_OPTION) ? Integer.parseInt(cmdline.getOptionValue(SAMPLESIZE_OPTION)) : -1;
    sampleDocnosFile = cmdline.hasOption(SAMPLEDOCNOS_OPTION) ? cmdline.getOptionValue(SAMPLEDOCNOS_OPTION) : null;

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SampleIntDocVectors(), args);
    return;
  }
}
