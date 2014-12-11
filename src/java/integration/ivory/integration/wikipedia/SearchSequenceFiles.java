package ivory.integration.wikipedia;

import ivory.core.data.document.WeightedIntDocVector;

import java.io.IOException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import tl.lin.data.map.HMapStFW;

/**
 * Read sequence files, output key-value pairs that match specified key.
 * 
 * @author ferhanture
 * 
 */
public class SearchSequenceFiles extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(SearchSequenceFiles.class);

  static enum mapoutput {
    count
  }

  private static Options options;;

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "SearchSequenceFiles", options );
    System.exit(-1);    
  }

  public SearchSequenceFiles() {
    super();
  }

  static class MyMapperTerm extends MapReduceBase implements
  Mapper<IntWritable, HMapStFW, IntWritable, HMapStFW> {
    private String[] keys;
    
    public void configure(JobConf job) {
      keys = job.get("keys").split(",");
    }

    public void map(IntWritable key, HMapStFW value, OutputCollector<IntWritable, HMapStFW> output,
        Reporter reporter) throws IOException {
      for (String compareKey : keys) {
        int k = Integer.parseInt(compareKey);
        if (k == key.get()) {
          output.collect(key, value);
        }
      }
    }
  }
  
  static class MyMapperInt extends MapReduceBase implements
      Mapper<IntWritable, WeightedIntDocVector, IntWritable, WeightedIntDocVector> {
    private String[] keys;

    public void configure(JobConf job) {
      keys = job.get("keys").split(",");
    }

    public void map(IntWritable key, WeightedIntDocVector value,
        OutputCollector<IntWritable, WeightedIntDocVector> output, Reporter reporter)
        throws IOException {
      for (String compareKey : keys) {
        int k = Integer.parseInt(compareKey);
        if (k == key.get()) {
          output.collect(key, value);
        }
      }
    }
  }
  
  public int run(String[] args) throws Exception { 
    CommandLine commandline = parseArgs(args);
    if (commandline == null) {
      printUsage();
      System.exit(-1);
    }
    String inputPath = commandline.getOptionValue(IN_OPTION);
    String outputPath = commandline.getOptionValue(OUT_OPTION);
    String searchedKeys = commandline.getOptionValue(KEYS_OPTION);
    String valueClassName = commandline.getOptionValue(VALUECLASS_OPTION);
    
    JobConf job = new JobConf(getConf(), SearchSequenceFiles.class);
    job.setJobName("SearchSequenceFiles");
     
    FileSystem.get(job).delete(new Path(outputPath), true);
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.set("mapred.child.java.opts", "-Xmx2048m");
    job.setInt("mapred.map.max.attempts", 100);
    job.setInt("mapred.reduce.max.attempts", 100);
    job.setInt("mapred.task.timeout", 600000000);
    job.set("keys", searchedKeys);
 
    LOG.setLevel(Level.INFO);
    
    LOG.info("Running job "+job.getJobName());
    LOG.info("Input directory: "+inputPath);
    LOG.info("Output directory: "+outputPath);
    LOG.info("Value class: "+valueClassName);

    if (valueClassName.contains("HMapSFW")) {
      job.setMapperClass(MyMapperTerm.class);
      job.setMapOutputValueClass(HMapStFW.class);
      job.setOutputValueClass(HMapStFW.class);
    } else {
      job.setMapperClass(MyMapperInt.class);      
      job.setMapOutputValueClass(WeightedIntDocVector.class);
      job.setOutputValueClass(WeightedIntDocVector.class);
    }
    job.setReducerClass(IdentityReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setNumReduceTasks(1);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    return 0;
  }

  private static final String IN_OPTION = "input";
  private static final String OUT_OPTION = "output";
  private static final String VALUECLASS_OPTION = "valueclass";
  private static final String KEYS_OPTION = "keys";
  private static final String LIBJARS_OPTION = "libjars";

  @SuppressWarnings("static-access")
  private static CommandLine parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("path to input <IntWritable, V> SequenceFiles").withArgName("path").hasArg().isRequired().create(IN_OPTION));
    options.addOption(OptionBuilder.withDescription("path to output").withArgName("path").hasArg().isRequired().create(OUT_OPTION));
    options.addOption(OptionBuilder.withDescription("Class of Value objects in SequenceFiles").withArgName("class").hasArg().isRequired().create(VALUECLASS_OPTION));
    options.addOption(OptionBuilder.withDescription("Integer keys to output, comma-separated").withArgName("comma-separated integers").hasArg().isRequired().create(KEYS_OPTION));
    options.addOption(OptionBuilder.withDescription("Hadoop option to load external jars").withArgName("jar packages").hasArg().create(LIBJARS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return null;
    }
    return cmdline;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SearchSequenceFiles(), args);
  }

}
