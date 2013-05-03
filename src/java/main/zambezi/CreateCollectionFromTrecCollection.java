package zambezi;

import ivory.core.tokenize.GalagoTokenizer;

import java.io.IOException;
import java.util.Arrays;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.trec.CountTrecDocuments;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

public class CreateCollectionFromTrecCollection extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(CreateCollectionFromTrecCollection.class);
  private static enum Count { DOCS };

  private static class MyMapper extends Mapper<LongWritable, TrecDocument, IntWritable, Text> {
    private static final IntWritable DOCNO = new IntWritable();
    private static final Text TEXT = new Text();

    private GalagoTokenizer tokenizer = new GalagoTokenizer();
    private StringBuffer buffer = new StringBuffer();

    @Override
    public void map(LongWritable key, TrecDocument doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Count.DOCS).increment(1);

      String[] terms = tokenizer.processContent(doc.getContent());
      buffer.delete(0, buffer.length());
      for (String t : terms) {
        if (t.matches("[a-zA-Z_0-9-]*")) {
          buffer.append(t + " ");
        }
      }
      TEXT.set(buffer.toString().trim());
      DOCNO.set(Integer.parseInt(doc.getDocid().split("-")[1]));
      context.write(DOCNO, TEXT);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public CreateCollectionFromTrecCollection() {}

  public static final String COLLECTION_OPTION = "collection";
  public static final String OUTPUT_OPTION = "output";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) output path").create(OUTPUT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(COLLECTION_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool: " + CountTrecDocuments.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output dir: " + outputPath);

    Job job = new Job(getConf(), CountTrecDocuments.class.getSimpleName());
    job.setJarByClass(CountTrecDocuments.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(TrecDocumentInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Count.DOCS).getValue();
    LOG.info("Read " + numDocs + " docs.");

    return numDocs;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + CountTrecDocuments.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new CreateCollectionFromTrecCollection(), args);
  }
}

