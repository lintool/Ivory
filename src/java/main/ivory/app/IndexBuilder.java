package ivory.app;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.index.BuildIPInvertedIndexDocSorted;
import ivory.core.index.BuildIntPostingsForwardIndex;
import ivory.core.preprocess.BuildDictionary;
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.preprocess.ComputeGlobalTermStatistics;
import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.tokenize.Tokenizer;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

@SuppressWarnings("unchecked")
public class IndexBuilder extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(IndexBuilder.class);

  public static final String COLLECTION_PATH_OPTION = "collection";
  public static final String COLLECTION_NAME_OPTION = "collectionName";
  public static final String INDEX_OPTION = "index";
  public static final String FORMAT_OPTION = "inputFormat";
  public static final String TOKENIZER_OPTION = "tokenizer";
  public static final String MAPPING_OPTION = "docnoMapping";
  public static final String MIN_DF_OPTION = "minDf";

  public static final String POSITIONAL_INDEX_IP_OPTION = "positionalIndexIP";
  public static final String NONPOSITIONAL_INDEX_IP_OPTION = "nonpositionalIndexIP";
  public static final String INDEX_PARTITIONS_OPTION = "indexPartitions";

  @SuppressWarnings({"static-access"}) @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(new Option(POSITIONAL_INDEX_IP_OPTION,
        "build positional index (IP algorithm)"));
    options.addOption(new Option(NONPOSITIONAL_INDEX_IP_OPTION,
        "build nonpositional index (IP algorithm)"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(COLLECTION_PATH_OPTION));
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("(required) collection name").create(COLLECTION_NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) index path").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("class").hasArg()
        .withDescription("(required) fully-qualified DocnoMapping").create(MAPPING_OPTION));

    options.addOption(OptionBuilder.withArgName("class").hasArg()
        .withDescription("(optional) fully-qualified Hadoop InputFormat: SequenceFileInputFormat default")
        .create(FORMAT_OPTION));
    options.addOption(OptionBuilder.withArgName("class").hasArg()
        .withDescription("(optional) fully-qualified Tokenizer: GalagoTokenizer default")
        .create(TOKENIZER_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("(optional) number of index partitions: 64 default")
        .create(INDEX_PARTITIONS_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("(optional) min Df")
        .create(MIN_DF_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(COLLECTION_PATH_OPTION) || !cmdline.hasOption(COLLECTION_NAME_OPTION) ||
        !cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(MAPPING_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String collection = cmdline.getOptionValue(COLLECTION_PATH_OPTION);
    String collectionName = cmdline.getOptionValue(COLLECTION_NAME_OPTION);
    String indexPath = cmdline.getOptionValue(INDEX_OPTION);

    int indexPartitions = cmdline.hasOption(INDEX_PARTITIONS_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(INDEX_PARTITIONS_OPTION)) : 64;

    Class<? extends DocnoMapping> docnoMappingClass = null;
    try {
      docnoMappingClass = (Class<? extends DocnoMapping>)
          Class.forName(cmdline.getOptionValue(MAPPING_OPTION));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    Class<? extends InputFormat> inputFormatClass = SequenceFileInputFormat.class;
    if (cmdline.hasOption(FORMAT_OPTION)) {
      try {
        inputFormatClass = (Class<? extends InputFormat<?, ?>>)
            Class.forName(cmdline.getOptionValue(FORMAT_OPTION));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    Class<? extends Tokenizer> tokenizerClass = GalagoTokenizer.class;
    if (cmdline.hasOption(TOKENIZER_OPTION)) {
      try {
        tokenizerClass = (Class<? extends Tokenizer>)
            Class.forName(cmdline.getOptionValue(TOKENIZER_OPTION));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    int minDf = 2;
    if (cmdline.hasOption(MIN_DF_OPTION)) {
      minDf = Integer.parseInt(cmdline.getOptionValue(MIN_DF_OPTION));
    }

    LOG.info("Tool name: " + IndexBuilder.class.getCanonicalName());
    LOG.info(String.format(" -%s %s", COLLECTION_PATH_OPTION, collection));
    LOG.info(String.format(" -%s %s", COLLECTION_NAME_OPTION, collectionName));
    LOG.info(String.format(" -%s %s", INDEX_OPTION, indexPath));
    LOG.info(String.format(" -%s %s", MAPPING_OPTION, docnoMappingClass.getCanonicalName()));
    LOG.info(String.format(" -%s %s", FORMAT_OPTION, inputFormatClass.getCanonicalName()));
    LOG.info(String.format(" -%s %s", TOKENIZER_OPTION, tokenizerClass.getCanonicalName()));
    LOG.info(String.format(" -%s %d", MIN_DF_OPTION, minDf));

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    // Create the index directory if it doesn't already exist.
    Path p = new Path(indexPath);
    if (!fs.exists(p)) {
      LOG.info("Index directory " + p + " doesn't exist, creating.");
      fs.mkdirs(p);
    } else {
      LOG.info("Index directory " + p + " already exists!");
      return -1;
    }

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    conf.set(Constants.CollectionName, collectionName);
    conf.set(Constants.CollectionPath, collection);
    conf.set(Constants.IndexPath, indexPath);
    conf.set(Constants.InputFormat, inputFormatClass.getCanonicalName());
    conf.set(Constants.Tokenizer, tokenizerClass.getCanonicalName());
    conf.set(Constants.DocnoMappingClass, docnoMappingClass.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());

    conf.setInt(Constants.DocnoOffset, 0); // docnos start at 1
    conf.setInt(Constants.MinDf, minDf);
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);

    Path mappingFile = env.getDocnoMappingData();
    docnoMappingClass.newInstance().getBuilder().build(new Path(collection), mappingFile, conf);

    new BuildTermDocVectors(conf).run();
    new ComputeGlobalTermStatistics(conf).run();
    new BuildDictionary(conf).run();
    new BuildIntDocVectors(conf).run();

    new BuildIntDocVectorsForwardIndex(conf).run();
    new BuildTermDocVectorsForwardIndex(conf).run();

    if (cmdline.hasOption(POSITIONAL_INDEX_IP_OPTION)) {
      LOG.info("Building positional index (IP algorithms)");
      LOG.info(String.format(" -%s: %d", INDEX_PARTITIONS_OPTION, indexPartitions));

      conf.setInt(Constants.NumReduceTasks, indexPartitions);
      conf.set(Constants.PostingsListsType,
          PostingsListDocSortedPositional.class.getCanonicalName());

      new BuildIPInvertedIndexDocSorted(conf).run();
      new BuildIntPostingsForwardIndex(conf).run();
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + IndexBuilder.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new IndexBuilder(), args);
  }
}