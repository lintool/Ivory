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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
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
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;

@SuppressWarnings("unchecked")
public class IndexBuilder extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(IndexBuilder.class);

  private static final String INPUT_OPTION = "collection";
  private static final String NAME_OPTION = "collectionName";
  private static final String INDEX_OPTION = "index";
  private static final String FORMAT_OPTION = "inputFormat";
  private static final String TOKENIZER_OPTION = "tokenizer";
  private static final String MAPPING_OPTION = "docnoMapping";

  @SuppressWarnings({"static-access"}) @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("(required) collection name").create(NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) output path").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("class").hasArg()
        .withDescription("(required) fully-qualified DocnoMapping").create(MAPPING_OPTION));

    options.addOption(OptionBuilder.withArgName("class").hasArg()
        .withDescription("(optional) fully-qualified Hadoop InputFormat: SequenceFileInputFormat default").create(FORMAT_OPTION));
    options.addOption(OptionBuilder.withArgName("class").hasArg()
        .withDescription("(optional) fully-qualified Tokenizer: GalagoTokenizer default").create(TOKENIZER_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(NAME_OPTION) ||
        !cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(MAPPING_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String collection = cmdline.getOptionValue(INPUT_OPTION);
    String collectionName = cmdline.getOptionValue(NAME_OPTION);
    String indexPath = cmdline.getOptionValue(INDEX_OPTION);

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
        inputFormatClass = (Class<? extends InputFormat>)
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

    LOG.info("Tool name: " + IndexBuilder.class.getCanonicalName());
    LOG.info(" - Collection path: " + collection);
    LOG.info(" - Collection name: " + collectionName);
    LOG.info(" - Index path: " + indexPath);
    LOG.info(" - DocnoMapping: " + docnoMappingClass.getCanonicalName());
    LOG.info(" - InputFormat: " + inputFormatClass.getCanonicalName());
    LOG.info(" - Tokenizer: " + tokenizerClass.getCanonicalName());

    int indexPartitions = 100;
    if ( docnoMappingClass.equals(TrecDocnoMapping.class)) {
      indexPartitions = 10;
      LOG.info("Recognized TREC collection: Setting indexPartitions = " + indexPartitions);
    }

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
    conf.setInt(Constants.MinDf, 2); // toss away singleton terms
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
    conf.setInt(Constants.TermIndexWindow, 8);

    Path mappingFile = env.getDocnoMappingData();
    docnoMappingClass.newInstance().getBuilder().build(new Path(collection), mappingFile, conf);

    new BuildTermDocVectors(conf).run();
    new ComputeGlobalTermStatistics(conf).run();
    new BuildDictionary(conf).run();
    new BuildIntDocVectors(conf).run();

    new BuildIntDocVectorsForwardIndex(conf).run();
    new BuildTermDocVectorsForwardIndex(conf).run();

    conf.setInt(Constants.NumReduceTasks, indexPartitions);
    conf.set(Constants.PostingsListsType, PostingsListDocSortedPositional.class.getCanonicalName());

    new BuildIPInvertedIndexDocSorted(conf).run();
    new BuildIntPostingsForwardIndex(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new IndexBuilder(), args);
  }
}