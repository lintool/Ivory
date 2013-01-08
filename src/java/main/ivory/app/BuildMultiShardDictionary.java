package ivory.app;

import ivory.core.Constants;
import ivory.core.index.MergeGlobalTermStatistics;
import ivory.core.preprocess.BuildDictionary;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BuildMultiShardDictionary extends Configured implements Tool {

  public static final String COLLECTION_NAME = "collectionName";
  public static final String OUTPUT_PATH = "outputPath";
  public static final String MIN_DF = "minDf";
  public static final String INDEX_PATHS = "indexPaths";

  @SuppressWarnings({ "static-access" })
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("collection name").create(COLLECTION_NAME));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_PATH));
    options.addOption(OptionBuilder.withArgName("paths").hasArg()
        .withDescription("index paths").create(INDEX_PATHS));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("min df").create(MIN_DF));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(COLLECTION_NAME) || !cmdline.hasOption(OUTPUT_PATH) ||
        !cmdline.hasOption(INDEX_PATHS) || !cmdline.hasOption(MIN_DF)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Configuration conf = getConf();
    conf.set(Constants.CollectionName, cmdline.getOptionValue(COLLECTION_NAME));
    conf.setInt(Constants.MinDf, Integer.parseInt(cmdline.getOptionValue(MIN_DF)));
    conf.set("Ivory.IndexPaths", cmdline.getOptionValue(INDEX_PATHS));
    conf.set("Ivory.DictionaryOutputPath", cmdline.getOptionValue(OUTPUT_PATH));

    new MergeGlobalTermStatistics(conf).run();

    conf.set(Constants.IndexPath, cmdline.getOptionValue(OUTPUT_PATH));
    new BuildDictionary(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildMultiShardDictionary(), args);
  }
}
