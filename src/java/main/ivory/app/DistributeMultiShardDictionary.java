package ivory.app;

import ivory.core.Constants;
import ivory.core.index.BuildIntPostingsForwardIndex;
import ivory.core.index.ReplacePostingsGlobalTermStatistics;

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

public class DistributeMultiShardDictionary extends Configured implements Tool {

  public static final String SHARD_PATH = "shardPath";
  public static final String DICTIONARY_PATH = "dictionaryPath";

  @SuppressWarnings({ "static-access" })
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("shard path").create(SHARD_PATH));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index path").create(DICTIONARY_PATH));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(SHARD_PATH) || !cmdline.hasOption(DICTIONARY_PATH)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Configuration conf = getConf();
    conf.set(Constants.IndexPath, cmdline.getOptionValue(SHARD_PATH));
    conf.set("Ivory.DictionaryPath", cmdline.getOptionValue(DICTIONARY_PATH));

    new ReplacePostingsGlobalTermStatistics(conf).run();
    new BuildIntPostingsForwardIndex(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistributeMultiShardDictionary(), args);
  }
}
