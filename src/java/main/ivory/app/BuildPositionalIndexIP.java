/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.app;

import ivory.core.Constants;
import ivory.core.index.BuildIPInvertedIndexDocSorted;
import ivory.core.index.BuildIntPostingsForwardIndex;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BuildPositionalIndexIP extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildPositionalIndexIP.class);

  @SuppressWarnings({"static-access"}) @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) index path").create(IndexBuilder.INDEX_PATH));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("(optional) number of index partitions: 64 default")
        .create(IndexBuilder.INDEX_PARTITIONS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(IndexBuilder.INDEX_PATH)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    int indexPartitions = cmdline.hasOption(IndexBuilder.INDEX_PARTITIONS) ?
        Integer.parseInt(cmdline.getOptionValue(IndexBuilder.INDEX_PARTITIONS)) : 64;

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    String indexPath = cmdline.getOptionValue(IndexBuilder.INDEX_PATH);

    Path p = new Path(indexPath);
    if (!fs.exists(p)) {
      LOG.warn("Index path doesn't exist...");
      return -1;
    }

    LOG.info("Tool name: " + BuildPositionalIndexIP.class.getSimpleName());
    LOG.info(String.format(" -%s %s", IndexBuilder.INDEX_PATH, indexPath));
    LOG.info(String.format(" -%s %d", IndexBuilder.INDEX_PARTITIONS, indexPartitions));

    conf.set(Constants.IndexPath, indexPath);
    conf.setInt(Constants.NumReduceTasks, indexPartitions);
    conf.set(Constants.PostingsListsType,
        ivory.core.data.index.PostingsListDocSortedPositional.class.getCanonicalName());

    new BuildIPInvertedIndexDocSorted(conf).run();
    new BuildIntPostingsForwardIndex(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildPositionalIndexIP(), args);
  }
}
