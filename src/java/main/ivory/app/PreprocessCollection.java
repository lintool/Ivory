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

import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public abstract class PreprocessCollection extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessCollection.class);
  
  protected String collectionPath;
  protected String indexPath;

  /**
   * Runs this tool.
   */
  @SuppressWarnings({"static-access"}) @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(IndexBuilder.COLLECTION_PATH_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) index path").create(IndexBuilder.INDEX_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(IndexBuilder.COLLECTION_PATH_OPTION) ||
        !cmdline.hasOption(IndexBuilder.INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    collectionPath = cmdline.getOptionValue(IndexBuilder.COLLECTION_PATH_OPTION);
    indexPath = cmdline.getOptionValue(IndexBuilder.INDEX_OPTION);

    LOG.info("Tool name: " + PreprocessCollection.class.getSimpleName());
    List<String> s = Lists.newArrayList();
    for (Map.Entry<String, String> e : getCollectionSettings().entrySet()) {
      LOG.info(String.format(" -%s %s", e.getKey(), e.getValue()));
      s.add("-" + e.getKey());
      s.add(e.getValue());
    }
    String[] arr = s.toArray(new String[s.size()]);

    IndexBuilder.main(arr);
    return 0;
  }

  public abstract Map<String, String> getCollectionSettings();
}
