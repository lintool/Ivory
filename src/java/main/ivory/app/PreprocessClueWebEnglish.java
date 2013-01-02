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
import ivory.core.RetrievalEnvironment;
import ivory.core.preprocess.BuildDictionary;
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.preprocess.ComputeGlobalTermStatistics;
import ivory.core.tokenize.GalagoTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcDocnoMappingBuilder;

public class PreprocessClueWebEnglish extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessClueWebEnglish.class);

  public static int[] SEGMENT_COUNTS = new int[] { 0, 50220423, 51577077, 50547493, 52311060,
      50756858, 50559093, 52472358, 49545346, 50738874, 45175228 };

  public static int[] DOCNO_OFFSETS = new int[] { 0, 0, 50220423, 101797500, 152344993, 204656053,
      255412911, 305972004, 358444362, 407989708, 458728582 };

  public static final String SEGMENT = "segment";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" }) @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();;

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(PreprocessCollection.COLLECTION_PATH));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) index path").create(PreprocessCollection.INDEX_PATH));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("(required) segment").create(SEGMENT));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(PreprocessCollection.COLLECTION_PATH) ||
        !cmdline.hasOption(PreprocessCollection.INDEX_PATH) ||
        !cmdline.hasOption(SEGMENT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String collection = cmdline.getOptionValue(PreprocessCollection.COLLECTION_PATH);
    String indexPath = cmdline.getOptionValue(PreprocessCollection.INDEX_PATH);
    int segment = Integer.parseInt(cmdline.getOptionValue(SEGMENT));

    LOG.info("Tool name: " + PreprocessClueWebEnglish.class.getSimpleName());
    LOG.info(" - collection path: " + collection);
    LOG.info(" - index path: " + indexPath);
    LOG.info(" - segement: " + segment);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    // Create the index directory if it doesn't already exist.
    Path p = new Path(indexPath);
    if (!fs.exists(p)) {
      LOG.info("index path doesn't exist, creating...");
      fs.mkdirs(p);
    } else {
      LOG.info("Index directory " + p + " already exists!");
      return -1;
    }

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    Path mappingFile = env.getDocnoMappingData();
    new ClueWarcDocnoMappingBuilder().build(new Path(collection), mappingFile, conf);

    conf.set(Constants.CollectionName, "ClueWeb:English:Segment" + segment);
    conf.set(Constants.CollectionPath, collection);
    conf.set(Constants.IndexPath, indexPath);
    conf.set(Constants.InputFormat, SequenceFileInputFormat.class.getCanonicalName());
    conf.set(Constants.Tokenizer, GalagoTokenizer.class.getCanonicalName());
    conf.set(Constants.DocnoMappingClass, ClueWarcDocnoMapping.class.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());

    conf.setInt(Constants.DocnoOffset, DOCNO_OFFSETS[segment]);
    conf.setInt(Constants.MinDf, 10);
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);

    new BuildTermDocVectors(conf).run();
    new ComputeGlobalTermStatistics(conf).run();
    new BuildDictionary(conf).run();
    new BuildIntDocVectors(conf).run();

    new BuildIntDocVectorsForwardIndex(conf).run();
    new BuildTermDocVectorsForwardIndex(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PreprocessClueWebEnglish(), args);
  }
}
