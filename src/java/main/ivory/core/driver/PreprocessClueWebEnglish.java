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

package ivory.core.driver;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.preprocess.BuildDictionary;
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.tokenize.GalagoTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;

public class PreprocessClueWebEnglish extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessClueWebEnglish.class);

  public static int[] SegmentDocCounts = new int[] { 3382356, 50220423, 51577077, 50547493,
      52311060, 50756858, 50559093, 52472358, 49545346, 50738874, 45175228 };

  public static int[] DocnoOffsets = new int[] { 0, 0, 50220423, 101797500, 152344993, 204656053,
      255412911, 305972004, 358444362, 407989708, 458728582 };

  private static int printUsage() {
    System.out.println("usage: [input-path] [index-path] [segment-num]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      printUsage();
      return -1;
    }

    String collection = args[0];
    String indexPath = args[1];
    int segment = Integer.parseInt(args[2]);

    LOG.info("Tool name: " + PreprocessClueWebEnglish.class.getCanonicalName());
    LOG.info(" - Collection path: " + collection);
    LOG.info(" - Index path: " + indexPath);
    LOG.info(" - segement number: " + segment);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    Path p = new Path(indexPath);
    if (!fs.exists(p)) {
      LOG.error("Error: index path doesn't exist!");
      return 0;
    }

    if (!fs.exists(env.getDocnoMappingData())) {
      LOG.error("Error: docno mapping data doesn't exist!");
      return 0;
    }

    conf.set(Constants.CollectionName, "ClueWeb:English:Segment" + segment);
    conf.set(Constants.CollectionPath, collection);
    conf.set(Constants.IndexPath, indexPath);
    conf.set(Constants.InputFormat, SequenceFileInputFormat.class.getCanonicalName());
    conf.set(Constants.Tokenizer, GalagoTokenizer.class.getCanonicalName());
    conf.set(Constants.DocnoMappingClass, ClueWarcDocnoMapping.class.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());

    conf.setInt(Constants.DocnoOffset, DocnoOffsets[segment]);
    conf.setInt(Constants.MinDf, 10);
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
    conf.setInt(Constants.TermIndexWindow, 8);

    new BuildTermDocVectors(conf).run();
    new BuildDictionary(conf).run();
//    new GetTermCount(conf).run();
//    new BuildTermIdMap(conf).run();
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
