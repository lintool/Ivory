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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.trecweb.TrecWebDocnoMappingBuilder;
import edu.umd.cloud9.collection.trecweb.Wt10gDocnoMapping;

public class PreprocessWt10g extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessWt10g.class);

  private static int printUsage() {
    System.out.println("usage: [input-path] [index-path]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      return -1;
    }

    String collection = args[0];
    String indexRootPath = args[1];

    LOG.info("Tool name: " + PreprocessWt10g.class.getCanonicalName());
    LOG.info(" - Collection path: " + collection);
    LOG.info(" - Index path: " + indexRootPath);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    // Create the index directory if it doesn't already exist.
    Path p = new Path(indexRootPath);
    if (!fs.exists(p)) {
      LOG.info("index directory doesn't exist, creating...");
      fs.mkdirs(p);
    } else {
      LOG.info("Index directory " + p + " already exists!");
      return -1;
    }

    RetrievalEnvironment env = new RetrievalEnvironment(indexRootPath, fs);
    Path mappingFile = env.getDocnoMappingData();
    new TrecWebDocnoMappingBuilder().build(new Path(collection), mappingFile, conf);

    conf.set(Constants.CollectionName, "Wt10g");
    conf.set(Constants.CollectionPath, collection);
    conf.set(Constants.IndexPath, indexRootPath);
    conf.set(Constants.InputFormat, SequenceFileInputFormat.class.getCanonicalName());
    conf.set(Constants.Tokenizer, GalagoTokenizer.class.getCanonicalName());
    conf.set(Constants.DocnoMappingClass, Wt10gDocnoMapping.class.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, mappingFile.toString());

    conf.setInt(Constants.DocnoOffset, 0); // docnos start at 1
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
    ToolRunner.run(new Configuration(), new PreprocessWt10g(), args);
  }
}
