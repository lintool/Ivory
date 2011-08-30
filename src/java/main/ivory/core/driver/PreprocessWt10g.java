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
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermIdMap;
import ivory.core.preprocess.GetTermCount;
import ivory.core.tokenize.GalagoTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.trecweb.NumberTrecWebDocuments;
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
    }

    RetrievalEnvironment env = new RetrievalEnvironment(indexRootPath, fs);

    // Look for the docno mapping, which maps from docid (String) to docno
    // (sequentially-number integer). If it doesn't exist create it.
    Path mappingFile = env.getDocnoMappingData();
    Path mappingDir = env.getDocnoMappingDirectory();

    if (!fs.exists(mappingFile)) {
      LOG.info("docno-mapping.dat doesn't exist, creating...");
      String[] arr = new String[] { collection, mappingDir.toString(),
          mappingFile.toString(), "100" };
      NumberTrecWebDocuments tool = new NumberTrecWebDocuments();
      tool.setConf(conf);
      tool.run(arr);

      fs.delete(mappingDir, true);
    }

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
    conf.setInt(Constants.TermIndexWindow, 8);

    new BuildTermDocVectors(conf).run();
    new GetTermCount(conf).run();
    new BuildTermIdMap(conf).run();
    new BuildIntDocVectors(conf).run();

    new BuildIntDocVectorsForwardIndex(conf).run();
    new BuildTermDocVectorsForwardIndex(conf).run();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PreprocessWt10g(), args);
    System.exit(res);
  }
}
