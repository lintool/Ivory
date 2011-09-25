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
import ivory.core.index.BuildIntPostingsForwardIndex;
import ivory.core.index.BuildIPInvertedIndexDocSorted;
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermIdMap;
import ivory.core.preprocess.BuildWeightedIntDocVectors;
import ivory.core.preprocess.GetTermCount;
import ivory.core.tokenize.GalagoTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.aquaint2.Aquaint2DocnoMapping;
import edu.umd.cloud9.collection.aquaint2.Aquaint2DocumentInputFormat2;
import edu.umd.cloud9.collection.aquaint2.BuildAquaint2ForwardIndex;
import edu.umd.cloud9.collection.aquaint2.NumberAquaint2Documents2;


public class PreprocessAquaint2 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessAquaint2.class);

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

    LOG.info("Tool name: " + PreprocessAquaint2.class.getCanonicalName());
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
              mappingFile.toString() };
      NumberAquaint2Documents2 tool = new NumberAquaint2Documents2();
      tool.setConf(conf);
      tool.run(arr);

      fs.delete(mappingDir, true);
    }

	int numMappers = 100;
	int numReducers = 100;
	conf.setInt("Ivory.NumMapTasks", numMappers);
	conf.setInt("Ivory.NumReduceTasks", numReducers);
    conf.set(Constants.CollectionName, "Aquaint2");
    conf.set(Constants.CollectionPath, collection);
    conf.set(Constants.IndexPath, indexRootPath);
    conf.set(Constants.InputFormat, Aquaint2DocumentInputFormat2.class.getCanonicalName());
    conf.set(Constants.Tokenizer, GalagoTokenizer.class.getCanonicalName());
    conf.set(Constants.DocnoMappingClass, Aquaint2DocnoMapping.class.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());

    conf.setInt(Constants.DocnoOffset, 0); // docnos start at 1
    conf.setInt(Constants.MinDf, 2); // toss away singleton terms
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
    conf.setInt(Constants.TermIndexWindow, 8);

    new BuildTermDocVectors(conf).run();
    new GetTermCount(conf).run();
    new BuildTermIdMap(conf).run();
    new BuildIntDocVectors(conf).run();

    new BuildIntDocVectorsForwardIndex(conf).run();
    new BuildTermDocVectorsForwardIndex(conf).run();

	new BuildIPInvertedIndexDocSorted(conf).run();

	conf.set(Constants.ScoringModel, "ivory.pwsim.score.TfIdf");
	conf.setBoolean(Constants.Normalize, true);

	new BuildIntPostingsForwardIndex(conf).run();

	// String findexDirPath = indexRootPath + "/findex";
	// String findexFilePath = indexRootPath + "/findex.dat";
	// new BuildWeightedIntDocVectors(conf).run();

	// conf.setBoolean(Constants.BuildWeighted, true);
    // new BuildIntDocVectorsForwardIndex(conf).run();

    // if (fs.exists(new Path(findexDirPath))) {
    //   LOG.info("ForwardIndex already exists: Skipping!");
    // } else {
	// 	new BuildAquaint2ForwardIndex ().runTool (conf, collection, findexDirPath, findexFilePath, mappingFile.toString ());
	// }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new PreprocessAquaint2(), args);
  }
}
