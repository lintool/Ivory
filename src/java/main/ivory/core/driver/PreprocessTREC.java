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
import ivory.core.preprocess.ComputeGlobalTermStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocnoMappingBuilder;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

public class PreprocessTREC extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessTREC.class);

  private static int printUsage() {
    System.out.println("usage: [input-path] [index-path] [collection-name] [language] [stopwords] [tokenizer-class] [tokenizer-model-path]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length < 6) {
      printUsage();
      return -1;
    }

    String collection = args[0];
    String indexRootPath = args[1];
    String collectionName = args[2];
    String language = args[3];
    boolean isStopWords = Boolean.parseBoolean(args[4]);
    String tokenizerClass = args[5];
    
    String tokenizerPath = null;
    if (args.length == 7) {
      tokenizerPath = args[6];
    }
    LOG.info("Tool name: " + PreprocessTREC.class.getCanonicalName());
    LOG.info(" - Collection path: " + collection);
    LOG.info(" - Index path: " + indexRootPath);
    LOG.info(" - Language: " + language);
    LOG.info(" - Stop-word removal?: " + isStopWords);
    LOG.info(" - Tokenizer class: " + tokenizerClass);
    LOG.info(" - Tokenizer path: " + tokenizerPath);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    // Create the index directory if it doesn't already exist.
    Path p = new Path(indexRootPath);
    if (!fs.exists(p)) {
      LOG.info("index directory doesn't exist, creating...");
      fs.mkdirs(p);
    }	

    RetrievalEnvironment env = new RetrievalEnvironment(indexRootPath, fs);
    Path mappingFile = env.getDocnoMappingData();
    new TrecDocnoMappingBuilder().build(new Path(collection), mappingFile, conf);

    conf.setBoolean(Constants.Stopword, isStopWords);
    conf.set(Constants.CollectionName, collectionName);
    conf.set(Constants.CollectionPath, collection);
    conf.set(Constants.IndexPath, indexRootPath);
    conf.set(Constants.InputFormat, TrecDocumentInputFormat.class.getCanonicalName());
    conf.set(Constants.Tokenizer, tokenizerClass);
    if (tokenizerPath != null) {
      conf.set(Constants.TokenizerData, tokenizerPath);
    }
    conf.set(Constants.DocnoMappingClass, TrecDocnoMapping.class.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());
    
    conf.setInt(Constants.DocnoOffset, 0); // docnos start at 1
    conf.setInt(Constants.MinDf, 2); // toss away singleton terms
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
    conf.setInt(Constants.TermIndexWindow, 8);
    conf.set(Constants.Language, language);
    
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
    ToolRunner.run(new Configuration(), new PreprocessTREC(), args);
  }
}
