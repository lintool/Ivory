/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval research
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

import java.io.IOException;

import ivory.core.Constants;
import ivory.core.RetrievalEnvironment;
import ivory.core.preprocess.BuildDictionary;
import ivory.core.preprocess.BuildIntDocVectors;
import ivory.core.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;
import ivory.core.preprocess.ComputeGlobalTermStatistics;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
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
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocnoMappingBuilder;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

public class PreprocessTrecForeign extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessTrecForeign.class);
  private Options options;

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    Configuration conf = parseArgs(args);
    FileSystem fs = FileSystem.get(conf);
    String indexRootPath = conf.get(Constants.IndexPath);
    String collection = conf.get(Constants.CollectionPath);

    RetrievalEnvironment env = new RetrievalEnvironment(indexRootPath, fs);
    Path mappingFile = env.getDocnoMappingData();
    new TrecDocnoMappingBuilder().build(new Path(collection), mappingFile, conf);

    conf.set(Constants.DocnoMappingClass, TrecDocnoMapping.class.getCanonicalName());
    conf.set(Constants.DocnoMappingFile, env.getDocnoMappingData().toString());
    conf.setInt(Constants.DocnoOffset, 0); // docnos start at 1
    conf.setInt(Constants.MinDf, 2); // toss away singleton terms
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
    conf.setInt(Constants.TermIndexWindow, 8);
    conf.set(Constants.InputFormat, TrecDocumentInputFormat.class.getCanonicalName());

    new BuildTermDocVectors(conf).run();
    new ComputeGlobalTermStatistics(conf).run();
    new BuildDictionary(conf).run();
    new BuildIntDocVectors(conf).run();

    new BuildIntDocVectorsForwardIndex(conf).run();
    new BuildTermDocVectorsForwardIndex(conf).run();

    return 0;
  }

  private static final String STOPWORDS_OPTION = "stopwords";
  private static final String INDEX_PATH_OPTION = "index";
  private static final String INPUT_PATH_OPTION = "input";
  private static final String LANGUAGE_OPTION = "lang";
  private static final String TOKENIZER_CLASS_OPTION = "tokenizerclass";
  private static final String TOKENIZER_MODEL_OPTION = "tokenizermodel";
  private static final String COLLECTION_NAME_OPTION = "name";

  @SuppressWarnings("static-access")
  private Configuration parseArgs(String[] args) {
    Configuration conf = getConf();
    options = new Options();
    options.addOption(OptionBuilder.withDescription("tokenizer class").withArgName("class").hasArg().isRequired().create(TOKENIZER_CLASS_OPTION));
    options.addOption(OptionBuilder.withDescription("path to tokenizer model file/directory").withArgName("path").hasArg().create(TOKENIZER_MODEL_OPTION));
    options.addOption(OptionBuilder.withDescription("path to index directory").withArgName("path").hasArg().isRequired().isRequired().create(INDEX_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to XML collection file").withArgName("path").hasArg().isRequired().create(INPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter collection language code").withArgName("en|de|fr|zh|es|ar|tr").hasArg().isRequired().create(LANGUAGE_OPTION));
    options.addOption(OptionBuilder.withDescription("path to stopwords file").withArgName("path").hasArg().create(STOPWORDS_OPTION));
    options.addOption(OptionBuilder.withDescription("collection name").withArgName("path").hasArg().create(COLLECTION_NAME_OPTION));
    try{

      FileSystem fs = FileSystem.get(conf);

      CommandLine cmdline;
      CommandLineParser parser = new GnuParser();
      cmdline = parser.parse(options, args);

      String collection = cmdline.getOptionValue(INPUT_PATH_OPTION);
      String indexRootPath = cmdline.getOptionValue(INDEX_PATH_OPTION);
      String language = cmdline.getOptionValue(LANGUAGE_OPTION);
      String tokenizerClass = cmdline.getOptionValue(TOKENIZER_CLASS_OPTION);
      String stopwordsFile = null;
      String tokenizerPath = null;

      conf.set(Constants.CollectionPath, collection);
      conf.set(Constants.IndexPath, indexRootPath);
      conf.set(Constants.Tokenizer, tokenizerClass);
      conf.set(Constants.Language, language);

      if (cmdline.hasOption(COLLECTION_NAME_OPTION)) {
        conf.set(Constants.CollectionName, cmdline.getOptionValue(COLLECTION_NAME_OPTION));
      }
      if (cmdline.hasOption(STOPWORDS_OPTION)) {
        stopwordsFile = cmdline.getOptionValue(STOPWORDS_OPTION);
        conf.set(Constants.StopwordList, stopwordsFile);
      }
      if (cmdline.hasOption(TOKENIZER_MODEL_OPTION)) {
        tokenizerPath = cmdline.getOptionValue(TOKENIZER_MODEL_OPTION);
        conf.set(Constants.TokenizerData, tokenizerPath);
      }

      LOG.info("Tool name: " + PreprocessTrecForeign.class.getCanonicalName());
      LOG.info(" - Collection path: " + collection);
      LOG.info(" - Index path: " + indexRootPath);
      LOG.info(" - Language: " + language);
      LOG.info(" - Stop-word removal?: " + stopwordsFile);
      LOG.info(" - Tokenizer class: " + tokenizerClass);
      LOG.info(" - Tokenizer path: " + tokenizerPath);

      // Create the index directory if it doesn't already exist.
      Path p = new Path(indexRootPath);
      if (!fs.exists(p)) {
        LOG.info("index directory doesn't exist, creating...");
        fs.mkdirs(p);
      }
    } catch (IOException exp) {
      LOG.info("Error creating index directory: " + exp.getMessage());
      exp.printStackTrace();
    } catch (ParseException exp) {
      LOG.info("Error parsing command line: " + exp.getMessage());
      throw new RuntimeException();
    }

    return conf;
  }
  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PreprocessTrecForeign(), args);
  }
}
