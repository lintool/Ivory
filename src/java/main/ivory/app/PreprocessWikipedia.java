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
import ivory.core.preprocess.BuildTargetLangWeightedIntDocVectors;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTranslatedTermDocVectors;
import ivory.core.preprocess.BuildWeightedIntDocVectors;
import ivory.core.preprocess.BuildWeightedTermDocVectors;
import ivory.core.preprocess.ComputeGlobalTermStatistics;
import ivory.core.tokenize.TokenizerFactory;
import java.io.IOException;
import java.util.Arrays;
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
import edu.umd.cloud9.collection.wikipedia.BuildWikipediaDocnoMapping;
import edu.umd.cloud9.collection.wikipedia.RepackWikipedia;
import edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMapping;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;

/**
 * Driver class that preprocesses a Wikipedia collection in any language. 
 * 
 * @author ferhanture
 *
 */
public class PreprocessWikipedia extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PreprocessWikipedia.class);

  /*
   * DEFINED PARAMETERS HERE:
   */
  private static final int MinDF = 2, MinNumTermsPerArticle = 5, TermIndexWindow = 8;
  private static final boolean IsNormalized = true;
  private static int MONO_LINGUAL = 0, CROSS_LINGUAL_E = 1, CROSS_LINGUAL_F = 2;
  private String indexRootPath;
  private String rawCollection;
  private String seqCollection;
  private String tokenizerClass;  
  private String collectionLang = null, tokenizerModel = null, collectionVocab = null, targetIndexPath = null, 
  fVocab_f2e = null, eVocab_f2e = null, fVocab_e2f, eVocab_e2f = null, ttable_f2e = null, ttable_e2f = null;
  private int mode;
  private Options options;

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( this.getClass().getCanonicalName(), options );
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if ( parseArgs(args) < 0 ) {
      printUsage();
      return -1;
    }
    Configuration conf = getConf();

    // user can either provide a tokenizer class as a program argument, 
    // or let the factory find an appropriate class based on language code
    try {
      Class.forName(tokenizerClass);
    } catch (Exception e) {
      tokenizerClass = TokenizerFactory.getTokenizerClass(collectionLang).getCanonicalName();
    }

    conf.set(Constants.Language, collectionLang);
    if (tokenizerModel != null) {
      conf.set(Constants.TokenizerData, tokenizerModel);
    }
    if (collectionVocab != null) {
      conf.set(Constants.CollectionVocab, collectionVocab);   // vocabulary to read collection from
    }

    // CROSS-LINGUAL CASE
    if (mode == CROSS_LINGUAL_E){		   // English side
      conf.set("Ivory.FinalVocab", collectionVocab);        // vocabulary to map terms to integers in BuildTargetLang...
    }

    if (mode == CROSS_LINGUAL_F) {			// non-English side, needs to be translated
      conf.set(Constants.TargetIndexPath, targetIndexPath);
      conf.set("Ivory.F_Vocab_F2E", fVocab_f2e);	
      conf.set("Ivory.E_Vocab_F2E", eVocab_f2e);
      conf.set("Ivory.TTable_F2E", ttable_f2e);
      conf.set("Ivory.E_Vocab_E2F", eVocab_e2f);	
      conf.set("Ivory.F_Vocab_E2F", fVocab_e2f);	
      conf.set("Ivory.TTable_E2F", ttable_e2f);
      conf.set("Ivory.FinalVocab", eVocab_f2e);            // vocabulary to map terms to integers in BuildTargetLang...
    }

    int numMappers = 100;
    int numReducers = 100;

    LOG.info("Tool name: WikipediaDriver");
    LOG.info(" - Index path: " + indexRootPath);
    LOG.info(" - Raw collection path: " + rawCollection);
    LOG.info(" - Compressed collection path: " + seqCollection);
    LOG.info(" - Collection language: " + collectionLang);
    LOG.info(" - Tokenizer class: " + tokenizerClass);
    LOG.info(" - Tokenizer model: " + tokenizerModel);
    LOG.info(" - Minimum # terms per article : " + MinNumTermsPerArticle);

    if (mode == CROSS_LINGUAL_E || mode == CROSS_LINGUAL_F) {
      LOG.info("Cross-lingual collection : Preprocessing "+collectionLang+" side.");
      LOG.info(" - Collection vocab file: " + collectionVocab);
      LOG.info(" - Tokenizer model: " + tokenizerModel);

      if (mode == CROSS_LINGUAL_F) {
        LOG.info(" - TTable file "+collectionLang+" --> E-language : " + ttable_f2e);
        LOG.info(" - Source vocab file: " + fVocab_f2e);
        LOG.info(" - Target vocab file: " + eVocab_f2e);
        LOG.info(" - TTable file "+"E-language --> "+collectionLang+" : " + ttable_e2f);
        LOG.info(" - Source vocab file: " + fVocab_f2e);
        LOG.info(" - Target vocab file: " + eVocab_f2e);
      }
    }

    FileSystem fs = FileSystem.get(conf);

    Path p = new Path(indexRootPath);
    if (!fs.exists(p)) {
      LOG.info("Index path doesn't exist, creating...");
      fs.mkdirs(p);
    }
    RetrievalEnvironment env = new RetrievalEnvironment(indexRootPath, fs);

    // Build docno mapping from raw collection
    Path mappingFile = env.getDocnoMappingData();
    if (!fs.exists(mappingFile)) {
      LOG.info(mappingFile + " doesn't exist, creating...");
      String[] arr = new String[] {
          "-input=" + rawCollection,
          "-output_path=" + indexRootPath + "/wiki-docid-tmp",
          "-output_file=" + mappingFile.toString() };
      LOG.info("Running BuildWikipediaDocnoMapping with args " + Arrays.toString(arr));

      BuildWikipediaDocnoMapping tool = new BuildWikipediaDocnoMapping();
      tool.setConf(conf);
      tool.run(arr);

      fs.delete(new Path(indexRootPath + "/wiki-docid-tmp"), true);
    }

    // Repack Wikipedia into sequential compressed block
    p = new Path(seqCollection);
    LOG.info(seqCollection + " doesn't exist, creating...");
    String[] arr = new String[] { "-input=" + rawCollection,
        "-output=" + seqCollection,
        "-mapping_file=" + mappingFile.toString(),
        "-compression_type=block",
        "-wiki_language=" + collectionLang };
    LOG.info("Running RepackWikipedia with args " + Arrays.toString(arr));

    RepackWikipedia tool = new RepackWikipedia();
    tool.setConf(conf);
    tool.run(arr);

    conf.set(Constants.CollectionName, "Wikipedia-"+collectionLang);
    conf.setInt(Constants.NumMapTasks, numMappers);
    conf.setInt(Constants.NumReduceTasks, numReducers);
    conf.set(Constants.CollectionPath, seqCollection);
    conf.set(Constants.IndexPath, indexRootPath);
    conf.set(Constants.InputFormat, SequenceFileInputFormat.class.getCanonicalName());
    conf.set(Constants.DocnoMappingClass, WikipediaDocnoMapping.class.getCanonicalName());
    conf.set(Constants.Tokenizer, tokenizerClass);			//"ivory.tokenize.OpenNLPTokenizer"
    conf.setInt(Constants.MinDf, MinDF);
    conf.setInt(Constants.MaxDf, Integer.MAX_VALUE);
    conf.setInt(Constants.DocnoOffset, 0); // docnos start at 1
    conf.setInt(Constants.TermIndexWindow, TermIndexWindow);

    // Builds term doc vectors from document collection, and filters the terms that are not included
    // in Ivory.SrcVocab.
    long startTime = System.currentTimeMillis();
    long preprocessStartTime = System.currentTimeMillis();
    LOG.info("Building term doc vectors...");
    int exitCode = new BuildTermDocVectors(conf).run();
    if (exitCode >= 0) {
      LOG.info("Job BuildTermDocVectors finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }else {
      LOG.info("Error: BuildTermDocVectors. Terminating...");
      return -1;
    }

    // Get CF and DF counts.
    startTime = System.currentTimeMillis();
    LOG.info("Counting terms...");
    exitCode = new ComputeGlobalTermStatistics(conf).run();
    LOG.info("TermCount = " + env.readCollectionTermCount());
    if (exitCode >= 0) {
      LOG.info("Job ComputeGlobalTermStatistics finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }else {
      LOG.info("Error: ComputeGlobalTermStatistics. Terminating...");
      return -1;
    }
    // Build a map from terms to sequentially generated integer term ids.
    startTime = System.currentTimeMillis();
    LOG.info("Building term-to-integer id mapping...");
    exitCode = new BuildDictionary(conf).run();
    if (exitCode >= 0) {
      LOG.info("Job BuildDictionary finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }else{
      LOG.info("Error: BuildDictionary. Terminating...");
      return -1;
    }

    // Compute term weights, and output weighted term doc vectors.
    LOG.info("Building weighted term doc vectors...");
    startTime = System.currentTimeMillis();

    conf.set("Ivory.ScoringModel", "ivory.pwsim.score.Bm25");
    conf.setBoolean("Ivory.Normalize", IsNormalized);
    conf.setInt("Ivory.MinNumTerms",MinNumTermsPerArticle);

    if (mode == CROSS_LINGUAL_F) {
      // Translate term doc vectors into English.
      exitCode = new BuildTranslatedTermDocVectors(conf).run();
    } else {
      // Build weighted term doc vectors.
      exitCode = new BuildWeightedTermDocVectors(conf).run();
    }
    if (exitCode >= 0) {
      LOG.info("Job BuildTranslated/WeightedTermDocVectors finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }else {
      LOG.info("Error: BuildTranslated/WeightedTermDocVectors. Terminating...");
      return -1;
    }

    // normalize (optional) and convert weighted term doc vectors into int doc vectors for efficiency
    startTime = System.currentTimeMillis();
    LOG.info("Building weighted integer doc vectors...");
    conf.setBoolean("Ivory.Normalize", IsNormalized);
    if (mode == MONO_LINGUAL) {
      exitCode = new BuildIntDocVectors(conf).run();
      exitCode = new BuildWeightedIntDocVectors(conf).run();
      if (exitCode >= 0) {
        LOG.info("Job BuildWeightedIntDocVectors finished in "+(System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
      }else {
        LOG.info("Error: BuildWeightedIntDocVectors. Terminating...");
        return -1;
      }
    } else {
      BuildTargetLangWeightedIntDocVectors weightedIntVectorsTool =
        new BuildTargetLangWeightedIntDocVectors(conf);

      int finalNumDocs = weightedIntVectorsTool.run();

      LOG.info("Job BuildTargetLangWeightedIntDocVectors finished in " +
          (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
      if (finalNumDocs > 0) {
        LOG.info("Changed doc count: " + env.readCollectionDocumentCount() +" => " + finalNumDocs);
        env.writeCollectionDocumentCount(finalNumDocs);
      }else {
        LOG.info("No document output! Terminating...");
        return -1;
      }
      // set Property.CollectionTermCount to the size of the target vocab. since all docs are translated into that vocab. This property is read by WriteRandomVectors via RunComputeSignatures.
      Vocab engVocabH = null;
      try {
        engVocabH = HadoopAlign.loadVocab(new Path(conf.get("Ivory.FinalVocab")), conf);
      } catch (IOException e) {
        e.printStackTrace();
      }	
      LOG.info("Changed term count: " + env.readCollectionTermCount() + " => " + engVocabH.size());
      env.writeCollectionTermCount(engVocabH.size());
    }

    LOG.info("Preprocessing job finished in " + (System.currentTimeMillis() - preprocessStartTime) / 1000.0 + " seconds");

    return 0;
  }

  private static final String MODE_OPTION = "mode";
  private static final String INDEX_PATH_OPTION = "index";
  private static final String TARGET_INDEX_PATH_OPTION = "targetindex";
  private static final String XML_PATH_OPTION = "xml";
  private static final String COMPRESSED_PATH_OPTION = "compressed";
  private static final String TOKENIZER_CLASS_OPTION = "tokenizerclass";
  private static final String TOKENIZER_MODEL_OPTION = "tokenizermodel";
  private static final String COLLECTION_VOCAB_OPTION = "collectionvocab";
  private static final String LANGUAGE_OPTION = "lang";
  private static final String FVOCAB_F2E_OPTION = "f_f2e_vocab";
  private static final String EVOCAB_F2E_OPTION = "e_f2e_vocab";
  private static final String FVOCAB_E2F_OPTION = "f_e2f_vocab";
  private static final String EVOCAB_E2F_OPTION = "e_e2f_vocab";
  private static final String TTABLE_F2E_OPTION = "f2e_ttable";
  private static final String TTABLE_E2F_OPTION = "e2f_ttable";

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    options = new Options();
    options.addOption(OptionBuilder.withDescription("preprocessing mode").withArgName("mono|crosslingF|crosslingE").hasArg().isRequired().create(MODE_OPTION));
    options.addOption(OptionBuilder.withDescription("path to index directory").withArgName("path").hasArg().isRequired().create(INDEX_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to target index directory (if processing f-side)").withArgName("path").hasArg().create(TARGET_INDEX_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to XML file").withArgName("path").hasArg().isRequired().create(XML_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("path to compressed collection").withArgName("path").hasArg().isRequired().create(COMPRESSED_PATH_OPTION));
    options.addOption(OptionBuilder.withDescription("tokenizer class").withArgName("class").hasArg().create(TOKENIZER_CLASS_OPTION));
    options.addOption(OptionBuilder.withDescription("path to tokenizer model file/directory").withArgName("path").hasArg().create(TOKENIZER_MODEL_OPTION));
    options.addOption(OptionBuilder.withDescription("path to collection vocab file").withArgName("path").hasArg().create(COLLECTION_VOCAB_OPTION));
    options.addOption(OptionBuilder.withDescription("two-letter collection language code").withArgName("en|de|fr|zh|es|ar|tr").hasArg().isRequired().create(LANGUAGE_OPTION));
    options.addOption(OptionBuilder.withDescription("path to f-side vocab file of f-to-e translation table").withArgName("path").hasArg().create(FVOCAB_F2E_OPTION));
    options.addOption(OptionBuilder.withDescription("path to e-side vocab file of f-to-e translation table").withArgName("path").hasArg().create(EVOCAB_F2E_OPTION));
    options.addOption(OptionBuilder.withDescription("path to f-side vocab file of e-to-f translation table").withArgName("path").hasArg().create(FVOCAB_E2F_OPTION));
    options.addOption(OptionBuilder.withDescription("path to e-side vocab file of e-to-f translation table").withArgName("path").hasArg().create(EVOCAB_E2F_OPTION));
    options.addOption(OptionBuilder.withDescription("path to f-to-e translation table").withArgName("path").hasArg().create(TTABLE_F2E_OPTION));
    options.addOption(OptionBuilder.withDescription("path to e-to-f translation table").withArgName("path").hasArg().create(TTABLE_E2F_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    String m = cmdline.getOptionValue(MODE_OPTION);
    mode = m.equals("mono") ? MONO_LINGUAL : (m.equals("crosslingF") ? CROSS_LINGUAL_F : (m.equals("crosslingE")) ? CROSS_LINGUAL_E : -1); 
    if (mode < 0) throw new RuntimeException("Incorrect mode selection!");
    if (mode == CROSS_LINGUAL_F) {
      if (!options.hasOption(FVOCAB_F2E_OPTION) || !options.hasOption(FVOCAB_E2F_OPTION) || !options.hasOption(EVOCAB_F2E_OPTION) || !options.hasOption(EVOCAB_E2F_OPTION)
          || !options.hasOption(TTABLE_F2E_OPTION) || !options.hasOption(TTABLE_E2F_OPTION)) {
        System.err.println("Error, missing translation table arguments: " + FVOCAB_F2E_OPTION + "," + EVOCAB_F2E_OPTION + "," 
            + FVOCAB_E2F_OPTION + "," + EVOCAB_E2F_OPTION + "," + TTABLE_F2E_OPTION + "," + TTABLE_E2F_OPTION);
        return -1;
      }
    }
    if (mode == CROSS_LINGUAL_E) {
      if (!options.hasOption(COLLECTION_VOCAB_OPTION)) {
        System.err.println("Error, missing collection vocab argument: " + COLLECTION_VOCAB_OPTION);
        return -1;
      }
      if (options.hasOption(FVOCAB_F2E_OPTION) || options.hasOption(FVOCAB_E2F_OPTION) || options.hasOption(EVOCAB_F2E_OPTION) || options.hasOption(EVOCAB_E2F_OPTION)
          || options.hasOption(TTABLE_F2E_OPTION) || options.hasOption(TTABLE_E2F_OPTION)) {
        System.err.println("Warning, translation table arguments are ignored in this mode!");
      }
    }
    indexRootPath = cmdline.getOptionValue(INDEX_PATH_OPTION);
    targetIndexPath = cmdline.getOptionValue(TARGET_INDEX_PATH_OPTION);
    rawCollection = cmdline.getOptionValue(XML_PATH_OPTION);
    seqCollection = cmdline.getOptionValue(COMPRESSED_PATH_OPTION);
    tokenizerClass = cmdline.getOptionValue(TOKENIZER_CLASS_OPTION);
    tokenizerModel = cmdline.getOptionValue(TOKENIZER_MODEL_OPTION);
    collectionVocab = cmdline.getOptionValue(COLLECTION_VOCAB_OPTION);
    collectionLang = cmdline.getOptionValue(LANGUAGE_OPTION);
    fVocab_f2e = cmdline.getOptionValue(FVOCAB_F2E_OPTION);
    eVocab_f2e = cmdline.getOptionValue(EVOCAB_F2E_OPTION);
    fVocab_e2f = cmdline.getOptionValue(FVOCAB_E2F_OPTION);
    eVocab_e2f = cmdline.getOptionValue(EVOCAB_E2F_OPTION);
    ttable_f2e = cmdline.getOptionValue(TTABLE_F2E_OPTION);
    ttable_e2f = cmdline.getOptionValue(TTABLE_E2F_OPTION);

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PreprocessWikipedia(), args);
  }
}
