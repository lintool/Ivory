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
import ivory.core.preprocess.BuildTargetLangWeightedIntDocVectors;
import ivory.core.preprocess.BuildTermDocVectors;
import ivory.core.preprocess.BuildTranslatedTermDocVectors;
import ivory.core.preprocess.BuildWeightedIntDocVectors;
import ivory.core.preprocess.BuildWeightedTermDocVectors;
import ivory.core.preprocess.ComputeGlobalTermStatistics;

import java.io.IOException;
import java.util.Arrays;

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
  static final int MinDF = 2, MinNumTermsPerArticle = 5, TermIndexWindow = 8;
  static final boolean IsNormalized = true;
  static final int NUM_MONO = 4, NUM_CROSS_E = 7, NUM_CROSS_F = 12;
  static int MONO_LINGUAL = 0, CROSS_LINGUAL_E = 1, CROSS_LINGUAL_F = 2;

  private static int printUsage() {
    System.out.println("\nThis program can be run in three different \"modes\":\n=====================\nInput: English Wikipedia collection\nOutput: English weighted document vectors" +
        "\nusage: [index-path] [raw-path] [compressed-path] [tokenizer-class]" +
        "\n\nInput: English side of cross-lingual Wikipedia collection\nOutput: English weighted document vectors (comparable with the document vectors generated from non-English side)" +
        "\nusage: [index-path] [raw-path] [compressed-path] [tokenizer-class] [collection-lang] [tokenizer-model] [collection-vocab]" +
        "\n\nInput: Non-English side of cross-lingual Wikipedia collection\nOutput: English weighted document vectors (comparable with the document vectors generated from English side)" +
    "\nusage: [index-path] [raw-path] [compressed-path] [tokenizer-class] [collection-lang] [tokenizer-model] [src-vocab_f] [trg-vocab_e] [prob-table_f-->e] [src-vocab_e] [trg-vocab_f] [prob-table_e-->f])");
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    int numArgs = args.length;
    int mode;
    if (numArgs >= NUM_MONO && numArgs < NUM_CROSS_E) {
      mode = MONO_LINGUAL;
      LOG.info("Mode: monolingual");
    } else if (numArgs == NUM_CROSS_E) {
      mode = CROSS_LINGUAL_E;
      LOG.info("Mode: crosslingual - English side");
    } else if (numArgs == NUM_CROSS_F) {
      mode = CROSS_LINGUAL_F;
      LOG.info("Mode: crosslingual - nonEnglish side");
    } else {
      printUsage();
      return -1;
    }
    Configuration conf = getConf();

    String collectionLang = null, tokenizerModel = null, collectionVocab = null,
    fVocab_f2e = null, eVocab_f2e = null, fVocab_e2f, eVocab_e2f = null, ttable_f2e = null, ttable_e2f = null;
    String indexRootPath = args[0];
    String rawCollection = args[1];
    String seqCollection = args[2];
    String tokenizerClass = args[3];	
    if (args.length > 4) {
      collectionLang = args[4];
      conf.set(Constants.Language, collectionLang);
      if (args.length > 5) {
        tokenizerModel = args[5];
        conf.set(Constants.TokenizerData, tokenizerModel);
      }
    }

    if (mode == CROSS_LINGUAL_E || mode == CROSS_LINGUAL_F) {		// CROSS-LINGUAL CASE
      collectionLang = args[4];
      tokenizerModel = args[5];
      collectionVocab = args[6];
      conf.set(Constants.Language, collectionLang);
      conf.set(Constants.TokenizerData, tokenizerModel);
      conf.set("Ivory.CollectionVocab", collectionVocab);
      conf.set("Ivory.FinalVocab", collectionVocab);

      if (mode == CROSS_LINGUAL_F) {			// non-English side, needs to be translated
        fVocab_f2e = args[6];		//  this is the collection vocab
        eVocab_f2e = args[7];
        ttable_f2e = args[8];
        eVocab_e2f = args[9];
        fVocab_e2f = args[10];
        ttable_e2f = args[11];

        conf.set("Ivory.F_Vocab_F2E", fVocab_f2e);	
        conf.set("Ivory.E_Vocab_F2E", eVocab_f2e);
        conf.set("Ivory.TTable_F2E", ttable_f2e);
        conf.set("Ivory.E_Vocab_E2F", eVocab_e2f);	
        conf.set("Ivory.F_Vocab_E2F", fVocab_e2f);	
        conf.set("Ivory.TTable_E2F", ttable_e2f);
        conf.set("Ivory.FinalVocab", eVocab_e2f);
      }
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
        LOG.info(" - TTable file "+collectionLang+" --> English : " + ttable_f2e);
        LOG.info(" - Source vocab file: " + fVocab_f2e);
        LOG.info(" - Target vocab file: " + eVocab_f2e);
        LOG.info(" - TTable file "+"English --> "+collectionLang+" : " + ttable_e2f);
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
    if (!fs.exists(p)) {
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
    }

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
    new BuildTermDocVectors(conf).run();
    LOG.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Get CF and DF counts.
    startTime = System.currentTimeMillis();
    LOG.info("Counting terms...");
    new ComputeGlobalTermStatistics(conf).run();
    LOG.info("TermCount = " + env.readCollectionTermCount());
    LOG.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Build a map from terms to sequentially generated integer term ids.
    startTime = System.currentTimeMillis();
    LOG.info("Building term-to-integer id mapping...");
    new BuildDictionary(conf).run();
    LOG.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Compute term weights, and output weighted term doc vectors.
    LOG.info("Building weighted term doc vectors...");
    startTime = System.currentTimeMillis();

    conf.set("Ivory.ScoringModel", "ivory.pwsim.score.Bm25");
    conf.setBoolean("Ivory.Normalize", IsNormalized);
    conf.setInt("Ivory.MinNumTerms",MinNumTermsPerArticle);

    if (mode == CROSS_LINGUAL_F) {
      // Translate term doc vectors into English.
      new BuildTranslatedTermDocVectors(conf).run();
    } else {
      // Build weighted term doc vectors.
      new BuildWeightedTermDocVectors(conf).run();
    }
    LOG.info("Job finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // normalize (optional) and convert weighted term doc vectors into int doc vectors for efficiency
    startTime = System.currentTimeMillis();
    LOG.info("Building weighted integer doc vectors...");
    conf.setBoolean("Ivory.Normalize", IsNormalized);
    if (mode == MONO_LINGUAL) {
      new BuildIntDocVectors(conf).run();
      new BuildWeightedIntDocVectors(conf).run();
      LOG.info("Job BuildWeightedIntDocVectors finished in " +
          (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    } else {
      BuildTargetLangWeightedIntDocVectors weightedIntVectorsTool =
        new BuildTargetLangWeightedIntDocVectors(conf);

      int finalNumDocs = weightedIntVectorsTool.run();

      LOG.info("Job BuildTargetLangWeightedIntDocVectors finished in " +
          (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
      if (finalNumDocs > 0) {
        LOG.info("Changed doc count from " + env.readCollectionDocumentCount() +
            " to = " + finalNumDocs);
        env.writeCollectionDocumentCount(finalNumDocs);
      }
      // set Property.CollectionTermCount to the size of the target vocab. since all docs are translated into that vocab. This property is read by WriteRandomVectors via RunComputeSignatures.
      Vocab engVocabH = null;
      try {
        engVocabH = HadoopAlign.loadVocab(new Path(conf.get("Ivory.FinalVocab")), conf);
      } catch (IOException e) {
        e.printStackTrace();
      }	
      LOG.info("Changed term count to : "+env.readCollectionTermCount() + " = " + engVocabH.size());
      env.writeCollectionTermCount(engVocabH.size());
    }

    LOG.info("Preprocessing job finished in " +
        (System.currentTimeMillis() - preprocessStartTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new PreprocessWikipedia(), args);
  }
}
