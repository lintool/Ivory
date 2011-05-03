/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package ivory.driver;

import ivory.preprocess.BuildIntDocVectors;
import ivory.preprocess.BuildTargetLangWeightedIntDocVectors;
import ivory.preprocess.BuildTermDocVectors;
import ivory.preprocess.BuildTermIdMap;
import ivory.preprocess.BuildTranslatedTermDocVectors;
import ivory.preprocess.BuildWeightedIntDocVectors;
import ivory.preprocess.BuildWeightedTermDocVectors;
import ivory.preprocess.GetTermCount;
import ivory.util.CLIRUtils;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.hooka.Vocab;
import edu.umd.cloud9.collection.wikipedia.BuildWikipediaDocnoMapping;
import edu.umd.cloud9.collection.wikipedia.RepackWikipedia;

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
	static final  int MONO_LINGUAL = 4, CROSS_LINGUAL_E = 7, CROSS_LINGUAL_F =12;

	private static int printUsage() {
		System.out.println("\nThis program can be run in three different \"modes\":\n=====================\nInput: English Wikipedia collection\nOutput: English weighted document vectors" +
				"\nusage: [index-path] [raw-path] [compressed-path] [tokenizer-class]" +
				"\n\nInput: English side of cross-lingual Wikipedia collection\nOutput: English weighted document vectors (comparable with the document vectors generated from non-English side)" +
				"\nusage: [index-path] [raw-path] [compressed-path] [tokenizer-class] [collection-lang] [tokenizer-model] [collection-vocab]" +
				"\n\nInput: Non-English side of cross-lingual Wikipedia collection\nOutput: English weighted document vectors (comparable with the document vectors generated from English side)" +
		"\nusage: [index-path] [raw-path] [compressed-path] [tokenizer-class] [collection-lang] [tokenizer-model] [src-vocab_f] [src-vocab_e] [prob-table_f-->e] [src-vocab_e] [trg-vocab_f] [prob-table_e-->f])");
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		int mode = args.length;
		if (mode != MONO_LINGUAL && mode != CROSS_LINGUAL_E && mode != CROSS_LINGUAL_F){
			printUsage();
			return -1;
		}

		String indexRootPath = args[0];
		String rawCollection = args[1]; 	//"/shared/Wikipedia/raw/dewiki-20100117-pages-articles.xml";
		String seqCollection = args[2]; 	//"/umd-lin/fture/pwsim/de-wikipedia/compressed.block/de-20100117";
		String tokenizerClass = args[3];	

		Configuration conf = new Configuration();

		String collectionLang = null, tokenizerModel = null, collectionVocab = null;
		String fVocab_f2e = null, eVocab_f2e = null, fVocab_e2f, eVocab_e2f = null, ttable_f2e = null, ttable_e2f = null;
		if(mode == CROSS_LINGUAL_E || mode == CROSS_LINGUAL_F){		// CROSS-LINGUAL CASE
			collectionLang = args[4];
			tokenizerModel = args[5];
			collectionVocab = args[6];
			conf.set("Ivory.Lang", collectionLang);
			conf.set("Ivory.TokenizerModel", tokenizerModel);
			conf.set("Ivory.CollectionVocab", collectionVocab);
			conf.set("Ivory.FinalVocab", collectionVocab);

			if(mode == CROSS_LINGUAL_F){			// non-English side, needs to be translated
				fVocab_f2e = args[6];		//  same as collection vocab
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
		LOG.info(" - Tokenizer class: " + tokenizerClass);
		LOG.info(" - Minimum # terms per article : " + MinNumTermsPerArticle);

		if(mode == CROSS_LINGUAL_E || mode == CROSS_LINGUAL_F){
			LOG.info("Cross-lingual collection : Preprocessing "+collectionLang+" side.");
			LOG.info(" - Collection vocab file: " + collectionVocab);
			LOG.info(" - Tokenizer model: " + tokenizerModel);

			if(mode == CROSS_LINGUAL_F){
				LOG.info(" - TTable file "+collectionLang+" --> English : " + ttable_f2e);
				LOG.info(" - Source vocab file: " + fVocab_f2e);
				LOG.info(" - Target vocab file: " + eVocab_f2e);
				LOG.info(" - TTable file "+"English --> "+collectionLang+" : " + ttable_e2f);
				LOG.info(" - Source vocab file: " + fVocab_f2e);
				LOG.info(" - Target vocab file: " + eVocab_f2e);
			}
		}
		LOG.info("Launching with " + numMappers + " mappers, " + numReducers + " reducers...");

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
			String[] arr = new String[] { rawCollection, indexRootPath + "/wiki-docid-tmp",
					mappingFile.toString(), new Integer(numMappers).toString() };
			BuildWikipediaDocnoMapping tool = new BuildWikipediaDocnoMapping();
			tool.setConf(conf);
			tool.run(arr);

			fs.delete(new Path(indexRootPath + "/wiki-docid-tmp"), true);
		}else{
			LOG.info(p+" exists");
		}

		// Repack Wikipedia into sequential compressed block
		p = new Path(seqCollection);
		if (!fs.exists(p)) {
			LOG.info(seqCollection + " doesn't exist, creating...");
			String[] arr = new String[] { rawCollection, seqCollection, mappingFile.toString(), "block"};
			RepackWikipedia tool = new RepackWikipedia();
			tool.setConf(conf);
			tool.run(arr);
		}

		conf.set("Ivory.CollectionName", "Wikipedia-"+collectionLang);
		conf.setInt("Ivory.NumMapTasks", numMappers);
		conf.setInt("Ivory.NumReduceTasks", numReducers);
		conf.set("Ivory.CollectionPath", seqCollection);
		conf.set("Ivory.IndexPath", indexRootPath);
		conf.set("Ivory.InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat");
		conf.set("Ivory.DocnoMappingClass", "edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMapping");
		conf.set("Ivory.Tokenizer", tokenizerClass);			//"ivory.tokenize.OpenNLPTokenizer"
		conf.setInt("Ivory.MinDf", MinDF);
		conf.setInt("Ivory.MaxDf", Integer.MAX_VALUE);

		// Builds term doc vectors from document collection, and filters the terms that are not included in Ivory.SrcVocab
		long startTime = System.currentTimeMillis();	
		long preprocessStartTime = System.currentTimeMillis();	
		LOG.info("Building term doc vectors...");
		BuildTermDocVectors termDocVectorsTool = new BuildTermDocVectors(conf);
		termDocVectorsTool.run();
		LOG.info("Job finished in "+(System.currentTimeMillis()-startTime)/1000.0+" seconds");

		// Get CF and DF counts
		startTime = System.currentTimeMillis();
		LOG.info("Counting terms...");
		GetTermCount termCountWithDfAndCfTool = new GetTermCount(conf);
		termCountWithDfAndCfTool.run();
		LOG.info("TermCount = "+env.readCollectionTermCount()+"\nJob finished in "+(System.currentTimeMillis()-startTime)/1000.0+" seconds");

		// Build a map from terms to sequentially generated integer term ids
		startTime = System.currentTimeMillis();
		conf.setInt("Ivory.TermIndexWindow", TermIndexWindow);
		LOG.info("Building term-to-integer id mapping...");
		BuildTermIdMap termIDsDfCfTool = new BuildTermIdMap(conf);
		termIDsDfCfTool.run();
		LOG.info("Job finished in "+(System.currentTimeMillis()-startTime)/1000.0+" seconds");

		// Compute term weights, and output weighted term doc vectors
		startTime = System.currentTimeMillis();
		LOG.info("Building weighted term doc vectors...");
		conf.set("Ivory.ScoringModel", "ivory.pwsim.score.Bm25");
		if(mode == CROSS_LINGUAL_F){
			conf.setInt("Ivory.MinNumTerms",MinNumTermsPerArticle);

			// translate term doc vectors into English. 
			conf.setBoolean("Ivory.Normalize", false);
			BuildTranslatedTermDocVectors weightedTermVectorsTool = new BuildTranslatedTermDocVectors(conf);
			weightedTermVectorsTool.run();
		}else{						
			conf.setInt("Ivory.MinNumTerms",MinNumTermsPerArticle);

			// get weighted term doc vectors
			conf.setBoolean("Ivory.Normalize", false);
			BuildWeightedTermDocVectors weightedTermVectorsTool = new BuildWeightedTermDocVectors(conf);
			weightedTermVectorsTool.run();
		}
		LOG.info("Job finished in "+(System.currentTimeMillis()-startTime)/1000.0+" seconds");

		// normalize (optional) and convert weighted term doc vectors into int doc vectors for efficiency
		startTime = System.currentTimeMillis();
		LOG.info("Building weighted integer doc vectors...");
		conf.setBoolean("Ivory.Normalize", IsNormalized);
		if(mode == MONO_LINGUAL){
			new BuildIntDocVectors(conf).run();
			new BuildWeightedIntDocVectors(conf).run();
			LOG.info("Job BuildWeightedIntDocVectors finished in "+(System.currentTimeMillis()-startTime)/1000.0+" seconds");
		}else{
			BuildTargetLangWeightedIntDocVectors weightedIntVectorsTool = new BuildTargetLangWeightedIntDocVectors(conf);
			LOG.info("Job BuildTargetLangWeightedIntDocVectors finished in "+(System.currentTimeMillis()-startTime)/1000.0+" seconds");

			int finalNumDocs = weightedIntVectorsTool.run();
			if(finalNumDocs > 0){
				LOG.info("Changed doc count from "+env.readCollectionDocumentCount() + " to = "+finalNumDocs);
				env.writeCollectionDocumentCount(finalNumDocs);
			}
			// set Property.CollectionTermCount to the size of the target vocab. since all docs are translated into that vocab. This property is read by WriteRandomVectors via RunComputeSignatures.
			Vocab engVocabH = null;
			try {
				engVocabH = CLIRUtils.loadVocab(new Path(conf.get("Ivory.FinalVocab")), conf);
			} catch (IOException e) {
				e.printStackTrace();
			}	
			LOG.info("Changed term count to : "+env.readCollectionTermCount() + " = " + engVocabH.size());
			env.writeCollectionTermCount(engVocabH.size());
		}
		
		LOG.info("Preprocessing job finished in "+(System.currentTimeMillis()-preprocessStartTime)/1000.0+" seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PreprocessWikipedia(), args);
		System.exit(res);
	}
}
