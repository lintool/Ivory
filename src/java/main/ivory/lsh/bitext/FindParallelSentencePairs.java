package ivory.lsh.bitext;

import ivory.core.RetrievalEnvironment;
import ivory.core.util.CLIRUtils;
import ivory.lsh.data.WikiDocInfo;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import opennlp.model.RealValueFileEventStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.map.HMapIV;

/**

 * @author ferhanture
 * 
 */
@SuppressWarnings("deprecation")
public class FindParallelSentencePairs extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(FindParallelSentencePairs.class);
	private static final int MinVectorTerms = 3, MinSentenceLength = 5, E=-1, F=1;

	enum Docs{
		pairsE, pairsF, pairs, pairsIncompleteF, pairsIncompleteE	
	}
	enum Sentences{
		pairsE, pairsF, pairsProcessed, pairsCandidate, pairsFilteredByVectorSize, pairsFilteredBySentRatio, parallel 
	}

	//AssertTrue
	//pairsCandidate=sum(pairsProcessed, pairsFilteredBySentRatio)

	//SanityCheck
	//pairsCandidate/Docs.pairsF = number of sentence pairs per doc pair 

	public FindParallelSentencePairs() {
	}

	private static int printUsage() {
		sLogger.info("usage: [cl-pwsim-output-path] [output-path] [e-path] [f-path] [e-dir] [f-dir] [vocab-dir] [e-lang] [f-lang] [classifier] [threshold] [classifier parallel-label id]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Candidate generation
	 * 
	 * Map: (docno, wikiPage) --> (<fDocno, eDocno>, <lang id,docno,vectors,sentences>)
	 * input is union of source and target collections
	 * 	Ê Ê Êsentences = extract sentences in wikiPage
	 * 		 vectors = convert sentence text into td-idf vector
	 * 	Ê Ê Êsimilar_pairs = from pwsim output, find if there's any pair corresponding to docno
	 * 	Ê Ê Êforeach similar_pair
	 * 		Ê Ê Ê emit(similar_pair, <lang id,docno,vectors,sentences>)
	 * 
	 * @author ferhanture
	 */
	private static class MyMapper extends MapReduceBase implements
	Mapper<Writable, Indexable, PairOfInts, WikiDocInfo> {

		HMapIV<ArrayListOfIntsWritable> pwsimMapping;		// mapping for pwsim pairs
		PairOfInts keyOut;
		JobConf mJob;
		WikiDocInfo valOut;
		PreprocessHelper helper;							// for modularity, helper provides methods to preprocess data

		public void configure(JobConf job) {
			sLogger.setLevel(Level.INFO);
			mJob = job;
			pwsimMapping = new HMapIV<ArrayListOfIntsWritable>();

			try {
				helper = new PreprocessHelper(MinVectorTerms, MinSentenceLength, job);
			} catch (Exception e) {
				e.printStackTrace();
			}

			keyOut = new PairOfInts();
			valOut = new WikiDocInfo();
		}

		/**
		 * if lang id points to foreign language, then load pwsim algorithm's output as mapping: {foreign docno N --> list<english docnos> associated with N}
		 * otherwise, mapping is like:  {english docno N --> list<foreign docnos> associated with N}
		 * 
		 * lang id is the same for every Map call of a given mapper, since input sequence files will be uniform in terms of language 
		 * (i.e., a mapper will receive either all foreign or all english documents)
		 * 
		 * @param pwsimMapping
		 * 		mapping from source (target) docno to list of target (source) docnos associated with it
		 * @param lang
		 * 		language identifier
		 * @param job
		 * 		job configuration object
		 * @param reporter
		 * 		reporter object for counters
		 */
		private static void loadPairs(HMapIV<ArrayListOfIntsWritable> pwsimMapping, String lang, JobConf job, Reporter reporter){
			try {
				Path[] localFiles = null;
				localFiles = DistributedCache.getLocalCacheFiles(job);

				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(job), localFiles[14], job);

				PairOfInts key = (PairOfInts) reader.getKeyClass().newInstance();
				IntWritable value = (IntWritable) reader.getValueClass().newInstance();

				while (reader.next(key, value)) {
					int fDocno = key.getRightElement();
					fDocno -= 1000000000; 
					int eDocno = key.getLeftElement();
					if(lang.equals("en")){
						if(!pwsimMapping.containsKey(eDocno)){
							pwsimMapping.put(eDocno, new ArrayListOfIntsWritable());
						}
						pwsimMapping.get(eDocno).add(fDocno);		// we add 1000000000 to foreign docnos to distinguish them during pwsim algo
					}else{
						if(!pwsimMapping.containsKey(fDocno)){
							pwsimMapping.put(fDocno, new ArrayListOfIntsWritable());
						}
						pwsimMapping.get(fDocno).add(eDocno);		// we add 1000000000 to foreign docnos to distinguish them during pwsim algo
					}
					key = (PairOfInts) reader.getKeyClass().newInstance();
					value = (IntWritable) reader.getValueClass().newInstance();
				}
				reader.close();
				sLogger.info(pwsimMapping.size()+" pairs loaded.");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public void map(Writable docnoKey, Indexable page, OutputCollector<PairOfInts, WikiDocInfo> output, Reporter reporter) throws IOException {
			int docno = ((IntWritable)docnoKey).get();
			WikipediaPage p = (WikipediaPage) page;
			String lang = p.getLanguage();
			ArrayListOfIntsWritable similarDocnos;

			// we only load the mapping once, during the first map() call of a mapper. 
			// this works b/c all input kv pairs of a given mapper will have same lang id (reason explained above)
			if(pwsimMapping.isEmpty()){
				loadPairs(pwsimMapping, lang, mJob, reporter);
				sLogger.debug(pwsimMapping.size());
			}
			
			// if no similar docs for docno, return
			if(pwsimMapping.containsKey(docno)){
				similarDocnos = pwsimMapping.get(docno); 	
			}else{
				return;
			}

			ArrayListWritable<Text> sentences;
			ArrayListWritable<HMapSFW> vectors;
			ArrayListOfIntsWritable sentLengths = new ArrayListOfIntsWritable();
			try {
				if(lang.equals("en")){
					// identify sentences in document, filter out ones below MinSentLength threshold
					sentences = helper.getESentences(p.getContent(), sentLengths);		
					
					// convert each sentence into a tf-idf vector, using general DF map for collection and a heuristic for avg. doc length
					// filter out sentences for which the vector has less than MinVectorTerms terms
					vectors = helper.createEDocVectors(sentences);

					// a hack so that we sync the sentence list after vectors are filtered by #terms
					sentences = helper.getTempSentences();		
				}else{
					sentences = helper.getFSentences(p.getContent(), sentLengths);
					vectors = helper.createFDocVectors(sentences);
					sentences = helper.getTempSentences();
				}
				if(sentences.size() != vectors.size()) {
					throw new RuntimeException("Sentences.size != Vectors.size");
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}

			for(int similarDocno : similarDocnos){	
				if(lang.equals("en")){
					keyOut.set(similarDocno, docno);
					valOut.set(E, vectors, sentences);

					reporter.incrCounter(Docs.pairsE, 1);
					reporter.incrCounter(Sentences.pairsE, vectors.size());
				}else{
					keyOut.set(docno, similarDocno);		
					valOut.set(F, vectors, sentences);

					reporter.incrCounter(Docs.pairsF, 1);
					reporter.incrCounter(Sentences.pairsF, vectors.size());
				}
				output.collect(keyOut, valOut);
			}
		}
	}

	/**
	 * Bilingual sentence pair detection
	 * 
	 * Reduce: (<fDocno, eDocno>, [ <E,eDocno,eVectors,eSentences>,  <F,fDocno,fVectors,fSentences>]) --> (fSentence, eSentence)
	 * 			
	 * @author ferhanture
	 *
	 */
	private static class MyReducer extends MapReduceBase implements
	Reducer<PairOfInts, WikiDocInfo, Text, Text>{
		int fDocno, eDocno;
		int classifierPositiveId;
		ArrayListWritable<HMapSFW> fVectors, eVectors;
		ArrayListWritable<Text> fSentences, eSentences;
		PreprocessHelper helper;						// for modularity, helper provides methods to preprocess data
		float classifierThreshold;
		Text emptyValue = new Text();

		public void configure(JobConf job) {
			sLogger.setLevel(Level.INFO);

			try {
				helper = new PreprocessHelper(MinVectorTerms, MinSentenceLength, job);
			} catch (Exception e) {
				e.printStackTrace();
			}
			classifierPositiveId = job.getInt("ClassifierId", -1);
			if(classifierPositiveId != 0 && classifierPositiveId != 1){
				throw new RuntimeException("Id of parallel label in MaxEnt classifier not specified properly: "+classifierPositiveId);
			}

			classifierThreshold = job.getFloat("ClassifierThreshold", 2);
			if (classifierThreshold > 1f) {
				throw new RuntimeException("Classifier confidence threshold > 1, provide value in [0,1]: "+classifierThreshold);				
			}
			
			eVectors = new ArrayListWritable<HMapSFW>();
			fVectors = new ArrayListWritable<HMapSFW>();
			eSentences = new ArrayListWritable<Text>();
			fSentences = new ArrayListWritable<Text>();
		}

		public void reduce(PairOfInts docnoPair, Iterator<WikiDocInfo> wikiTexts,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			eVectors.clear();
			fVectors.clear();
			eSentences.clear();
			fSentences.clear();

			fDocno = docnoPair.getLeftElement();
			eDocno = docnoPair.getRightElement();

			// parse WikiDocInfo object into sentences and vectors, based on the language id
			WikiDocInfo page;
			int eCnt = 0, fCnt = 0;
			while (wikiTexts.hasNext() && (eCnt < 1 || fCnt < 1)) {
				page = wikiTexts.next();
				if(page.getLangID() == F && fVectors.isEmpty()){
					fCnt++;
					fVectors = page.getVectors();
					fSentences = page.getSentences();
					reporter.incrCounter(Sentences.pairsF, fVectors.size());
				}else if(page.getLangID() == E && eVectors.isEmpty()){
					eCnt++;
					eVectors = page.getVectors();
					eSentences = page.getSentences();
					reporter.incrCounter(Sentences.pairsE, eVectors.size());
				}
			}

			/**
			 * @TODO look into exact cause of this...
			 *  
			 * if the input collection has differences from the pwsim output, 
			 * we may not find the actual wiki page corresponding to a similar pair of docnos
			 */
			if((eCnt < 1 || fCnt < 1)){
				sLogger.debug("Read "+eCnt+","+fCnt+" pages: ="+eDocno+","+fDocno);
				if(fVectors.isEmpty()){
					reporter.incrCounter(Docs.pairsIncompleteF, 1);
				}else{
					reporter.incrCounter(Docs.pairsIncompleteE, 1);
				}
				return;
			}

			reporter.incrCounter(Docs.pairs, 1);
			
			// if either document has no vectors, no need to continue
			if(fVectors.size()==0 || eVectors.size()==0){
				return;
			}

			// counters for debug purposes only
			reporter.incrCounter(Sentences.pairsCandidate, fVectors.size()*eVectors.size());
			int numProcessed = 0;
			long time = 0;
			
			// classify each e-f sentence pair in the candidate set
			for (int f = 0; f < fVectors.size(); f++) {
				HMapSFW fVector = fVectors.get(f);
				int fSentLength = fSentences.get(f).getLength();

				for (int e = 0; e < eVectors.size(); e++) {
					HMapSFW eVector = eVectors.get(e);
					int eSentLength = eSentences.get(e).getLength();

					if (eSentLength > 2 * fSentLength || fSentLength > 2 * eSentLength) {
						reporter.incrCounter(Sentences.pairsFilteredBySentRatio, 1);
						continue;
					}

					reporter.incrCounter(Sentences.pairsProcessed, 1);				
					numProcessed++;
					
					// compute features
					long start = System.currentTimeMillis();
					String[] instance = CLIRUtils.computeFeaturesF1(eVector, fVector, eSentLength, fSentLength);
					time += (System.currentTimeMillis()-start);

					// classify w/ maxent model
					// emit if labeled parallel
					if(instance == null){
						throw new RuntimeException("SHOULD NOT HAPPEN!");
					}

					// apply MaxEnt classifier to instance
					float[] values = RealValueFileEventStream.parseContexts(instance);
					double[] probs = helper.getClassifier().eval(instance, values);

					// check if confidence above specified threshold
					double confidence = probs[classifierPositiveId];
					if(confidence>classifierThreshold){
						reporter.incrCounter(Sentences.parallel, 1);
						output.collect(new Text(fSentences.get(f)+"<GERMAN2ENGLISH>"+eSentences.get(e)), emptyValue);
					}
				}
			}
//			sLogger.info("Finished processing "+numProcessed+" out of "+fVectors.size()*eVectors.size()+", avg process time="+time/(1f*numProcessed)+" avg map time="+(System.currentTimeMillis()-mapStartTime)/(1f*numProcessed));
		}
	}

	/**
	 * Runs this tool.
	 */

	public int run(String[] args) throws Exception {
		if (args.length != 12) {
			printUsage();
			return -1;
		}
		JobConf conf = new JobConf(getConf(), FindParallelSentencePairs.class);

		// Read commandline argument
		String pwsimPairsPath = args[0];
		String outputPath = args[1];

		String eCollectionPath = args[2];
		String fCollectionPath = args[3];

		String eDir = args[4];
		String fDir = args[5];

		RetrievalEnvironment eEnv = new RetrievalEnvironment(eDir, FileSystem.get(conf));

		String vocabDir = args[6];
		String eLang = args[7];
		String fLang = args[8];
		String classifierFile = args[9];

		float classifierThreshold = Float.parseFloat(args[10]);
		int classifierId = Integer.parseInt(args[11]);

		conf.setJobName("FindParallelSentences_" + fLang +"-" + eLang +"_F1="+classifierThreshold+"["+classifierId+"]");

		String eSentDetect = vocabDir+"/"+eLang+"-sent.bin";
		String eTokenizer = vocabDir+"/"+eLang+"-token.bin";
		String eVocabSrc = vocabDir+"/vocab."+eLang+"-"+fLang+"."+eLang;
		String eVocabTrg = vocabDir+"/vocab."+fLang+"-"+eLang+"."+eLang;

		String fSentDetect = vocabDir+"/"+fLang+"-sent.bin";
		String fTokenizer = vocabDir+"/"+fLang+"-token.bin";
		String fVocabSrc = vocabDir+"/vocab."+fLang+"-"+eLang+"."+fLang;
		String fVocabTrg = vocabDir+"/vocab."+eLang+"-"+fLang+"."+fLang;

		String f2e_ttableFile = vocabDir+"/ttable."+fLang+"-"+eLang;
		String e2f_ttableFile = vocabDir+"/ttable."+eLang+"-"+fLang;

		int numReducers = 50;

		conf.set("eDir", eDir);
		conf.set("fDir", fDir);
		conf.set("eLang", eLang);
		conf.set("fLang", fLang);
		conf.setInt("NumReducers", numReducers);
		conf.setFloat("ClassifierThreshold", classifierThreshold);
		conf.setInt("ClassifierId", classifierId);

		sLogger.info("caching files...");

		//e-files

		sLogger.info("caching files...0,1,2,3,4");

		DistributedCache.addCacheFile(new URI(eEnv.getDfByTermData()), conf);
		DistributedCache.addCacheFile(new URI(eSentDetect), conf);
		DistributedCache.addCacheFile(new URI(eTokenizer), conf);
		DistributedCache.addCacheFile(new URI(eVocabSrc), conf);
		DistributedCache.addCacheFile(new URI(eVocabTrg), conf);

		//f-files
		
		sLogger.info("caching files...5,6,7,8,9");

		DistributedCache.addCacheFile(new URI(fDir+"/transDf.dat"), conf);
		DistributedCache.addCacheFile(new URI(fSentDetect), conf);
		DistributedCache.addCacheFile(new URI(fTokenizer), conf);
		DistributedCache.addCacheFile(new URI(fVocabSrc), conf);
		DistributedCache.addCacheFile(new URI(fVocabTrg), conf);

		/////cross-lang files

		sLogger.info("caching files...10,11,12,13,14");

		DistributedCache.addCacheFile(new URI(f2e_ttableFile), conf);
		DistributedCache.addCacheFile(new URI(e2f_ttableFile), conf);
		DistributedCache.addCacheFile(new URI(eEnv.getIndexTermsData()), conf);
		DistributedCache.addCacheFile(new URI(classifierFile), conf);
		DistributedCache.addCacheFile(new URI(pwsimPairsPath), conf);

		FileInputFormat.addInputPaths(conf, eCollectionPath);
		FileInputFormat.addInputPaths(conf, fCollectionPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInt("mapred.task.timeout", 60000000);
		conf.set("mapred.child.java.opts", "-Xmx2000m");
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		conf.setNumMapTasks(100);
		conf.setNumReduceTasks(numReducers);
		conf.setInt("mapred.min.split.size", 2000000000);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputKeyClass(PairOfInts.class);
		conf.setMapOutputValueClass(WikiDocInfo.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);	

		return 0;
	}


	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FindParallelSentencePairs(), args);
		System.exit(res);
	}

}


