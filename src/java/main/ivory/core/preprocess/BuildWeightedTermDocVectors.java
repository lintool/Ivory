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

package ivory.core.preprocess;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.document.LazyTermDocVector;
import ivory.core.data.document.TermDocVector;
import ivory.core.data.stat.DfTableArray;
import ivory.core.data.stat.DocLengthTable;
import ivory.core.data.stat.DocLengthTable2B;
import ivory.core.data.stat.DocLengthTable4B;
import ivory.core.data.stat.PrefixEncodedGlobalStats;
import ivory.core.data.stat.PrefixEncodedGlobalStatsWithIndex;
import ivory.core.util.CLIRUtils;
import ivory.pwsim.score.ScoringModel;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.hooka.Vocab;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.MapKF;

public class BuildWeightedTermDocVectors extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(BuildWeightedTermDocVectors.class);

	static{
		sLogger.setLevel(Level.WARN);
	}
	protected static enum Docs{
		Total, ZERO, SHORT
	}
	private static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, LazyTermDocVector, IntWritable, HMapSFW> {

		static IntWritable mDocno = new IntWritable();
		private static DocLengthTable mDLTable;
		private static ScoringModel mScoreFn;
		int MIN_SIZE = 0;

		boolean shortDocLengths = false; 
		private boolean normalize = false;
		DefaultFrequencySortedDictionary dict;
		DfTableArray dfTable; 
		
		public void configure(JobConf conf){
			normalize = conf.getBoolean("Ivory.Normalize", false);
			shortDocLengths = conf.getBoolean("Ivory.ShortDocLengths", false);
			MIN_SIZE = conf.getInt("Ivory.MinNumTerms", 0);

			Path[] localFiles;
			try {
				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (conf.get ("mapred.job.tracker").equals ("local")) {
					FileSystem fs = FileSystem.get (conf);
					//sLogger.info ("fs: " + fs);
					String indexPath = conf.get ("Ivory.IndexPath");
					//sLogger.info ("indexPath: " + indexPath);
					RetrievalEnvironment env = new RetrievalEnvironment (indexPath, fs);
					//sLogger.info ("env: " + env);
					localFiles = new Path [5];
					localFiles [0] = new Path (env.getIndexTermsData ());
					localFiles [1] = new Path (env.getIndexTermIdsData ());
					localFiles [2] = new Path (env.getIndexTermIdMappingData ());
					localFiles [3] = new Path (env.getDfByTermData ());
					localFiles [4] = env.getDoclengthsData ();

				} else {
					localFiles = DistributedCache.getLocalCacheFiles (conf);
				}
			} catch (IOException e2) {
				throw new RuntimeException ("Local cache files not read properly.");
			}

			try{
				dict = new DefaultFrequencySortedDictionary(localFiles[0], localFiles[1], localFiles[2], FileSystem.getLocal(conf));
				dfTable = new DfTableArray(localFiles[3], FileSystem.getLocal(conf));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error loading Terms File for dictionary from "+localFiles[0]);
			}

			sLogger.info("Global Stats table loaded successfully.");

			try {
				if(shortDocLengths)
					mDLTable = new DocLengthTable2B(localFiles[4], FileSystem.getLocal(conf));
				else 
					mDLTable = new DocLengthTable4B(localFiles[4], FileSystem.getLocal(conf));
			} catch (IOException e1) {
				throw new RuntimeException("Error loading dl table from "+localFiles[4]);
			}	
			try {
				mScoreFn = (ScoringModel) Class.forName(conf.get("Ivory.ScoringModel")).newInstance();

				// this only needs to be set once for the entire collection
				mScoreFn.setDocCount(mDLTable.getDocCount());
				mScoreFn.setAvgDocLength(mDLTable.getAvgDocLength());
			} catch (Exception e) {
				throw new RuntimeException("Error initializing Ivory.ScoringModel from "+conf.get("Ivory.ScoringModel"));
			}	
		}

		HMapSFW weightedVector = new HMapSFW();

		String term;
		float wt, sum2;
		public void map(IntWritable docno, LazyTermDocVector doc,
				OutputCollector<IntWritable, HMapSFW> output, Reporter reporter)
		throws IOException {
			mDocno.set(docno.get());
			int docLen = mDLTable.getDocLength(mDocno.get());

			weightedVector.clear();
			TermDocVector.Reader r = doc.getReader();

			sLogger.debug("===================================BEGIN READ DOC");
			sum2 = 0;
			while(r.hasMoreTerms()){
				term = r.nextTerm();
				int id = dict.getId(term); 
				if(id != -1){
					int df = dfTable.getDf(id);
//				}
//				if(df>=1){
					sLogger.debug(term);
					mScoreFn.setDF(df);
					wt = mScoreFn.computeDocumentWeight(r.getTf(), docLen);
					weightedVector.put(term, wt);
					sum2 += wt * wt;
				}else{
					sLogger.debug("skipping term "+term+" (not in dictionary)");
				}
			}
			sLogger.debug("===================================END READ DOC");
			if(normalize){
				/*length-normalize doc vectors*/
				sum2 = (float) Math.sqrt(sum2);
				for(MapKF.Entry<String> e : weightedVector.entrySet()){
					float score = weightedVector.get(e.getKey());
					weightedVector.put(e.getKey(), score/sum2);
				}
			}
			if(weightedVector.size()==0){
				reporter.incrCounter(Docs.ZERO, 1);
			}else if(weightedVector.size()<MIN_SIZE){
				reporter.incrCounter(Docs.SHORT, 1);
			}
			else{
				output.collect(mDocno, weightedVector);
				reporter.incrCounter(Docs.Total, 1);
			}
		}
	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
		"Ivory.IndexPath", 
		//"Ivory.OutputPath",
		"Ivory.ScoringModel",
		"Ivory.Normalize",
	};

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildWeightedTermDocVectors(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("deprecation")
	public int runTool() throws Exception {
		sLogger.info("PowerTool: GetWeightedTermDocVectors");

		JobConf conf = new JobConf(BuildWeightedTermDocVectors.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = getConf().get("Ivory.IndexPath");
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
		String outputPath = env.getWeightedTermDocVectorsDirectory();
		int mapTasks = getConf().getInt("Ivory.NumMapTasks", 0);
		int minSplitSize = getConf().getInt("Ivory.MinSplitSize", 0);
		String collectionName = getConf().get("Ivory.CollectionName");

		String termsFilePath = env.getIndexTermsData();
		String termsIdsFilePath = env.getIndexTermIdsData();
		String termIdMappingFilePath = env.getIndexTermIdMappingData();
		String dfByTermFilePath = env.getDfByTermData();
		
		Path inputPath = new Path(env.getTermDocVectorsDirectory());
		Path weightedVectorsPath = new Path(outputPath);

		if (fs.exists(weightedVectorsPath)) {
			//fs.delete(weightedVectorsPath, true);
			sLogger.info("Output path already exists!");
			return 0;
		}

		/* add terms file to cache */
		if (!fs.exists(new Path(termsFilePath)) || !fs.exists(new Path(termsIdsFilePath)) || !fs.exists(new Path(termIdMappingFilePath))) {
			throw new RuntimeException("Error, terms file " + termsFilePath + "/" + termsIdsFilePath + "/" + termIdMappingFilePath + "doesn't exist!");
		}
		DistributedCache.addCacheFile(new URI(termsFilePath), conf);
		DistributedCache.addCacheFile(new URI(termsIdsFilePath), conf);
		DistributedCache.addCacheFile(new URI(termIdMappingFilePath), conf);

		/* add df table to cache */
		if (!fs.exists(new Path(dfByTermFilePath))) {
			throw new RuntimeException("Error, df data file " + dfByTermFilePath + "doesn't exist!");
		}
		DistributedCache.addCacheFile(new URI(dfByTermFilePath), conf);

		/* add dl table to cache */
		Path docLengthFile = env.getDoclengthsData();
		if (!fs.exists(docLengthFile)) {
			throw new RuntimeException("Error, doc-length data file " + docLengthFile + "doesn't exist!");
		}
		DistributedCache.addCacheFile(docLengthFile.toUri(), conf);

		conf.setMapperClass(MyMapper.class);
		//conf.setInt("mapred.task.timeout",3600000);
		conf.setJobName("GetWeightedTermDocVectors:" + collectionName);
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);
		conf.setInt("mapred.min.split.size", minSplitSize);
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("Ivory.MinNumTerms", getConf().getInt("Ivory.MinNumTerms", Integer.MAX_VALUE));		
		conf.setBoolean("Ivory.Normalize", getConf().getBoolean("Ivory.Normalize", false));
		if(getConf().get("Ivory.ShortDocLengths")!=null){
			conf.set("Ivory.ShortDocLengths", getConf().get("Ivory.ShortDocLengths"));
		}
		conf.set("Ivory.ScoringModel", getConf().get("Ivory.ScoringModel"));

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, weightedVectorsPath);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(HMapSFW.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(HMapSFW.class);

		sLogger.info("Running job: "+conf.getJobName());
		
		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}
}
