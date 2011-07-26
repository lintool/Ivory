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

package ivory.preprocess;

import ivory.data.IntDocVector;
import ivory.data.LazyIntDocVector;
import ivory.data.TermDocVector;
import ivory.data.TermIdMapWithCache;
import ivory.tokenize.DocumentProcessingUtils;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import edu.umd.cloud9.util.PowerTool;

@SuppressWarnings("deprecation")
public class BuildIntDocVectors extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(BuildIntDocVectors.class);
	{
		sLogger.setLevel (Level.WARN);
	}

	protected static enum Docs {
		Skipped, Total
	}

	protected static enum MapTime {
		DecodingAndIdMapping, EncodingAndSpilling
	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<IntWritable, TermDocVector, IntWritable, IntDocVector> {

		private TermIdMapWithCache termIDMap = null;

		public void configure(JobConf job) {

			String termsFile = job.get("Ivory.PrefixEncodedTermsFile");
			String termIDsFile = job.get("Ivory.TermIDsFile");
			String idToTermFile = job.get("Ivory.idToTermFile");

			try {
				// Detect if we're in standalone mode; if so, we can't use the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (job.get("mapred.job.tracker").equals("local")) {
					FileSystem fs = FileSystem.get(job);
					String indexPath = job.get("Ivory.IndexPath");

					RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

					termsFile = env.getIndexTermsData();
					termIDsFile = env.getIndexTermIdsData();
					idToTermFile = env.getIndexTermIdMappingData();
					try {
						termIDMap = new TermIdMapWithCache(new Path(termsFile),
								new Path(termIDsFile), new Path(idToTermFile), 0.2f, fs);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException("Error initializing Term to Id map!");
					}
				} else {
					Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
					try {
						termIDMap = new TermIdMapWithCache(localFiles[0],
								localFiles[1], localFiles[2], 0.3f, FileSystem.getLocal(job));
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException("Error initializing Term to Id map!");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}

		}

		public void map(IntWritable key, TermDocVector doc,
				OutputCollector<IntWritable, IntDocVector> output, Reporter reporter)
				throws IOException {
			long startTime = System.currentTimeMillis();
			TreeMap<Integer, int[]> termPositionsMap = DocumentProcessingUtils
					.getTermIDsPositionsMap(doc, termIDMap);
			reporter.incrCounter(MapTime.DecodingAndIdMapping, System.currentTimeMillis()
					- startTime);

			startTime = System.currentTimeMillis();
			IntDocVector docVector = new LazyIntDocVector(termPositionsMap);
			sLogger.debug ("in map, writing key: " + key + ", value of doc vector");
			output.collect(key, docVector);
			reporter.incrCounter(MapTime.EncodingAndSpilling, System.currentTimeMillis()
					- startTime);
			reporter.incrCounter(Docs.Total, 1);
		}

		public void close() throws IOException {
		}
	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks", "Ivory.IndexPath" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildIntDocVectors(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unused")
	public int runTool() throws Exception {
		// create a new JobConf, inheriting from the configuration of this
		// PowerTool
		JobConf conf = new JobConf(getConf(), BuildIntDocVectors.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);

		String collectionName = env.readCollectionName();

		sLogger.info("PowerTool: BuildIntDocVectors");
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info("This is new!");
		String termsFile = env.getIndexTermsData();
		String termIDsFile = env.getIndexTermIdsData();
		String idToTermFile = env.getIndexTermIdMappingData();

		Path termsFilePath = new Path(termsFile);
		Path termIDsFilePath = new Path(termIDsFile);

		if (!fs.exists(termsFilePath) || !fs.exists(termIDsFilePath)) {
			sLogger.error("Error, terms files don't exist!");
			return 0;
		}

		Path outputPath = new Path(env.getIntDocVectorsDirectory());
		if (fs.exists(outputPath)) {
			sLogger.info("IntDocVectors already exist: skipping!");
			return 0;
		}

		DistributedCache.addCacheFile(new URI(termsFile), conf);
		DistributedCache.addCacheFile(new URI(termIDsFile), conf);
		DistributedCache.addCacheFile(new URI(idToTermFile), conf);

		conf.setJobName("BuildIntDocVectors:" + collectionName);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, env.getTermDocVectorsDirectory());
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat
				.setOutputCompressionType(conf, SequenceFile.CompressionType.RECORD);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(LazyIntDocVector.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(LazyIntDocVector.class);

		conf.setMapperClass(MyMapper.class);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}
}
