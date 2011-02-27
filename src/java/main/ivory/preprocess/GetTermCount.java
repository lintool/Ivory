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

import ivory.data.TermDocVector;
import ivory.util.Constants;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfIntLong;
import edu.umd.cloud9.util.PowerTool;

@SuppressWarnings("deprecation")
public class GetTermCount extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(GetTermCount.class);

	protected static enum Statistics {
		Docs, Terms, SumOfDocLengths
	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<IntWritable, TermDocVector, Text, PairOfIntLong> {

		private static Text sTerm = new Text();
		private static PairOfIntLong sPair = new PairOfIntLong();

		public void configure(JobConf job) {
			sLogger.setLevel(Level.WARN);
		}

		public void map(IntWritable key, TermDocVector doc,
				OutputCollector<Text, PairOfIntLong> output, Reporter reporter) throws IOException {

			TermDocVector.Reader r = doc.getReader();
			reporter.setStatus("d" + key.get());
			int dl = 0;
			int tf;
			while (r.hasMoreTerms()) {
				sTerm.set(r.nextTerm());
				tf = r.getTf();
				dl += tf;
				sPair.set(1, tf);
				output.collect(sTerm, sPair);
			}

			reporter.incrCounter(Statistics.Docs, 1);
			reporter.incrCounter(Statistics.SumOfDocLengths, dl);
		}
	}

	private static class MyCombiner extends MapReduceBase implements
			Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {

		private static PairOfIntLong sPair = new PairOfIntLong();

		public void reduce(Text key, Iterator<PairOfIntLong> values,
				OutputCollector<Text, PairOfIntLong> output, Reporter reporter) throws IOException {
			int df = 0;
			long cf = 0;
			while (values.hasNext()) {
				sPair = values.next();
				df += sPair.getLeftElement();
				cf += sPair.getRightElement();
			}

			sPair.set(df, cf);
			output.collect(key, sPair);
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {

		private int minDf, maxDf;

		public void configure(JobConf job) {
			minDf = job.getInt("Ivory.MinDf", 2);
			maxDf = job.getInt("Ivory.MaxDf", Integer.MAX_VALUE);
		}

		PairOfIntLong dfcf = new PairOfIntLong();

		public void reduce(Text key, Iterator<PairOfIntLong> values,
				OutputCollector<Text, PairOfIntLong> output, Reporter reporter) throws IOException {
			int df = 0;
			long cf = 0;
			while (values.hasNext()) {
				dfcf = values.next();
				df += dfcf.getLeftElement();
				cf += dfcf.getRightElement();
			}
			if (df < minDf || df > maxDf)
				return;
			reporter.incrCounter(Statistics.Terms, 1);
			dfcf.set(df, cf);
			output.collect(key, dfcf);
		}
	}

	public static final String[] RequiredParameters = { Constants.NumMapTasks,
			Constants.CollectionName, Constants.IndexPath, Constants.MinDf, Constants.MaxDf };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public GetTermCount(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {
		// create a new JobConf, inheriting from the configuration of this
		// PowerTool
		JobConf conf = new JobConf(getConf(), GetTermCount.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get(Constants.IndexPath);
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		int mapTasks = conf.getInt(Constants.NumMapTasks, 0);
		int reduceTasks = conf.getInt(Constants.NumReduceTasks, 0);

		String collectionName = env.readCollectionName();
		String termDocVectorsPath = env.getTermDocVectorsDirectory();
		String termDfCfPath = env.getTermDfCfDirectory();

		if (!fs.exists(new Path(indexPath))) {
			sLogger.info("index path doesn't existing: skipping!");
			return 0;
		}

		sLogger.info("PowerTool: GetTermCount");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);
		sLogger.info(" - MinDf: " + conf.getInt(Constants.MinDf, 0));
		sLogger.info(" - MaxDf: " + conf.getInt(Constants.MaxDf, Integer.MAX_VALUE));

		Path outputPath = new Path(termDfCfPath);
		if (fs.exists(outputPath)) {
			sLogger.error("TermDfCf directory exist: skipping!");
			return 0;
		}

		conf.setJobName("GetTermCount:" + collectionName);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

		FileInputFormat.setInputPaths(conf, new Path(termDocVectorsPath));
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(PairOfIntLong.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PairOfIntLong.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		Counters counters = job.getCounters();
		// write out number of postings
		int collectionTermCount = (int) counters.findCounter(Statistics.Terms).getCounter();
		env.writeCollectionTermCount(collectionTermCount);
		// NOTE: this value is not the same as number of postings, because
		// postings for non-English terms are discarded, or as result of df cut

		long collectionLength = counters.findCounter(Statistics.SumOfDocLengths).getCounter();
		env.writeCollectionLength(collectionLength);
		return 0;
	}
}
