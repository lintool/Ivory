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

package ivory.preprocess;

import ivory.data.IntDocVector;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;


import edu.umd.cloud9.util.PowerTool;

@SuppressWarnings("deprecation")
public class BuildIntDocVectorsForwardIndex extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(BuildIntDocVectorsForwardIndex.class);

	protected static enum Dictionary {
		Size
	};

	private static class MyMapRunner implements
			MapRunnable<IntWritable, IntDocVector, IntWritable, Text> {

		private String mInputFile;
		private Text outputValue = new Text();

		public void configure(JobConf job) {
			mInputFile = job.get("map.input.file");
		}

		public void run(RecordReader<IntWritable, IntDocVector> input,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			IntWritable key = input.createKey();
			IntDocVector value = input.createValue();
			int fileNo = Integer.parseInt(mInputFile.substring(mInputFile.lastIndexOf("-") + 1));

			long pos = input.getPos();
			while (input.next(key, value)) {
				outputValue.set(fileNo + "\t" + pos);

				output.collect(key, outputValue);
				reporter.incrCounter(Dictionary.Size, 1);

				pos = input.getPos();
			}
			sLogger.info("last termid: " + key + "(" + fileNo + ", " + pos + ")");
		}
	}

	public static final long BigNumber = 1000000000;

	private static class MyReducer extends MapReduceBase implements
			Reducer<IntWritable, Text, Text, Text> {

		FSDataOutputStream mOut;

		int mCollectionDocumentCount;
		int mCurDoc = 0;

		public void configure(JobConf job) {
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
			} catch (Exception e) {
				throw new RuntimeException("Error opening the FileSystem!");
			}

			String indexPath = job.get("Ivory.IndexPath");

			RetrievalEnvironment env = null;
			try {
				env = new RetrievalEnvironment(indexPath, fs);
			} catch (IOException e) {
				throw new RuntimeException("Unable to create RetrievalEnvironment!");
			}

			boolean buildWeighted = job.getBoolean ("Ivory.BuildWeighted", false);
			String forwardIndexPath = (buildWeighted ? 
									   env.getWeightedIntDocVectorsForwardIndex () :
									   env.getIntDocVectorsForwardIndex ());
			mCollectionDocumentCount = env.readCollectionDocumentCount ();

			try {
				mOut = fs.create (new Path (forwardIndexPath), true);
				mOut.writeInt(env.readDocnoOffset());
				mOut.writeInt(mCollectionDocumentCount);
			} catch (Exception e) {
				throw new RuntimeException("Error in creating files!");
			}

		}

		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] s = values.next().toString().split("\\s+");

			//sLogger.info (key + ": " + s[0] + " " + s[1]);
			if (values.hasNext()) {
				String[] s2 = values.next().toString().split("\\s+");
				throw new RuntimeException("There shouldn't be more than one value, key: " + key + ", first value: " + StringUtils.join (s, " ") + ", second value: " + StringUtils.join (s2, " "));
			}

			int fileNo = Integer.parseInt(s[0]);
			long filePos = Long.parseLong(s[1]);
			long pos = BigNumber * fileNo + filePos;

			mCurDoc++;

			mOut.writeLong(pos);
		}

		public void close() throws IOException {
			mOut.close();

			if (mCurDoc != mCollectionDocumentCount) {
				throw new IOException("Expected " + mCollectionDocumentCount
						+ " docs, actually got " + mCurDoc + " terms!");
			}
		}
	}

	public BuildIntDocVectorsForwardIndex(Configuration conf) {
		super(conf);
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.NumMapTasks" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public int runTool() throws Exception {
		JobConf conf = new JobConf(getConf(), BuildIntDocVectorsForwardIndex.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		String collectionName = env.readCollectionName();
		boolean buildWeighted = conf.getBoolean ("Ivory.BuildWeighted", false);

		sLogger.info("Tool: BuildIntDocVectorsIndex");
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - BuildWeighted: " + buildWeighted);
		sLogger.info(" - NumMapTasks: " + mapTasks);

		String intDocVectorsPath;
		String forwardIndexPath;
		if (buildWeighted) {
			intDocVectorsPath = env.getWeightedIntDocVectorsDirectory ();
			forwardIndexPath = env.getWeightedIntDocVectorsForwardIndex ();
		} else {
			intDocVectorsPath = env.getIntDocVectorsDirectory ();
			forwardIndexPath = env.getIntDocVectorsForwardIndex ();
		}

		if (!fs.exists(new Path(intDocVectorsPath))) {
			sLogger.info("Error: IntDocVectors don't exist!");
			return 0;
		}

		if (fs.exists (new Path (forwardIndexPath))) {
			sLogger.info ("IntDocVectorIndex already exists: skipping!");
			return 0;
		}

		conf.setJobName("BuildIntDocVectorsForwardIndex:" + collectionName);

		Path inputPath = new Path(intDocVectorsPath);
		FileInputFormat.setInputPaths(conf, inputPath);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputFormat(NullOutputFormat.class);

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);

		return 0;
	}
}
