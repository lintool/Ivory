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

package ivory.index;

import ivory.data.PostingsList;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.FSLineReader;
import edu.umd.cloud9.util.PowerTool;

public class ExtractCfFromPostings extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(ExtractCfFromPostings.class);

	protected static enum Count {
		TOTAL_TERMS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, PostingsList, Text, LongWritable> {

		private static final LongWritable sCf = new LongWritable();

		public void map(Text key, PostingsList p, OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws IOException {
			sCf.set(p.getCf());
			reporter.incrCounter(Count.TOTAL_TERMS, p.getCf());

			output.collect(key, sCf);
		}
	}

	private static class DataWriterMapper extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			String indexPath = conf.get("Ivory.IndexPath");
			FileSystem fs = FileSystem.get(conf);

			String cfDirectory = RetrievalEnvironment.getCfDirectory(indexPath);
			String cfFile = RetrievalEnvironment.getCfFile(indexPath);
			int numDocs = RetrievalEnvironment.readCollectionDocumentCount(fs, indexPath);
			int vocabSize = RetrievalEnvironment.readNumberOfPostings(fs, indexPath);
			long collectionTermCount = RetrievalEnvironment.readCollectionTermCount(fs, indexPath);

			sLogger.info("Total number of documents in collection: " + numDocs);
			sLogger.info("Total number of terms in collection: " + collectionTermCount);
			sLogger.info("Vocabulary size: " + vocabSize);

			Path p = new Path(cfDirectory);

			FileStatus[] fileStats = fs.listStatus(p);

			FSDataOutputStream out = fs.create(new Path(cfFile), true);
			out.writeInt(numDocs);
			out.writeInt(vocabSize);

			int cntVocabSize = 0;
			long cntNumTerms = 0;
			for (int i = 0; i < fileStats.length; i++) {
				// skip log files
				if (fileStats[i].getPath().getName().startsWith("_"))
					continue;

				sLogger.info("reading " + fileStats[i].getPath().toString());

				FSLineReader reader = new FSLineReader(fileStats[i].getPath(), fs);

				Text line = new Text();
				while (reader.readLine(line) > 0) {
					String[] arr = line.toString().split("\\t+", 2);

					String term = arr[0];
					long cf = Long.parseLong(arr[1]);

					out.writeUTF(term.toString());
					WritableUtils.writeVLong(out, cf);
					cntVocabSize++;
					cntNumTerms += cf;

					if (cntVocabSize % 100000 == 0)
						sLogger.info(cntVocabSize + " terms");

				}
				reader.close();
			}

			out.close();

			sLogger.info(cntVocabSize + " terms total");

			if (cntVocabSize != vocabSize)
				throw new RuntimeException("Vocabulary size different than expected! "
						+ "Expected=" + vocabSize + ", Actual=" + cntVocabSize);

			if (cntNumTerms != collectionTermCount)
				throw new RuntimeException("Total number of terms different than expected!"
						+ "Expected=" + collectionTermCount + ", Actual=" + cntNumTerms);
		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.NumMapTasks" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public ExtractCfFromPostings(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {
		sLogger.info("Building cf table...");

		JobConf conf = new JobConf(getConf(), ExtractCfFromPostings.class);
		FileSystem fs = FileSystem.get(conf);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);
		;

		sLogger.info("Characteristics of the collection:");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info("Characteristics of the job:");
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: 1");
		sLogger.info(" - MinSplitSize: " + minSplitSize);

		Path outputPath = new Path(RetrievalEnvironment.getCfDirectory(indexPath));
		Path inputPath = new Path(RetrievalEnvironment.getPostingsDirectory(indexPath));

		fs.delete(outputPath, true);

		conf.setJobName("ExtractCfFromPostings:" + collectionName + ":extracting");

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);
		conf.setInt("mapred.min.split.size", minSplitSize);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		long startTime = System.currentTimeMillis();
		RunningJob rj = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		Counters counters = rj.getCounters();

		int vocabSize = (int) counters.findCounter("org.apache.hadoop.mapred.Task$Counter", 8,
				"REDUCE_OUTPUT_RECORDS").getCounter();

		long collectionTermCount = counters.getCounter(Count.TOTAL_TERMS);

		sLogger.info("Vocabulary size: " + vocabSize);
		sLogger.info("Total number of terms in collection: " + collectionTermCount);

		int vocabSizeExpected = RetrievalEnvironment.readNumberOfPostings(fs, indexPath);
		long termCountExpected = RetrievalEnvironment.readCollectionTermCount(fs, indexPath);

		if (vocabSize != vocabSizeExpected) {
			throw new RuntimeException("Vocabulary size different than expected! " + "Expected="
					+ vocabSize + ", Actual=" + vocabSizeExpected);
		}

		if (collectionTermCount != termCountExpected) {
			throw new RuntimeException("Collection term count different than expected! "
					+ "Expected=" + collectionTermCount + ", Actual=" + termCountExpected);
		}

		writeData();

		return 0;
	}

	private void writeData() throws IOException {
		FileSystem fs = FileSystem.get(getConf());

		String indexPath = getConf().get("Ivory.IndexPath");

		String collectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);
		String cfDataFile = RetrievalEnvironment.getCfFile(indexPath);
		String outputPath = RetrievalEnvironment.getCfDirectory(indexPath);

		sLogger.info("Writing cf data to " + cfDataFile + "...");

		JobConf conf = new JobConf(getConf(), ExtractCfFromPostings.class);
		conf.setJobName("ExtractCfFromPostings:" + collectionName + ":encoding");
		conf.setSpeculativeExecution(false);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(DataWriterMapper.class);

		JobClient.runJob(conf);

		// remove temp output
		fs.delete(new Path(outputPath), true);

		sLogger.info("Done!");
	}
}
