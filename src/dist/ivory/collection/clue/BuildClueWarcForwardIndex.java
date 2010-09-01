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

package ivory.collection.clue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import edu.umd.cloud9.mapred.NoSplitSequenceFileInputFormat;
import edu.umd.cloud9.util.FSProperty;
import edu.umd.cloud9.util.PowerTool;

/**
 * <p>
 * Tool for building a document forward index for the ClueWeb09 collection.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class BuildClueWarcForwardIndex extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(BuildClueWarcForwardIndex.class);

	private static enum Records {
		TOTAL, PAGES
	};

	private static class MyMapRunner implements
			MapRunnable<LongWritable, ClueWarcRecord, Text, LongWritable> {

		private Mapper<LongWritable, ClueWarcRecord, Text, LongWritable> mapper;

		@SuppressWarnings("unchecked")
		public void configure(JobConf job) {
			this.mapper = (Mapper) ReflectionUtils.newInstance(job.getMapperClass(), job);
		}

		public void run(RecordReader<LongWritable, ClueWarcRecord> input,
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			try {
				LongWritable key = input.createKey();
				ClueWarcRecord value = input.createValue();

				LongWritable pos = new LongWritable(0);

				while (input.next(key, value)) {
					mapper.map(pos, value, output, reporter);
					pos.set(input.getPos());
				}
			} finally {
				mapper.close();
			}
		}

	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, ClueWarcRecord, Text, LongWritable> {

		private static OutputCollector<Text, LongWritable> sOutput;
		private static int sCnt = 0;
		private static String file;

		private static final Text t = new Text();
		private static final LongWritable i = new LongWritable();

		public void configure(JobConf job) {
			file = job.get("map.input.file");
			file = file.substring(file.indexOf("part-"));
		}

		public void map(LongWritable key, ClueWarcRecord doc,
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);
			sOutput = output;

			String id = doc.getHeaderMetadataItem("WARC-TREC-ID");
			sLogger.info(key + ", " + id);

			if (id != null) {
				reporter.incrCounter(Records.PAGES, 1);

				if (sCnt % 1000 == 0) {
					t.set(file + "\t" + id);
					i.set(key.get());

					sOutput.collect(t, i);
				}

				sCnt++;
			}

		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildClueWarcForwardIndex(Configuration conf) {
		super(conf);
	}

	/**
	 * Runs this tool.
	 */
	public int runTool() throws Exception {
		JobConf conf = new JobConf(getConf(), BuildClueWarcForwardIndex.class);
		FileSystem fs = FileSystem.get(getConf());

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionPath = FSProperty.readString(fs, indexPath + "/property.CollectionPath");
		String collectionName = FSProperty.readString(fs, indexPath + "/property.CollectionName");
		String outputPath = indexPath + "/doc-forward-index";
		String findexFile = indexPath + "/doc-forward-index.dat";

		sLogger.info("forward index path: " + outputPath);
		sLogger.info("base path of collection: " + collectionPath);

		conf.setJobName("BuildClueForwardIndex:" + collectionName);

		conf.setNumMapTasks(100);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(collectionPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(NoSplitSequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		// delete the output directory if it exists already
		fs.delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		// remove index file if it's in the way
		fs.delete(new Path(findexFile), true);
		fs.rename(new Path(outputPath + "/part-00000"), new Path(findexFile));

		FSProperty.writeString(fs, indexPath + "/property.ForwardIndexClass",
				"ivory.collection.clue.ClueWarcForwardIndex");

		return 0;
	}
}
