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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class ComputeDictionarySize extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(ComputeDictionarySize.class);

	protected static enum Dictionary {
		Size
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, PostingsList, Text, IntWritable> {

		private int mDfThreshold;
		private static Pattern sPatternAlphanumeric = Pattern.compile("[\\w]+");

		public void configure(JobConf job) {
			mDfThreshold = job.getInt("Ivory.DfThreshold", 0);
		}

		public void map(Text key, PostingsList p, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			Matcher m = sPatternAlphanumeric.matcher(key.toString());
			if (!m.matches())
				return;

			if (p.getDf() > mDfThreshold) {
				reporter.incrCounter(Dictionary.Size, 1);
			}
		}
	}

	public static final String[] RequiredParameters = { "Ivory.IndexPath", "Ivory.DfThreshold" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public ComputeDictionarySize(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {
		sLogger.info("Computing Dictionary Size...");

		JobConf conf = new JobConf(getConf(), ComputeDictionarySize.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		int dfTheshold = conf.getInt("Ivory.DfThreshold", 0);

		String collectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);

		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - DfThreshold: " + dfTheshold);

		Path inputPath = new Path(RetrievalEnvironment.getPostingsDirectory(indexPath));
		Path outputPath = new Path(RetrievalEnvironment.getTempDirectory(indexPath));

		fs.delete(outputPath, true);

		conf.setJobName("ComputeDictionarySize:" + collectionName);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapperClass(MyMapper.class);
		conf.setNumReduceTasks(0);

		RunningJob rj = JobClient.runJob(conf);
		Counters counters = rj.getCounters();
		long numTerms = counters.findCounter(Dictionary.Size).getCounter();

		fs.delete(outputPath, true);

		return (int) numTerms;
	}
}
