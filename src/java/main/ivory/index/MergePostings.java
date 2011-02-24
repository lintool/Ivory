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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.PowerTool;

public class MergePostings extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(MergePostings.class);

	public static final String[] RequiredParameters = { "Ivory.IndexPath" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public MergePostings(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unchecked")
	public int runTool() throws Exception {
		sLogger.info("PowerTool: MergePostings");
		JobConf conf = new JobConf(getConf(), MergePostings.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = getConf().get("Ivory.IndexPath");

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		String collectionName = env.readCollectionName();
		String postingsType = env.readPostingsType();

		Path inputPath = new Path(env.getPostingsDirectory());
		Path outputPath = new Path(env.getTempDirectory());

		fs.delete(outputPath, true);

		conf.setJobName("MergePostings:" + collectionName);

		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputKeyClass(Text.class);

		conf.setSpeculativeExecution(false);

		Class<PostingsList> postingsClass = (Class<PostingsList>) Class.forName(postingsType);

		conf.setMapOutputValueClass(postingsClass);
		conf.setOutputValueClass(postingsClass);

		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		fs.delete(inputPath, true);
		fs.rename(outputPath, inputPath);

		return 0;
	}
}
