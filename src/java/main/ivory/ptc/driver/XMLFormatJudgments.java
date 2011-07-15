/**
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

package ivory.ptc.driver;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;

import ivory.ptc.data.PseudoJudgments;
import ivory.ptc.data.PseudoQuery;

/**
 * Driver that formats the pseudo judgments and outputs an XML file
 * that can be used directly for training.
 *
 * @author Nima Asadi
 */
@SuppressWarnings("deprecation")
public class XMLFormatJudgments extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(XMLFormatJudgments.class);
	static {
		LOG.setLevel(Level.INFO);
	}

	public static class MyReducer extends MapReduceBase
	    implements Reducer<PseudoQuery, PseudoJudgments, Text, Text> {
		private static final Text keyOut = new Text();
		private static final Text valueOut = new Text("");
		private static final ClueWarcDocnoMapping mDocnoMapping = new ClueWarcDocnoMapping();
		private static PseudoJudgments nextJudgeJudgments;
		private static int id = 1;

    @Override
		public void configure(JobConf job) {
			Path[] localFiles;
      try {
        localFiles = DistributedCache.getLocalCacheFiles(job);
      } catch (IOException e) {
        throw new RuntimeException("Local cache files not read properly.", e);
      }
			try {
				mDocnoMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
			} catch (Exception e) {
				throw new RuntimeException("Error initializing DocnoMapping!", e);
			}
		}

    @Override
		public void reduce(PseudoQuery key, Iterator<PseudoJudgments> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			while(values.hasNext()) {
				nextJudgeJudgments = values.next();
				for(int i = 0; i < nextJudgeJudgments.size(); i++) {
					keyOut.set(id + " 0 " + mDocnoMapping.getDocid( nextJudgeJudgments.getDocno(i) ) + " 1");
					output.collect(keyOut, valueOut);
				}
			}
			id++;
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [docno-mapping]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

  @Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}
		JobConf conf = new JobConf(getConf(), XMLFormatJudgments.class);
		// Command line arguments
		String inPath = args[0];
		String outPath = args[1];
		String docnoMapping = args[2];
		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);
		int mapTasks = 1;
		int reduceTasks = 1;

		conf.setJobName("FormatPseudoJudgments");
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		DistributedCache.addCacheFile(new URI(docnoMapping), conf);
		FileSystem.get(conf).delete(outputPath);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputKeyClass(PseudoQuery.class);
		conf.setMapOutputValueClass(PseudoJudgments.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new XMLFormatJudgments(), args);
		System.exit(res);
	}
}
