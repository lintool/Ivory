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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import ivory.ptc.data.PseudoJudgments;
import ivory.ptc.data.PseudoQuery;

/**
 * Driver that formats the pseudo queries and outputs an XML file
 * that can be used directly for training.
 *
 * @author Nima Asadi
 */
@SuppressWarnings("deprecation")
public class XMLFormatQueries extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(XMLFormatQueries.class);
	static {
		LOG.setLevel(Level.INFO);
	}

	public static class MyReducer extends MapReduceBase
	    implements Reducer<PseudoQuery, PseudoJudgments, Text, Text> {
		private static final Text keyOut = new Text();
		private static final Text valueOut = new Text("");
		private static OutputCollector<Text, Text> outputCollector;
		private static boolean firstTime = true;
		private static int id = 1;

    @Override
		public void reduce(PseudoQuery key, Iterator<PseudoJudgments> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			if (firstTime) {
				outputCollector = output;
				keyOut.set("<parameters>");
				output.collect(keyOut, valueOut);
				firstTime = false;
			}
			keyOut.set("<query id=\"" + id + "\">" + key.getQuery() + "</query>");
			output.collect(keyOut, valueOut);
			id++;
		}

		public void close() throws IOException {
			keyOut.set("</parameters>");
			if(outputCollector != null) {
				outputCollector.collect(keyOut, valueOut);
			}
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

  @Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		JobConf conf = new JobConf(getConf(), XMLFormatQueries.class);
		// Command line arguments
		String inPath = args[0];
		String outPath = args[1];
		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);
		int mapTasks = 1;
		int reduceTasks = 1;

		conf.setJobName("FormatPseudoQueries");
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.set("mapred.child.java.opts", "-Xmx2048m");
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
		int res = ToolRunner.run(new Configuration(), new XMLFormatQueries(), args);
		System.exit(res);
	}
}
