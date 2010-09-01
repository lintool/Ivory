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

package ivory.smrf.retrieval;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.FSProperty;

/**
 * @author Tamer Elsayed
 */
public class RunQueryBroker extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(RunQueryBroker.class);

	protected static enum Time {
		Query
	};

	private static class Server extends NullMapper {

		public void run(JobConf conf, Reporter reporter) throws IOException {

			String configPath = conf.get("ConfigPath");
			String queriesFilePath = conf.get("QueriesFilePath");
			String resultsFilePath = conf.get("ResultsFilePath");
			String runtag = conf.get("Runtag");
			int numHits = conf.getInt("NumHits", 0);

			FileSystem fs = FileSystem.get(conf);
			String fname = appendPath(configPath, "broker.brokerhost");
			sLogger.info("Reading broker address from: " + fname);
			String brokerAddress = FSProperty.readString(fs, fname);
			sLogger.info("Broker address: " + brokerAddress);

			BrokerBatchQueryRunner qr;
			try {
				qr = new BrokerBatchQueryRunner(queriesFilePath, runtag, brokerAddress,
						resultsFilePath, numHits);

				long start = System.currentTimeMillis();
				qr.runQueries();
				long end = System.currentTimeMillis();

				reporter.incrCounter(Time.Query, (end - start));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public RunQueryBroker() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [config-path] [runtag] [queries-file] [results-file] [num-hits]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String configPath = args[0];
		FileSystem fs = FileSystem.get(getConf());

		sLogger.info("server config path: " + configPath);
		FileStatus[] stats = fs.listStatus(new Path(configPath));

		if (stats == null) {
			sLogger.info("Error: " + configPath + " not found!");
			return -1;
		}

		String runtag = args[1];
		String queriesFilePath = args[2];
		String resultsFilePath = args[3];
		int numHits = Integer.parseInt(args[4]);

		JobConf conf = new JobConf(RunQueryBroker.class);
		conf.setJobName("RunQueryBroker");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(Server.class);

		conf.set("QueriesFilePath", queriesFilePath);
		conf.set("ConfigPath", configPath);
		conf.set("ResultsFilePath", resultsFilePath);
		conf.set("Runtag", runtag);
		conf.setInt("NumHits", numHits);

		conf.set("mapred.child.java.opts", "-Xmx2048m");

		JobClient client = new JobClient(conf);
		client.submitJob(conf);

		sLogger.info("runner started!");

		return 0;
	}

	private static String appendPath(String base, String file) {
		return base + (base.endsWith("/") ? "" : "/") + file;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RunQueryBroker(), args);
		System.exit(res);
	}
}
