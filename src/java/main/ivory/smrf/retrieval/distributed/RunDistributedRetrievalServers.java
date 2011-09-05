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

package ivory.smrf.retrieval.distributed;


import ivory.core.util.XMLTools;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


import edu.umd.cloud9.io.FSProperty;
import edu.umd.cloud9.mapred.NullOutputFormat;

/**
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class RunDistributedRetrievalServers extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RunDistributedRetrievalServers.class);

	static enum Heartbeat {
		COUNT
	}

	public static class ServerMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, NullWritable> {

		// The sole job of this thread is to increment counters once in a while
		// to let the job track know we're still alive.
		private static class HeartbeatRunnable implements Runnable {
			Reporter mReporter;

			public HeartbeatRunnable(Reporter reporter) {
				mReporter = reporter;
			}

			public void run() {
				while (true) {
					try {
						mReporter.incrCounter(Heartbeat.COUNT, 1);
						Thread.sleep(60000);
					} catch (InterruptedException e) {
					}
				}
			}
		}

		private String mConfigPath;
		private String mConfigFile;
		private FileSystem mFS;

		public void configure(JobConf conf) {
			mConfigFile = conf.get("Ivory.ConfigFile");
			mConfigPath = conf.get("Ivory.ConfigPath");
			try {
				mFS = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
				throws IOException {

			Thread pulse = new Thread(new HeartbeatRunnable(reporter));
			pulse.start();

			String[] parameters = value.toString().trim().split("\\s+");
			String sid = parameters[0];
			int port = Integer.parseInt(parameters[1]);

			sLogger.info("Mapper launched!");
			sLogger.info(" - host name: " + InetAddress.getLocalHost().toString());
			sLogger.info(" - port: " + port);
			sLogger.info(" - server id: " + sid);
			sLogger.info(" - config path: " + mConfigPath);

			writeIPAddressToHDFS(sid, port);

			RetrievalServer server = new RetrievalServer();
			server.initialize(sid, mConfigFile, mFS);
			server.start(port);

			// signal that the server is ready
			FSProperty.writeInt(mFS, appendPath(mConfigPath, sid + ".ready"), 1);

			while (true)
				;
		}

		/**
		 * Writes the IP address of the current host to HDFS so that the broker
		 * read it to contact the server
		 * 
		 * @throws IOException
		 *             if writing to the file system fails
		 */
		private void writeIPAddressToHDFS(String sid, int port) throws IOException {
			String hostName = InetAddress.getLocalHost().toString();
			String hostIP = "";

			int k = hostName.lastIndexOf("/");
			if (k >= 0 && k < hostName.length())
				hostIP = hostName.substring(k + 1);
			else {
				k = hostName.lastIndexOf("\\");
				if (k >= 0 && k < hostName.length())
					hostIP = hostName.substring(k + 1);
				else
					hostIP = hostName;
			}
			String fname = appendPath(mConfigPath, sid + ".host");
			sLogger.info("Writing host address to " + fname);
			FSProperty.writeString(mFS, fname, hostIP + ":" + port);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	private RunDistributedRetrievalServers() {
	}

	private static int printUsage() {
		System.out.println("usage: [config-file] [config-path]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
			return -1;
		}

		String configFile = args[0];

		FileSystem fs = FileSystem.get(getConf());

		Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
				fs.open(new Path(configFile)));

		sLogger.info("Reading configuration to determine number of servers to launch:");
		List<String> sids = new ArrayList<String>();
		NodeList servers = d.getElementsByTagName("server");
		for (int i = 0; i < servers.getLength(); i++) {
			Node node = servers.item(i);

			// get server id
			String sid = XMLTools.getAttributeValue(node, "id", null);
			if (sid == null) {
				throw new Exception("Must specify a query id attribute for every server!");
			}

			sLogger.info(" - sid: " + sid);
			sids.add(sid);
		}

		int port = 7000;
		int numServers = sids.size();
		String configPath = args[1];

		if (fs.exists(new Path(configPath))) {
			fs.delete(new Path(configPath), true);
		}

		String fname = appendPath(configPath, "config-" + numServers + ".txt");
		sLogger.info("Writing configuration to: " + fname);
		StringBuffer sb = new StringBuffer();
		for (int n = 0; n < numServers; n++) {
			port++;
			sb.append(sids.get(n) + " " + port + "\n");
		}

		FSDataOutputStream out = fs.create(new Path(fname), true);
		out.writeBytes(sb.toString());
		out.close();

		JobConf conf = new JobConf(getConf(), RetrievalServer.class);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NLineInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(ServerMapper.class);

		FileInputFormat.setInputPaths(conf, new Path(fname));

		conf.set("Ivory.ConfigFile", configFile);
		conf.set("Ivory.ConfigPath", configPath);
		conf.setJobName("RetrievalServers");
		//conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		// conf.set("mapred.job.queue.name", "search");

		JobClient client = new JobClient(conf);
		client.submitJob(conf);

		sLogger.info("Waiting for servers to start up...");

		// poll HDFS for hostnames and ports
		boolean allStarted = true;
		do {
			allStarted = true;
			for (int n = 0; n < numServers; n++) {
				String f = appendPath(configPath, sids.get(n) + ".host");
				if (!fs.exists(new Path(f))) {
					allStarted = false;
				}
			}
			Thread.sleep(10000);
			sLogger.info(" ...");
		} while (!allStarted);

		// poll HDFS for ready signal that the index is ready
		boolean allReady = true;
		do {
			allReady = true;
			for (int n = 0; n < numServers; n++) {
				String f = appendPath(configPath, sids.get(n) + ".ready");
				if (!fs.exists(new Path(f))) {
					allReady = false;
				}
			}
			Thread.sleep(10000);
			sLogger.info(" ...");
		} while (!allReady);

		sLogger.info("All servers ready!");
		sLogger.info("Host information:");
		for (int n = 0; n < numServers; n++) {
			String f = appendPath(configPath, sids.get(n) + ".host");
			sLogger.info(" sid=" + sids.get(n) + ", " + FSProperty.readString(fs, f));
		}

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
		int res = ToolRunner.run(new Configuration(), new RunDistributedRetrievalServers(), args);
		System.exit(res);
	}

}
