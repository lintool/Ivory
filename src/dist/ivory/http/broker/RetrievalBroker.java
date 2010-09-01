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

package ivory.http.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;

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
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;

import edu.umd.cloud9.demo.DemoNullInput;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.FSProperty;

/**
 * @author Tamer Elsayed
 */
public class RetrievalBroker extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RetrievalBroker.class);

	private static HashMap<Integer, Integer> docnoToServerMapping = new HashMap<Integer, Integer>();

	private static class Server extends NullMapper {

		private String[] serverAddresses = null;

		public void run(JobConf conf, Reporter reporter) throws IOException {
			int port = 9999;

			String configPath = conf.get("ServerAddressPath");

			String serverIDsStr = conf.get("serverIDs");

			sLogger.info("Host: " + InetAddress.getLocalHost().toString());
			sLogger.info("Port: " + port);
			sLogger.info("ServerAddresses: " + serverIDsStr);

			String[] serverIDs = serverIDsStr.split(";");

			FileSystem fs = FileSystem.get(conf);
			serverAddresses = new String[serverIDs.length];
			for (int i = 0; i < serverIDs.length; i++) {
				String fname = configPath + "/" + serverIDs[i] + ".host";
				serverAddresses[i] = FSProperty.readString(fs, fname);
			}

			HttpServer server = new HttpServer();
			SocketListener listener = new SocketListener();
			listener.setPort(port);
			server.addListener(listener);

			ServletHolder queryServletHolder = null;
			ServletHolder fetchServletHolder = null;
			try {
				HttpContext context = server.getContext("/");
				ServletHandler handler = new ServletHandler();

				queryServletHolder = handler.addServlet("Query", QueryServlet.ACTION,
						QueryServlet.class.getName());
				fetchServletHolder = handler.addServlet("BrokerFetch", BrokerFetchServlet.ACTION,
						BrokerFetchServlet.class.getName());

				handler.addServlet("Home", "/", HomeServlet.class.getName());

				context.addHandler(handler);

			} catch (Exception e) {
				e.printStackTrace();
			}

			sLogger.info("Starting retrieval broker...");
			try {
				server.start();
				// Thread.sleep(10000);
				QueryServlet queryServlet = (QueryServlet) queryServletHolder.getServlet();
				queryServlet.setRetrievalServerAddresses(serverAddresses);
				queryServlet.setDocMapping(docnoToServerMapping);

				BrokerFetchServlet fetchServlet = (BrokerFetchServlet) fetchServletHolder
						.getServlet();
				fetchServlet.setRetrievalServerAddresses(serverAddresses);
				fetchServlet.setDocMapping(docnoToServerMapping);
				sLogger.info("Broker successfully started!");
			} catch (Exception e) {
				e.printStackTrace();
			}

			String s = InetAddress.getLocalHost().toString() + ":" + port;
			FSProperty.writeString(FileSystem.get(conf), appendPath(configPath, "broker.ready"), s);

			while (true)
				;
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public RetrievalBroker() {
	}

	private static int printUsage() {
		System.out.println("usage: [config-path]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			printUsage();
			return -1;
		}

		String configPath = args[0];
		FileSystem fs = FileSystem.get(getConf());

		String ids = "";

		sLogger.info("server config path: " + configPath);
		FileStatus[] stats = fs.listStatus(new Path(configPath));

		if (stats == null) {
			sLogger.info("Error: " + configPath + " not found!");
			return -1;
		}

		for (int i = 0; i < stats.length; i++) {
			String s = stats[i].getPath().toString();
			if (!s.endsWith(".host"))
				continue;

			String sid = s.substring(s.lastIndexOf("/") + 1, s.lastIndexOf(".host"));
			sLogger.info("sid=" + sid + ", host=" + s);

			if (ids.length() != 0)
				ids += ";";

			ids += sid;
		}

		JobConf conf = new JobConf(DemoNullInput.class);
		conf.setJobName("ThreadedRetrievalBroker:" + ids);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(Server.class);

		conf.set("serverIDs", ids);
		conf.set("ServerAddressPath", configPath);
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.set("mapred.job.queue.name", "search");

		fs.delete(new Path(appendPath(configPath, "broker.ready")), true);

		JobClient client = new JobClient(conf);
		client.submitJob(conf);

		sLogger.info("broker started!");

		while (true) {
			String f = appendPath(configPath, "broker.ready");
			if (fs.exists(new Path(f))) {
				break;
			}

			Thread.sleep(5000);
		}

		String s = FSProperty.readString(FileSystem.get(conf), appendPath(configPath,
				"broker.ready"));
		sLogger.info("broker ready at " + s);

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
		int res = ToolRunner.run(new Configuration(), new RetrievalBroker(), args);
		System.exit(res);
	}
}
