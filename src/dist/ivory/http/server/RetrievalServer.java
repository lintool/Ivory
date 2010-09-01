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

package ivory.http.server;

import ivory.http.NLineInputFormat;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.builder.MRFBuilderFactory;
import ivory.smrf.model.expander.MRFExpander;
import ivory.smrf.model.expander.MRFExpanderFactory;
import ivory.smrf.retrieval.QueryRunner;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
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
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.FSProperty;

/**
 * @author Tamer Elsayed
 */
public class RetrievalServer extends Configured implements Tool {

	private static final String PREFIX = "_PATH_";
	private static final Logger sLogger = Logger.getLogger(RetrievalServer.class);

	private static String[] sModelSpecifications = new String[] {
			"<model id=\"robust04-lm-ql\" type=\"FullIndependence\" mu=\"1000.0\" output=\"robust04-lm-ql.ranking\" />",
			"<model id=\"robust04-bm25-base\" type=\"Feature\" output=\"robust04-bm25-base.ranking\"><feature id=\"term\" weight=\"1.0\" cliqueSet=\"term\" potential=\"IvoryExpression\" generator=\"term\" scoreFunction=\"BM25\" k1=\"0.5\" b=\"0.3\" /></model>" };

	private static QueryRunner sQueryRunner = null;
	private static RetrievalEnvironment sEnv = null;

	// use BM25 by default
	private static String sDefaultModelSpec = sModelSpecifications[1];

	public static class Server extends NLineServerMapper {

		int port;
		String indexPath;
		String serverID;
		String addressPath;
		private String collectionName;

		/**
		 * Writes the IP address of the current host to HDFS so that the broker
		 * read it to contact the server
		 * 
		 * @throws IOException
		 *             if writing to the file system fails
		 */
		private void writeIPAddressToHDFS(Configuration conf) throws IOException {
			FileSystem fs = FileSystem.get(conf);
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
			String fname = appendPath(addressPath, serverID + ".host");
			sLogger.info("Writing host address to " + fname);
			sLogger.info("  address: " + hostIP + ":" + port);

			FSProperty.writeString(fs, fname, hostIP + ":" + port);

			sLogger.info("writing done.");
		}

		/**
		 * Initializes the retrieval engine used to search the index
		 */
		private void startQueryRunner() {
			sLogger.info("Starting query runner ...");

			Node modelNode = null;
			Node expanderNode = null;
			Set<String> stopwords = null;

			// initialize retrieval environment variables
			MRFBuilder builder = null;
			MRFExpander expander = null;

			// default retrieval model
			try {
				Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
						new InputSource(new StringReader(sDefaultModelSpec)));
				modelNode = d.getElementsByTagName("model").item(0);

				// retrieval environment
				sEnv = new RetrievalEnvironment(indexPath);

				// get the MRF builder
				builder = MRFBuilderFactory.getBuilder(sEnv, modelNode.cloneNode(true));

				// get the MRF expander
				expander = null;
				if (expanderNode != null) {
					expander = MRFExpanderFactory.getExpander(sEnv, expanderNode.cloneNode(true));
				}
				if (stopwords != null && stopwords.size() != 0) {
					expander.setStopwordList(stopwords);
				}

				// query runner
				sQueryRunner = new QueryRunner(builder, expander);
				sLogger.info("Query runner initialized.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * Initializes the HTTP server that defines search services. Four
		 * services are provided:
		 * <ul>
		 * <li> A search user interface
		 * <li> A ranked-retrieval service for the user
		 * <li> A ranked-retrieval service for the broker
		 * <li> A document-retrieval service by both the user and the broker
		 * </ul>
		 */
		private void startHTTPServer() {
			sLogger.info("Initilaizing server...");
			HttpServer server = new HttpServer();
			SocketListener listener = new SocketListener();
			listener.setPort(port);
			server.addListener(listener);
			ServletHolder directQueryServletHolder = null;
			ServletHolder brokerQueryServletHolder = null;
			ServletHolder homeServletHolder = null;
			try {
				HttpContext context = server.getContext("/");
				ServletHandler handler = new ServletHandler();

				// Form for local queries
				homeServletHolder = handler.addServlet("Home", "/", HomeServlet.class.getName());

				// Responding to local queries
				directQueryServletHolder = handler.addServlet("DirectQuery",
						DirectQueryServlet.ACTION, DirectQueryServlet.class.getName());

				// Responding to queries from broker
				brokerQueryServletHolder = handler.addServlet("BrokerQuery",
						BrokerQueryServlet.ACTION, BrokerQueryServlet.class.getName());

				// Responding to broker's document fetch requests
				ServletHolder fetchHolder = handler.addServlet("Fetch", FetchDocnoServlet.ACTION,
						FetchDocnoServlet.class.getName());
				ServletContext fetchContext = fetchHolder.getServletContext();
				fetchContext.setAttribute("IndexPath", indexPath);

				context.addHandler(handler);
			} catch (Exception e) {
				e.printStackTrace();
			}

			sLogger.info("Starting server...");
			try {
				server.start();

				((DirectQueryServlet) directQueryServletHolder.getServlet())
						.set(sQueryRunner, sEnv);

				((BrokerQueryServlet) brokerQueryServletHolder.getServlet())
						.setQueryRunnerAndRetrievalEnv(sQueryRunner, sEnv);

				((HomeServlet) homeServletHolder.getServlet()).setCollectionName(collectionName);

				sLogger.info("Server successfully started!");
			} catch (Exception e) {
				sLogger.info("Server fails to start!");
				e.printStackTrace();
			}
		}

		public void run(LongWritable key, Text value, JobConf conf, Reporter reporter)
				throws IOException {

			addressPath = conf.get("AddressPath");

			String[] parameters = value.toString().trim().split("\\s+");
			indexPath = parameters[0].substring(parameters[0].indexOf(PREFIX) + PREFIX.length());
			collectionName = RetrievalEnvironment.readCollectionName(FileSystem.get(conf),
					indexPath);
			serverID = parameters[1];
			port = Integer.parseInt(parameters[2]);

			sLogger.info("Host: " + InetAddress.getLocalHost().toString());
			sLogger.info("Port: " + port);
			sLogger.info("Index path: " + indexPath);
			sLogger.info("Server ID: " + serverID);
			sLogger.info("Address Path: " + addressPath);

			writeIPAddressToHDFS(conf);
			startQueryRunner();
			startHTTPServer();

			// signal that the server is ready
			FSProperty.writeInt(FileSystem.get(conf), appendPath(addressPath, serverID + ".ready"), 0);

			while (true)
				;
		}
	}

	public static void runNLineRetrievalServers(String serversAddressPath, String configFilePath)
			throws Exception {

		JobConf conf = new JobConf(RetrievalServer.class);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NLineInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(Server.class);

		FileInputFormat.setInputPaths(conf, new Path(configFilePath));

		conf.set("AddressPath", serversAddressPath);
		conf.setJobName("RetrievalServers");
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.set("mapred.job.queue.name", "search");

		JobClient client = new JobClient(conf);
		client.submitJob(conf);
	}

	/**
	 * Creates an instance of this tool.
	 */
	private RetrievalServer() {
	}

	private static int printUsage() {
		System.out.println("usage: [lm|bm25] [config-path] [index-path1] [index-path2] ...");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			printUsage();
			return -1;
		}

		if (args[0].equals("lm")) {
			sDefaultModelSpec = sModelSpecifications[0];
		} else if (args[0].equals("bm25")) {
			sDefaultModelSpec = sModelSpecifications[1];
		} else {
			throw new RuntimeException("Unsupported retrieval model: " + args[0]);
		}

		int port = 8000;
		int numServers = (args.length - 2);
		String addressPath = args[1];

		FileSystem fs = FileSystem.get(new Configuration());

		if (fs.exists(new Path(addressPath))) {
			fs.delete(new Path(addressPath), true);
		}

		String fname = appendPath(addressPath, "config" + 1 + "-" + numServers + ".txt");
		sLogger.info("Writing configuration to: " + fname);
		StringBuffer sb = new StringBuffer();
		for (int sid = 1; sid <= numServers; sid++) {
			port++;
			sb.append(PREFIX + args[sid + 1] + " " + sid + " " + port + "\n");
			sLogger.info(" sid=" + sid + ", index=" + args[sid + 1]);
		}
		FSProperty.writeString(fs, fname, sb.toString());
		runNLineRetrievalServers(addressPath, fname);

		sLogger.info("Waiting for host information (polling HDFS)...");

		// poll HDFS for hostnames and ports
		boolean allStarted = true;
		do {
			allStarted = true;
			for (int sid = 1; sid <= numServers; sid++) {
				String f = appendPath(addressPath, new Integer(sid).toString() + ".host");
				// sLogger.info(" Checking if " + f + " exists");
				if (!fs.exists(new Path(f))) {
					allStarted = false;
				}
			}
			Thread.sleep(5000);
			sLogger.info(" ...");
		} while (!allStarted);

		// poll HDFS for ready signal that the index is ready
		boolean allReady = true;
		do {
			allReady = true;
			for (int sid = 1; sid <= numServers; sid++) {
				String f = appendPath(addressPath, new Integer(sid).toString() + ".ready");
				// sLogger.info(" Checking if " + f + " exists");
				if (!fs.exists(new Path(f))) {
					allReady = false;
				}
			}
			Thread.sleep(5000);
			sLogger.info(" ...");
		} while (!allReady);

		sLogger.info("Host information: ");
		for (int i = 1; i <= numServers; i++) {
			String f = addressPath + "/" + new Integer(i).toString() + ".host";
			sLogger.info(" sid=" + i + ", " + FSProperty.readString(fs, f));
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
		int res = ToolRunner.run(new Configuration(), new RetrievalServer(), args);
		System.exit(res);
	}
}
