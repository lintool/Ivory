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

import ivory.smrf.retrieval.Accumulator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import edu.umd.cloud9.io.FSProperty;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

/**
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class RunRetrievalBroker extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RunRetrievalBroker.class);

	private static Map<Integer, Integer> docnoToServerMapping = new HashMap<Integer, Integer>();

	private static class ServerMapper extends NullMapper {

		private String[] serverAddresses = null;

		public void run(JobConf conf, Reporter reporter) throws IOException {
			int port = 9999;

			String configPath = conf.get("ServerAddressPath");
			String scoreMergeModel = conf.get("ScoreMergeModel");

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
			String fname = appendPath(configPath, "broker.brokerhost");
			sLogger.info("Writing host address to " + fname);
			sLogger.info("  address: " + hostIP + ":" + port);

			FSProperty.writeString(fs, fname, hostIP + ":" + port);

			sLogger.info("writing done.");
			sLogger.info("Score merging model: " + scoreMergeModel);

			if (!scoreMergeModel.equals("sort") && !scoreMergeModel.equals("normalize")) {
				throw new RuntimeException("Unsupported score mergeing model: " + scoreMergeModel);
			}

			String serverIDsStr = conf.get("serverIDs");

			sLogger.info("Host: " + InetAddress.getLocalHost().toString());
			sLogger.info("Port: " + port);
			sLogger.info("ServerAddresses: " + serverIDsStr);

			String[] serverIDs = serverIDsStr.split(";");

			serverAddresses = new String[serverIDs.length];
			for (int i = 0; i < serverIDs.length; i++) {
				fname = configPath + "/" + serverIDs[i] + ".host";
				serverAddresses[i] = FSProperty.readString(fs, fname);
			}

			Server server = new Server(port);
			Context root = new Context(server, "/", Context.SESSIONS);

			root.addServlet(new ServletHolder(new QueryServlet(serverAddresses,
					docnoToServerMapping, scoreMergeModel)), QueryServlet.ACTION);
			root.addServlet(new ServletHolder(new PlainTextQueryServlet(serverAddresses,
					docnoToServerMapping, scoreMergeModel)), PlainTextQueryServlet.ACTION);
			root.addServlet(new ServletHolder(new BrokerFetchServlet(serverAddresses,
					docnoToServerMapping)), BrokerFetchServlet.ACTION);
			root.addServlet(new ServletHolder(new HomeServlet()), "/");

			sLogger.info("Starting retrieval broker...");
			try {
				server.start();
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
	public RunRetrievalBroker() {
	}

	private static int printUsage() {
		System.out.println("usage: [config-path] [score-merge-model]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		String configPath = args[0];

		FileSystem fs = FileSystem.get(getConf());

		String ids = "";

		sLogger.info("Starting retrieval broker...");
		sLogger.info("server config path: " + configPath);
		FileStatus[] stats = fs.listStatus(new Path(configPath));

		if (stats == null) {
			sLogger.info("Error: " + configPath + " not found!");
			return -1;
		}

		String scoreMergeModel = args[1];
		if (!scoreMergeModel.equals("sort") && !scoreMergeModel.equals("normalize")) {
			throw new RuntimeException("Unsupported score merging model: " + args[1]);
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

		JobConf conf = new JobConf(RunRetrievalBroker.class);
		conf.setJobName("RetrievalBroker");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(ServerMapper.class);

		conf.set("serverIDs", ids);
		conf.set("ServerAddressPath", configPath);
		conf.set("ScoreMergeModel", scoreMergeModel);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

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
		int res = ToolRunner.run(new Configuration(), new RunRetrievalBroker(), args);
		System.exit(res);
	}

	public static class QueryServlet extends HttpServlet {
		private static final long serialVersionUID = -5998786589277554550L;

		public static final String ACTION = "/search";
		public static final String QUERY_FIELD = "query";

		private String[] serverAddresses;
		private Map<Integer, Integer> docnoToServerMapping = null;
		private String scoreMergeModel = "";

		public QueryServlet(String[] addresses, Map<Integer, Integer> mapping, String model) {
			serverAddresses = addresses;
			docnoToServerMapping = mapping;
			scoreMergeModel = model;
		}

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			sLogger.info("Triggered servlet for running queries");
			res.setContentType("text/html");
			PrintWriter out = res.getWriter();

			String query = null;
			if (req.getParameterValues("query") != null)
				query = req.getParameterValues("query")[0];

			sLogger.info("Raw query: " + query);

			long startTime = System.currentTimeMillis();
			ServerThread[] servers = new ServerThread[serverAddresses.length];
			Thread[] threads = new Thread[serverAddresses.length];
			for (int i = 0; i < serverAddresses.length; i++) {
				servers[i] = new ServerThread(serverAddresses[i], query);
				threads[i] = new Thread(servers[i]);
				threads[i].start();
			}
			try {
				for (Thread thread : threads) {
					thread.join();
				}
				sLogger.info("All servers: done.");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			sLogger.info("Score merging model: " + scoreMergeModel);

			Accumulator[] results = new Accumulator[0];
			for (int i = 0; i < servers.length; i++) {
				Accumulator[] serverResults = null;
				if (scoreMergeModel.equals("sort")) {
					serverResults = servers[i].getResults();
				} else {
					serverResults = servers[i].getZNormalizedResults();
				}

				if (docnoToServerMapping != null) {
					for (Accumulator a : serverResults)
						docnoToServerMapping.put(a.docno, i);
				}
				results = mergeScores(results, serverResults);
			}

			String formattedOutput = getFormattedResults(results, servers);
			long endTime = System.currentTimeMillis();
			sLogger.info("query execution time (ms): " + (endTime - startTime));

			out.println(formattedOutput);
			out.close();
		}

		protected String getFormattedResults(Accumulator[] results, ServerThread[] servers) {
			StringBuffer sb = new StringBuffer();
			sb.append("<html><head><title>Threaded Broker Results</title></head>\n<body>");

			sb.append("<ol>");
			for (Accumulator a : results) {
				sb.append("<li>docno <a href=" + BrokerFetchServlet.formatRequestURL(a.docno) + ">"
						+ a.docno + "</a> (" + a.score + ")</li>\n");
			}
			sb.append("</ol>");
			sb.append("</body></html>\n");

			return sb.toString();
		}

		private Accumulator[] mergeScores(Accumulator[] iScores, Accumulator[] jScores) {
			// assuming that scored documents are mutual exclusive
			Accumulator[] results = new Accumulator[iScores.length + jScores.length];
			int i = 0, j = 0, k = 0;
			while (i < iScores.length && j < jScores.length) {
				if (iScores[i].score > jScores[j].score) {
					results[k] = iScores[i];
					i++;
				} else {
					results[k] = jScores[j];
					j++;
				}
				k++;
			}
			while (i < iScores.length) {
				results[k] = iScores[i];
				i++;
				k++;
			}

			while (j < jScores.length) {
				results[k] = jScores[j];
				j++;
				k++;
			}

			return results;
		}

		protected static class ServerThread implements Runnable {

			String address;
			String query;
			String textResults = null;
			HashMap<Integer, String> docnoMapping = new HashMap<Integer, String>();

			public ServerThread() {
			}

			public ServerThread(String addr, String q) {
				address = addr;
				query = q;
			}

			public void set(String addr, String q) {
				address = addr;
				query = q;
			}

			public String getOriginalDocid(int docno) {
				return docnoMapping.get(docno);
			}

			public String getTextResults() {
				return textResults;
			}

			public Accumulator[] getZNormalizedResults() {
				float sum = 0, sumSq = 0;
				if (textResults == null)
					return null;
				String[] lines = textResults.split("\t");
				Accumulator[] results = new Accumulator[lines.length / 3];
				int i = 0;
				int j = 0;
				while (i < lines.length) {
					int docid = -1;
					try {
						docid = Integer.parseInt(lines[i]);
					} catch (NumberFormatException e) {
						i++;
						continue;
					}
					i++;
					float score = Float.parseFloat(lines[i]);
					sum += score;
					sumSq += score * score;
					i++;
					String originalDocID = lines[i];
					docnoMapping.put(new Integer(docid), originalDocID);
					i++;
					results[j] = new Accumulator(docid, score);
					j++;

				}
				int n = results.length;
				float muo = sum / n;
				float sigma = (float) Math.sqrt((sumSq - n * muo * muo) / (n - 1));

				for (Accumulator a : results) {
					a.score = (a.score - muo) / sigma;
				}
				sLogger.info("returning z-normalized scores.");
				return results;
			}

			public Accumulator[] getMaxMinNormalizedResults() {
				float min = Float.MAX_VALUE, max = Float.MIN_VALUE;
				if (textResults == null)
					return null;
				String[] lines = textResults.split("\t");
				Accumulator[] results = new Accumulator[lines.length / 3];
				int i = 0;
				int j = 0;
				while (i < lines.length) {
					int docid = -1;
					try {
						docid = Integer.parseInt(lines[i]);
					} catch (NumberFormatException e) {
						i++;
						continue;
					}
					i++;
					float score = Float.parseFloat(lines[i]);
					if (score > max)
						max = score;
					else if (score < min)
						min = score;
					i++;
					String originalDocID = lines[i];
					docnoMapping.put(new Integer(docid), originalDocID);
					i++;
					results[j] = new Accumulator(docid, score);
					j++;

				}
				float d = max - min;
				for (Accumulator a : results) {
					a.score = (a.score - min) / d;
				}
				sLogger.info("returning max/min normalized scores.");
				return results;
			}

			public Accumulator[] getResults() {
				if (textResults == null)
					return null;
				String[] lines = textResults.split("\t");
				Accumulator[] results = new Accumulator[lines.length / 3];
				int i = 0;
				int j = 0;
				while (i < lines.length) {
					int docid = -1;
					try {
						docid = Integer.parseInt(lines[i]);
					} catch (NumberFormatException e) {
						i++;
						continue;
					}
					i++;
					float score = Float.parseFloat(lines[i]);
					i++;
					String originalDocID = lines[i];
					docnoMapping.put(new Integer(docid), originalDocID);
					i++;
					results[j] = new Accumulator(docid, score);
					j++;
				}
				sLogger.info("returning original scores.");
				return results;
			}

			public void run() {
				try {
					String url = "http://" + address + RetrievalServer.QueryBrokerServlet.ACTION
							+ "?" + RetrievalServer.QueryBrokerServlet.QUERY_FIELD + "="
							+ query.replaceAll(" ", "+");

					sLogger.info("fetching " + url);

					textResults = HttpUtils.fetchURL(new URL(url));
					sLogger.info(Thread.currentThread().getName() + "-" + address + ": done.");
					docnoMapping.clear();
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
			}

		}

	}

	public static class PlainTextQueryServlet extends QueryServlet {
		private static final long serialVersionUID = -5998786589277554554L;
		public static final String ACTION = "/psearch";

		public PlainTextQueryServlet(String[] addresses, Map<Integer, Integer> mapping, String model) {
			super(addresses, mapping, model);
		}

		protected String getFormattedResults(Accumulator[] results, ServerThread[] servers) {
			StringBuffer sb = new StringBuffer();
			int k = 0;
			for (Accumulator a : results) {
				String origDocID = getOriginalDocID(a.docno, servers);
				if (origDocID == null) {
					sLogger.info("Docno not found in all servers: " + a.docno + " !!");
				}
				sb.append(a.docno + "\t" + a.score + "\t" + origDocID + "\n");
				k++;
				//if (k >= 2000)
				if (k >= 10000)
					break;
			}
			return sb.toString();
		}

		private String getOriginalDocID(int docno, ServerThread[] servers) {
			String s = "";
			for (ServerThread server : servers) {
				s = server.getOriginalDocid(docno);
				if (s != null)
					return s;
			}
			return null;
		}

	}

	public static class HomeServlet extends HttpServlet {
		private static final long serialVersionUID = 7368950575963429946L;

		protected void doGet(HttpServletRequest httpServletRequest,
				HttpServletResponse httpServletResponse) throws ServletException, IOException {
			httpServletResponse.setContentType("text/html");
			PrintWriter out = httpServletResponse.getWriter();

			out.println("<html><head><title>Ivory Search Interface</title><head>");
			out.println("<body>");
			out.println("<h3>Run a query:</h3>");
			out.println("<form method=\"post\" action=\"" + QueryServlet.ACTION + "\">");
			out.println("<input type=\"text\" name=\"" + QueryServlet.QUERY_FIELD
					+ "\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Run query!\" />");
			out.println("</form>");
			out.println("</p>");

			out.print("</body></html>\n");

			out.close();
		}
	}

	public static class BrokerFetchServlet extends HttpServlet {
		private static final long serialVersionUID = -5998986589277554550L;

		public static final String ACTION = "/BrokerFetch";
		public static final String DOCNO_FIELD = "docno";

		private String[] serverAddresses;

		private Map<Integer, Integer> docnoToServerMapping = null;

		public BrokerFetchServlet(String[] addresses, Map<Integer, Integer> mapping) {
			serverAddresses = addresses;
			docnoToServerMapping = mapping;
		}

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			sLogger.info("Triggered servlet for fetching a document");
			res.setContentType("text/html");
			PrintWriter out = res.getWriter();

			String docno = null;
			if (req.getParameterValues(DOCNO_FIELD) != null)
				docno = req.getParameterValues(DOCNO_FIELD)[0];

			sLogger.info("Raw query: " + docno);

			Integer serverNo = docnoToServerMapping.get(Integer.parseInt(docno));
			if (serverNo == null) {
				sLogger.info("document not found in results/mapping-table!!");
				return;
			}

			long startTime = System.currentTimeMillis();
			String document = HttpUtils.fetchURL(new URL("http://" + this.serverAddresses[serverNo]
					+ RetrievalServer.FetchDocnoServlet.ACTION + "?"
					+ RetrievalServer.FetchDocnoServlet.DOCNO + "=" + docno));
			long endTime = System.currentTimeMillis();
			sLogger.info("document fetched in time (ms): " + (endTime - startTime));
			out.println(document);
			out.close();
		}

		public static String formatRequestURL(int docno) {
			return ACTION + "?" + DOCNO_FIELD + "=" + new Integer(docno).toString();
		}
	}
}
