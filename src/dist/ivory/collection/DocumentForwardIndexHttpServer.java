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

package ivory.collection;

import ivory.data.DocumentForwardIndex;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.ServletHandler;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

/**
 * <p>
 * Web server for providing access to documents in a collection.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class DocumentForwardIndexHttpServer {

	private static final Logger sLogger = Logger.getLogger(DocumentForwardIndexHttpServer.class);

	private static DocumentForwardIndex<Indexable> sForwardIndex;
	private static DocnoMapping sDocnoMapping;
	private static String sCollectionName;
	private static int sNumDocs;
	private static int sDocnoOffset;

	@SuppressWarnings("unchecked")
	private static class Server extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			int port = 8888;

			FileSystem fs = FileSystem.get(conf);
			String indexPath = conf.get("Ivory.IndexPath");

			sCollectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);
			sNumDocs = RetrievalEnvironment.readCollectionDocumentCount(fs, indexPath);
			sDocnoOffset = RetrievalEnvironment.readDocnoOffset(fs, indexPath);

			String collectionPath = RetrievalEnvironment.readCollectionPath(fs, indexPath);
			String docnoMappingClass = RetrievalEnvironment.readDocnoMappingClass(fs, indexPath);
			String docnoMappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);
			String indexClass = RetrievalEnvironment.readDocumentForwardIndexClass(fs, indexPath);
			String indexFile = RetrievalEnvironment.getDocumentForwardIndex(indexPath);

			sLogger.info("host: " + InetAddress.getLocalHost().toString());
			sLogger.info("port: " + port);
			sLogger.info("index path: " + indexPath);
			sLogger.info("forward index: " + indexFile);
			sLogger.info("collection name: " + sCollectionName);
			sLogger.info("base path of collection: " + collectionPath);
			sLogger.info("collection document count: " + sNumDocs);

			try {
				sDocnoMapping = (DocnoMapping) Class.forName(docnoMappingClass).newInstance();
				sDocnoMapping.loadMapping(new Path(docnoMappingFile), fs);

			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}

			try {
				sForwardIndex = (DocumentForwardIndex<Indexable>) Class.forName(indexClass)
						.newInstance();
				sForwardIndex.loadIndex(sDocnoMapping, indexFile, collectionPath);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing forward index!");
			}

			HttpServer server = new HttpServer();
			SocketListener listener = new SocketListener();
			listener.setPort(port);
			server.addListener(listener);

			try {
				HttpContext context = server.getContext("/");
				ServletHandler handler = new ServletHandler();
				handler.addServlet("FetchDocid", "/fetch_docid", FetchDocid.class.getName());
				handler.addServlet("FetchDocno", "/fetch_docno", FetchDocno.class.getName());
				handler.addServlet("Home", "/", Home.class.getName());
				handler.addServlet("Dump", "/dump/*", "org.mortbay.servlet.Dump");

				context.addHandler(handler);

			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}

			while (true)
				;
		}
	}

	private DocumentForwardIndexHttpServer() {
	}

	// this has to be public
	public static class Home extends HttpServlet {

		static final long serialVersionUID = 8253865405L;
		static final Random r = new Random();

		protected void doGet(HttpServletRequest httpServletRequest,
				HttpServletResponse httpServletResponse) throws ServletException, IOException {
			httpServletResponse.setContentType("text/html");
			PrintWriter out = httpServletResponse.getWriter();

			out.println("<html><head><title>Collection Access: " + sCollectionName
					+ "</title><head>");
			out.println("<body>");

			out.println("<h3>Fetch a docid from " + sCollectionName + "</h3>");

			String id;
			out.println("<p>(random examples: ");

			id = sDocnoMapping.getDocid(r.nextInt(sNumDocs) + 1 + sDocnoOffset);
			out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>, ");

			id = sDocnoMapping.getDocid(r.nextInt(sNumDocs) + 1 + sDocnoOffset);
			out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>, ");

			id = sDocnoMapping.getDocid(r.nextInt(sNumDocs) + 1 + sDocnoOffset);
			out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>)</p>");

			out.println("<form method=\"post\" action=\"fetch_docid\">");
			out.println("<input type=\"text\" name=\"docid\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Fetch!\" />");
			out.println("</form>");
			out.println("</p>");

			out.println("<h3>Fetch a docno from " + sCollectionName + "</h3>");

			int n;
			out.println("<p>(random examples: ");

			n = r.nextInt(sNumDocs) + 1 + sDocnoOffset;
			out.println("<a href=\"/fetch_docno?docno=" + n + "\">" + n + "</a>, ");

			n = r.nextInt(sNumDocs) + 1 + sDocnoOffset;
			out.println("<a href=\"/fetch_docno?docno=" + n + "\">" + n + "</a>, ");

			n = r.nextInt(sNumDocs) + 1 + sDocnoOffset;
			out.println("<a href=\"/fetch_docno?docno=" + n + "\">" + n + "</a>)</p>");

			out.println("<p>");
			out.println("<form method=\"post\" action=\"fetch_docno\">");
			out.println("<input type=\"text\" name=\"docno\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Fetch!\" />");
			out.println("</form>");
			out.println("</p>");

			out.print("</body></html>\n");

			out.close();
		}
	}

	// this has to be public
	public static class FetchDocid extends HttpServlet {
		static final long serialVersionUID = 3986721097L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			sLogger.info("triggered servlet for fetching docids");

			res.setContentType(sForwardIndex.getContentType());

			PrintWriter out = res.getWriter();

			String docid = null;
			if (req.getParameterValues("docid") != null)
				docid = req.getParameterValues("docid")[0];

			Indexable doc = sForwardIndex.getDocid(docid);

			if (doc != null) {
				sLogger.info("fetched: " + doc.getDocid());
				out.print(doc.getContent());
			} else {
				sLogger.info("trapped error fetching " + docid);

				out.print("<html><head><title>Invalid docid!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid doc: " + docid + "</h3>\n");
				out.print("</body></html>\n");
			}

			out.close();
		}

	}

	// this has to be public
	public static class FetchDocno extends HttpServlet {
		static final long serialVersionUID = 5970126341L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			sLogger.info("triggered servlet for fetching docids");

			res.setContentType(sForwardIndex.getContentType());

			PrintWriter out = res.getWriter();

			int docno = 0;
			if (req.getParameterValues("docno") != null)
				docno = Integer.parseInt(req.getParameterValues("docno")[0]);

			Indexable doc = sForwardIndex.getDocno(docno);

			if (doc != null) {
				sLogger.info("fetched: " + doc.getDocid() + " = docno " + docno);
				out.print(doc.getContent());
			} else {
				sLogger.info("trapped error fetching " + docno);

				out.print("<html><head><title>Invalid docno!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid doc: " + docno + "</h3>\n");
				out.print("</body></html>\n");
			}

			out.close();
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		JobConf conf = new JobConf(DocumentForwardIndexHttpServer.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = args[0];

		String collectionName = RetrievalEnvironment.readCollectionName(fs, indexPath);

		System.out.println("index path: " + indexPath);

		conf.setJobName("ForwardIndexServer:" + collectionName);

		conf.set("mapred.child.java.opts", "-Xmx1024m");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(Server.class);

		conf.set("Ivory.IndexPath", indexPath);

		JobClient client = new JobClient(conf);
		client.submitJob(conf);
		System.out.println("server started!");
	}
}
