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

import ivory.core.RetrievalEnvironment;
import ivory.core.util.XMLTools;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.QueryRunner;
import ivory.smrf.retrieval.ThreadedQueryRunner;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.DocumentForwardIndex;
import edu.umd.cloud9.collection.Indexable;

/**
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class RetrievalServer {
	private static final Logger sLogger = Logger.getLogger(RetrievalServer.class);
	/*{
		sLogger.setLevel(Level.INFO);
	}*/

	private QueryRunner mQueryRunner;
	private RetrievalEnvironment mEnv=null;
	private DocnoMapping mDocnoMapping;
	private DocumentForwardIndex<Indexable> mForwardIndex;
	private String mSid;

	public void initialize(String sid, String config, FileSystem fs) {
		System.out.println("$$ Initializing RetrievalServer for \"" + sid + "\"...");
		sLogger.info("Initializing RetrievalServer for \"" + sid + "\"...");

		mSid = sid;

		Document d = null;
		try {
			d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
					fs.open(new Path(config)));
		} catch (Exception e) {
			e.printStackTrace();
		}

		String indexPath = null;
		String findexPath = null;

		NodeList servers = d.getElementsByTagName("server");
		for (int i = 0; i < servers.getLength(); i++) {
			Node node = servers.item(i);

			// pick out the correct server id
			String id = XMLTools.getAttributeValue(node, "id", null);
			if (!id.equals(sid))
				continue;

			sLogger.info(" - sid: " + sid);

			NodeList children = node.getChildNodes();
			for (int j = 0; j < children.getLength(); j++) {
				Node child = children.item(j);
				if ("index".equals(child.getNodeName())) {
					sLogger.info(" - index: " + child.getTextContent().trim());

					indexPath = child.getTextContent().trim();
					if(mEnv == null){
						try {
							mEnv = new RetrievalEnvironment(indexPath, fs);
							mEnv.initialize(true);
						} catch (Exception e) {
							e.printStackTrace();
							throw new RuntimeException();
						}
					}
				}

				if ("findex".equals(child.getNodeName())) {
					sLogger.info(" - findex: " + child.getTextContent().trim());

					// initialize forward index
					findexPath = child.getTextContent().trim();
				}

				if ("docscore".equals(child.getNodeName())) {
					sLogger.info(" - docscore: " + child.getTextContent().trim());

					String type = XMLTools.getAttributeValue(child, "type", "");
					String provider = XMLTools.getAttributeValue(child, "provider", "");
					String path = child.getTextContent();

					if (type.equals("") || provider.equals("") || path.equals("")) {
						throw new RuntimeException("Invalid docscore!");
					}
					System.out.println("$$ Loading docscore: type=" + type + ", provider=" +
							provider + ", path="
							+ path);
					sLogger.info("Loading docscore: type=" + type + ", provider=" +
							provider + ", path="
							+ path);

					if(mEnv == null){
						try {
							mEnv = new RetrievalEnvironment(indexPath, fs);
							mEnv.initialize(true);
						} catch (Exception e) {
							e.printStackTrace();
							throw new RuntimeException();
						}
					}
					mEnv.loadDocScore(type, provider, path);
				}
				
				if("importancemodel".equals(child.getNodeName())) {
					sLogger.info(" - importancemodel: " + child.getTextContent().trim());
					
					String importanceModelId = XMLTools.getAttributeValue(child, "id", null);
					if(importanceModelId == null) {
						throw new RuntimeException("Invalid importance model!");
					}
					
					ConceptImportanceModel importanceModel = null;
					try {
						importanceModel = ConceptImportanceModel.get(child);
					}
					catch(Exception e) {
						throw new RuntimeException(e);
					}
					
					if(mEnv == null){
						try {
							mEnv = new RetrievalEnvironment(indexPath, fs);
							mEnv.initialize(true);
						} catch (Exception e) {
							e.printStackTrace();
							throw new RuntimeException();
						}
					}
					mEnv.addImportanceModel(importanceModelId, importanceModel);
				}

			}

			if (indexPath == null) {
				throw new RuntimeException("Error: must specify an index location!");
			}

			if (findexPath == null)
				sLogger.warn("forward index not specified: will not be able to access documents.");
		}
		if(mEnv == null){
			try {
				mEnv = new RetrievalEnvironment(indexPath, fs);
				mEnv.initialize(true);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
		}

		try {
			Node modelNode = d.getElementsByTagName("model").item(0);

			MRFBuilder builder = MRFBuilder.get(mEnv, modelNode.cloneNode(true));

			
			// Set the default number of hits to 2000 because that's what we had in our
			// official TREC 2009 web track runs; otherwise, IF merging approach
			// will give slightly different results, so we won't be able to
			// replicate results...
			//mQueryRunner = new ThreadedQueryRunner(builder, null, 1, 2000);
			int hits = Integer.parseInt(XMLTools.getAttributeValue(modelNode, "hits", 2000+""));
			mQueryRunner = new ThreadedQueryRunner(builder, null, 1, hits);

			// load docno/docid mapping
			try {
				mDocnoMapping = (DocnoMapping) Class.forName(
						mEnv.readDocnoMappingClass()).newInstance();

				mDocnoMapping.loadMapping(mEnv.getDocnoMappingData(), fs);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}

			// load document forward index
			if (findexPath != null) {
				FSDataInputStream in = fs.open(new Path(findexPath));
				String indexClass = in.readUTF();
				in.close();

				try {
					mForwardIndex = (DocumentForwardIndex<Indexable>) Class.forName(indexClass)
					.newInstance();
					mForwardIndex.loadIndex(new Path(findexPath), new Path(mEnv.getDocnoMappingData().toString()), fs);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException("Error initializing forward index!");
				}

			}

			sLogger.info("RetrievalServer successfully initialized.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start(int port) {
		sLogger.info("Staring server...");

		Server server = new Server(port);
		Context root = new Context(server, "/", Context.SESSIONS);
		root.addServlet(
				new ServletHolder(new QueryBrokerServlet(mQueryRunner, mEnv, mDocnoMapping)),
				QueryBrokerServlet.ACTION);
		root.addServlet(
				new ServletHolder(new QueryDirectServlet(mQueryRunner, mEnv, mDocnoMapping)),
				QueryDirectServlet.ACTION);
		root.addServlet(new ServletHolder(new FetchDocnoServlet(mForwardIndex)),
				FetchDocnoServlet.ACTION);
		root.addServlet(new ServletHolder(new HomeServlet(mSid)), "/");

		try {
			server.start();
			sLogger.info("Server successfully started!");
		} catch (Exception e) {
			sLogger.info("Server fails to start!");
			e.printStackTrace();
		}
	}

	public RetrievalServer() {
	}

	private static String join(String[] terms, String sep) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < terms.length; i++) {
			sb.append(terms[i]);
			if (i < terms.length - 1)
				sb.append(sep);
		}

		return sb.toString();
	}

	public static class HomeServlet extends HttpServlet {
		private static final long serialVersionUID = 7368950575963429946L;

		private String mSid;

		public HomeServlet(String sid) {
			mSid = sid;
		}

		protected void doGet(HttpServletRequest httpServletRequest,
				HttpServletResponse httpServletResponse) throws ServletException, IOException {
			httpServletResponse.setContentType("text/html");
			PrintWriter out = httpServletResponse.getWriter();

			out.println("<html><head><title>Ivory Search Interface: " + mSid + "</title><head>");
			out.println("<body>");
			out.println("<h3>Run a query on " + mSid + ":</h3>");
			out.println("<form method=\"post\" action=\"" + QueryDirectServlet.ACTION + "\">");
			out.println("<input type=\"text\" name=\"" + QueryDirectServlet.QUERY_FIELD
					+ "\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Run query!\" />");
			out.println("</form>");
			out.println("</p>");

			out.print("</body></html>\n");

			out.close();
		}
	}

	public static class QueryDirectServlet extends HttpServlet {
		public static final String ACTION = "/DirectQuery";
		public static final String QUERY_FIELD = "query";

		private static final long serialVersionUID = -5998786589277554550L;

		private QueryRunner mQueryRunner = null;
		private RetrievalEnvironment mEnv = null;
		private DocnoMapping mDocnoMapping = null;

		public QueryDirectServlet(QueryRunner queryRunner, RetrievalEnvironment env,
				DocnoMapping mapping) {
			mQueryRunner = queryRunner;
			mEnv = env;
			mDocnoMapping = mapping;
		}

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
		IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
		throws ServletException, IOException {
			sLogger.info("Triggered servlet for direct querying");
			res.setContentType("text/html");
			PrintWriter out = res.getWriter();

			String query = null;
			if (req.getParameterValues(QUERY_FIELD) != null)
				query = req.getParameterValues(QUERY_FIELD)[0];

			sLogger.info("Raw query: " + query);

			long startTime = System.currentTimeMillis();

			String[] queryTokens = mEnv.tokenize(query);
			sLogger.info("Tokenized query: " + join(queryTokens, " "));

			// run the query
			Accumulator[] results = mQueryRunner.runQuery(queryTokens);
			long endTime = System.currentTimeMillis();

			sLogger.info("query execution time (ms): " + (endTime - startTime));

			StringBuffer sb = new StringBuffer();
			sb.append("<html><head><title>Server Results</title></head>\n<body>");

			sb.append("<ol>");
			for (Accumulator a : results) {
				sb.append("<li>docno " + a.docno + ", docid <a href="
						+ FetchDocnoServlet.formatRequestURL(a.docno) + ">"
						+ mDocnoMapping.getDocid(a.docno) + "</a> (" + a.score + ")</li>\n");
			}
			sb.append("</ol>");
			sb.append("</body></html>\n");

			out.print(sb.toString());

			out.close();
		}

	}

	public static class QueryBrokerServlet extends HttpServlet {
		private static final long serialVersionUID = -5998786589277554550L;

		public static final String ACTION = "/BrokerQuery";
		public static final String QUERY_FIELD = "query";

		private QueryRunner mQueryRunner = null;
		private RetrievalEnvironment mEnv = null;
		private DocnoMapping mDocnoMapping = null;

		public QueryBrokerServlet(QueryRunner queryRunner, RetrievalEnvironment env,
				DocnoMapping docnoMapping) {
			mQueryRunner = queryRunner;
			mEnv = env;
			mDocnoMapping = docnoMapping;
		}

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
		IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
		throws ServletException, IOException {
			sLogger.info("Broker triggered servlet for running queries");
			res.setContentType("text/html");

			String query = null;
			if (req.getParameterValues(QUERY_FIELD) != null)
				query = req.getParameterValues(QUERY_FIELD)[0];

			long startTime = System.currentTimeMillis();

			String[] queryTokens = mEnv.tokenize(query);
			sLogger.info("Tokenized query: " + join(queryTokens, " "));

			// run the query
			Accumulator[] results = mQueryRunner.runQuery(queryTokens);
			long endTime = System.currentTimeMillis();

			sLogger.info("query execution time (ms): " + (endTime - startTime));

			StringBuffer sb = new StringBuffer();
			for (Accumulator a : results) {
				sb.append(a.docno + "\t" + a.score + "\t" + mDocnoMapping.getDocid(a.docno) + "\t");
			}
			PrintWriter out = res.getWriter();
			out.print(sb.toString().trim());
			out.close();
		}
	}

	public static class FetchDocnoServlet extends HttpServlet {
		static final long serialVersionUID = 3986721097L;

		public static final String ACTION = "/fetch_docno";
		public static final String DOCNO = "docno";

		private DocumentForwardIndex<Indexable> mForwardIndex;

		public FetchDocnoServlet(DocumentForwardIndex<Indexable> findex) {
			mForwardIndex = findex;
		}

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
		IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
		throws ServletException, IOException {
			sLogger.info("triggered servlet for fetching document by docno");

			if (mForwardIndex == null) {
				res.setContentType("text/html");

				PrintWriter out = res.getWriter();
				out.print("<html><head><title>Service Unavailable</title><head>\n");
				out.print("<body>\n");
				out.print("<h3>No document access is available!</h3>\n");
				out.print("</body></html>\n");
				out.close();
			}

			int docno = 0;
			try {
				if (req.getParameterValues(DOCNO) != null)
					docno = Integer.parseInt(req.getParameterValues(DOCNO)[0]);

				Indexable doc = mForwardIndex.getDocument(docno);

				if (doc != null) {
					sLogger.info("fetched: " + doc.getDocid() + " = docno " + docno);
					res.setContentType(doc.getDisplayContentType());

					PrintWriter out = res.getWriter();
					out.print(doc.getDisplayContent());
					out.close();
				} else {
					throw new Exception();
				}
			} catch (Exception e) {
				sLogger.info("trapped error fetching " + docno);
				res.setContentType("text/html");

				PrintWriter out = res.getWriter();
				out.print("<html><head><title>Invalid docno!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid doc: " + docno + "</h3>\n");
				out.print("</body></html>\n");
				out.close();
			}
		}

		public static String formatRequestURL(int docno) {
			return ACTION + "?" + DOCNO + "=" + new Integer(docno).toString();
		}
	}

}
