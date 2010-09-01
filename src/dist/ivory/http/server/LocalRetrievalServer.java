package ivory.http.server;

import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.builder.MRFBuilderFactory;
import ivory.smrf.model.expander.MRFExpander;
import ivory.smrf.model.expander.MRFExpanderFactory;
import ivory.smrf.retrieval.QueryRunner;
import ivory.util.RetrievalEnvironment;

import java.io.StringReader;
import java.net.InetAddress;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class LocalRetrievalServer extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(LocalRetrievalServer.class);

	private static String[] sModelSpecifications = new String[] {
			"<model id=\"robust04-lm-ql\" type=\"FullIndependence\" mu=\"1000.0\" output=\"robust04-lm-ql.ranking\" />",
			"<model id=\"robust04-bm25-base\" type=\"Feature\" output=\"robust04-bm25-base.ranking\"><feature id=\"term\" weight=\"1.0\" cliqueSet=\"term\" potential=\"IvoryExpression\" generator=\"term\" scoreFunction=\"BM25\" k1=\"0.5\" b=\"0.3\" /></model>" };

	private static QueryRunner sQueryRunner = null;
	private static RetrievalEnvironment sEnv = null;

	// use BM25 by default
	private static String sDefaultModelSpec = sModelSpecifications[1];

	/**
	 * Creates an instance of this tool.
	 */
	private LocalRetrievalServer() {
	}

	private static int printUsage() {
		System.out.println("usage: [lm|bm25] [index-path] [port]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
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

		String indexPath = args[1];
		int port = Integer.parseInt(args[2]);

		sLogger.info("Host: " + InetAddress.getLocalHost().toString());
		sLogger.info("Port: " + port);
		sLogger.info("Index path: " + indexPath);

		String collectionName = RetrievalEnvironment.readCollectionName(FileSystem.get(getConf()),
				indexPath);

		startQueryRunner(indexPath);
		startHTTPServer(indexPath, collectionName, port);

		while (true)
			;
	}

	private void startQueryRunner(String indexPath) {
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

	private void startHTTPServer(String indexPath, String collectionName, int port) {
		sLogger.info("Initilaizing server...");
		HttpServer server = new HttpServer();
		SocketListener listener = new SocketListener();
		listener.setPort(port);
		server.addListener(listener);
		ServletHolder directQueryServletHolder = null;
		ServletHolder homeServletHolder = null;

		try {
			HttpContext context = server.getContext("/");
			ServletHandler handler = new ServletHandler();

			homeServletHolder = handler.addServlet("Home", "/", HomeServlet.class.getName());

			directQueryServletHolder = handler.addServlet("DirectQuery", DirectQueryServlet.ACTION,
					DirectQueryServlet.class.getName());

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

			((DirectQueryServlet) directQueryServletHolder.getServlet()).set(sQueryRunner, sEnv);
			((HomeServlet) homeServletHolder.getServlet()).setCollectionName(collectionName);

			sLogger.info("Server successfully started!");
		} catch (Exception e) {
			sLogger.info("Server fails to start!");
			e.printStackTrace();
		}
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LocalRetrievalServer(), args);
		System.exit(res);
	}
}
