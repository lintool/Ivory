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

import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;
import ivory.util.ResultWriter;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.umd.cloud9.collection.DocnoMapping;

public class BatchQueryRunner {
	private static final Logger LOGGER = Logger.getLogger(BatchQueryRunner.class);

	private DocnoMapping mDocnoMapping = null;
	private FileSystem mFileSystem = null;
	private String mIndexPath = null;
	private RetrievalEnvironment mEnv = null;

	private Map<String, String> mQueries = new LinkedHashMap<String, String>();
	private Map<String, Node> mModels = new LinkedHashMap<String, Node>();
	private Map<String, Node> mDocscores = new LinkedHashMap<String, Node>();
	private Map<String, Node> mExpanders = new HashMap<String, Node>();
	private HashSet<String> mStopwords = new HashSet<String>();

	private Map<String, QueryRunner> mQueryRunners = new LinkedHashMap<String, QueryRunner>();

	/**
	 * @param args
	 * @throws SAXException
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SMRFException
	 * @throws NotBoundException
	 */
	public BatchQueryRunner(String[] args, FileSystem fs) throws SAXException, IOException,
			ParserConfigurationException, Exception, NotBoundException {
		mFileSystem = fs;
		parseParameters(args);

		// make sure we have an index to run against
		if (mIndexPath == null) {
			throw new Exception("Must specify an index!");
		}

		mDocnoMapping = RetrievalEnvironment.loadDocnoMapping(mIndexPath);
		mEnv = new RetrievalEnvironment(mIndexPath, fs);
		mEnv.initialize(true);

		for (Map.Entry<String, Node> n : mDocscores.entrySet()) {
			String type = XMLTools.getAttributeValue(n.getValue(), "type", "");
			String provider = XMLTools.getAttributeValue(n.getValue(), "provider", "");
			String path = n.getValue().getTextContent();

			if (type.equals("") || provider.equals("") || path.equals("")) {
				throw new Exception("Invalid docscore!");
			}

			LOGGER.info("Loading docscore: type=" + type + ", provider=" + provider + ", path="
					+ path);
			mEnv.loadDocScore(type, provider, path);
		}

		// make sure there are models that need evaluated
		if (mModels.size() == 0) {
			throw new Exception("Must specify at least one model!");
		}

		// make sure we have some queries to run
		if (mQueries.size() == 0) {
			throw new Exception("Must specify at least one query!");
		}
	}

	public void runQueries() throws Exception {
		for (String modelID : mModels.keySet()) {

			Node modelNode = mModels.get(modelID);
			Node expanderNode = mExpanders.get(modelID);

			// initialize retrieval environment variables
			QueryRunner runner = null;
			MRFBuilder builder = null;
			MRFExpander expander = null;
			try {
				// get the MRF builder
				builder = MRFBuilder.get(mEnv, modelNode.cloneNode(true));

				// get the MRF expander
				expander = null;
				if (expanderNode != null) {
					expander = MRFExpander.getExpander(mEnv, expanderNode.cloneNode(true));
				}
				if (mStopwords != null && mStopwords.size() != 0) {
					expander.setStopwordList(mStopwords);
				}

				int numHits = XMLTools.getAttributeValue(modelNode, "hits", 1000);

				LOGGER.info("number of hits: " + numHits);
				runner = new ThreadedQueryRunner(builder, expander, 1, numHits);

				mQueryRunners.put(modelID, runner);
				// multi-threaded query evaluation still a bit unstable, setting
				// thread=1 for now
			} catch (Exception e) {
				e.printStackTrace();
			}

			for (String queryID : mQueries.keySet()) {
				String rawQueryText = mQueries.get(queryID);
				String[] queryTokens = mEnv.tokenize(rawQueryText);

				LOGGER.info("query id:" + queryID + ", query:" + rawQueryText);

				// execute the query
				runner.runQuery(queryID, queryTokens);
			}

			// where should we output these results?
			ResultWriter resultWriter = null;
			Node model = mModels.get(modelID);
			String fileName = XMLTools.getAttributeValue(model, "output", null);
			boolean compress = XMLTools.getAttributeValue(model, "compress", false);
			resultWriter = getResultWriter(fileName, compress);
			printResults(modelID, runner, resultWriter);
			// flush the result writer
			resultWriter.flush();
		}

	}

	public Set<String> getModels() {
		return mModels.keySet();
	}

	public Map<String, Accumulator[]> getResults(String model) {
		return mQueryRunners.get(model).getResults();
	}

	public DocnoMapping getDocnoMapping() {
		return mDocnoMapping;
	}

	public ResultWriter getResultWriter(String fileName, boolean compress) throws Exception {
		return new ResultWriter(fileName, compress, mFileSystem);
	}

	private void printResults(String modelID, QueryRunner runner, ResultWriter resultWriter)
			throws Exception {

		for (String queryID : mQueries.keySet()) {
			// get the ranked list for this query
			Accumulator[] list = runner.getResults(queryID);
			if (list == null) {
				LOGGER.info("null results for: " + queryID);
				continue;
			}

			if (mDocnoMapping == null) {
				// print the results
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + list[i].docno + " " + (i + 1) + " "
							+ list[i].score + " " + modelID);
				}
			} else {
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + mDocnoMapping.getDocid(list[i].docno)
							+ " " + (i + 1) + " " + list[i].score + " " + modelID);
				}
			}
		}
	}

	private void parseParameters(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			String element = args[i];
			Document d = null;
			d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
					mFileSystem.open(new Path(element)));
			parseParameters(d);
		}
	}

	private void parseParameters(Document d) throws Exception, RemoteException, NotBoundException {
		// parse query elements
		NodeList queries = d.getElementsByTagName("query");
		for (int i = 0; i < queries.getLength(); i++) {
			// query XML node
			Node node = queries.item(i);

			// get query id
			String queryID = XMLTools.getAttributeValue(node, "id", null);
			if (queryID == null) {
				throw new Exception("Must specify a query id attribute for every query!");
			}

			// get query text
			String queryText = node.getTextContent();

			// add query to lookup
			if (mQueries.get(queryID) != null) {
				throw new Exception(
						"Duplicate query ids not allowed! Already parsed query with id=" + queryID);
			}
			mQueries.put(queryID, queryText);
		}

		// parse model elements
		NodeList models = d.getElementsByTagName("model");
		for (int i = 0; i < models.getLength(); i++) {
			// model XML node
			Node node = models.item(i);

			// get model id
			String modelID = XMLTools.getAttributeValue(node, "id", null);
			if (modelID == null) {
				throw new Exception("Must specify a model id for every model!");
			}

			// parse parent nodes
			NodeList children = node.getChildNodes();
			for (int j = 0; j < children.getLength(); j++) {
				Node child = children.item(j);
				if ("expander".equals(child.getNodeName())) {
					if (mExpanders.containsKey(modelID)) {
						throw new Exception("Only one expander allowed per model!");
					}
					mExpanders.put(modelID, child);
				}
			}

			// add model to lookup
			if (mModels.get(modelID) != null) {
				throw new Exception(
						"Duplicate model ids not allowed! Already parsed model with id=" + modelID);
			}
			mModels.put(modelID, node);
		}

		// parse stopwords
		NodeList stopwords = d.getElementsByTagName("stopword");
		for (int i = 0; i < stopwords.getLength(); i++) {
			// stopword node
			Node node = stopwords.item(i);

			// get stopword
			String stopword = node.getTextContent();

			// add stopword to lookup
			mStopwords.add(stopword);
		}

		// parse index
		NodeList index = d.getElementsByTagName("index");
		if (index.getLength() > 0) {
			if (mIndexPath != null) {
				throw new Exception(
						"Must specify only one index! There is no support for multiple indexes!");
			}
			mIndexPath = index.item(0).getTextContent();
		}

		// parse model elements
		NodeList docscores = d.getElementsByTagName("docscore");
		for (int i = 0; i < docscores.getLength(); i++) {
			// model XML node
			Node node = docscores.item(i);

			// get model id
			String docscoreType = XMLTools.getAttributeValue(node, "type", "");
			if (docscoreType.equals("")) {
				throw new Exception("Must specify a type for every docscore!");
			}

			// add model to lookup
			if (mDocscores.get(docscoreType) != null) {
				throw new Exception(
						"Duplicate docscore types not allowed! Already parsed model with id="
								+ docscoreType);
			}
			mDocscores.put(docscoreType, node);
		}
	}

}
