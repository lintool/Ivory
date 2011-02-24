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

import ivory.exception.ConfigurationException;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.util.ResultWriter;
import ivory.util.RetrievalEnvironment;
import ivory.util.XMLTools;

import java.io.IOException;
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

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.DocnoMapping;

public class BatchQueryRunner {
	private static final Logger LOGGER = Logger.getLogger(BatchQueryRunner.class);

	private DocnoMapping mDocnoMapping = null;
	protected FileSystem mFs = null;
	protected String mIndexPath = null;
	protected RetrievalEnvironment mEnv = null;

	private Map<String, String> mQueries = new LinkedHashMap<String, String>();
	private Map<String, Node> mImportanceModels = new LinkedHashMap<String, Node>();
	private Map<String, Node> mModels = new LinkedHashMap<String, Node>();
	private Map<String, Node> mDocscores = new LinkedHashMap<String, Node>();
	private Map<String, Node> mExpanders = new HashMap<String, Node>();
	private Set<String> mStopwords = new HashSet<String>();
	private Map<String, Map<String, Double>> mJudgments = new HashMap<String, Map<String, Double>>();

	private Map<String, QueryRunner> mQueryRunners = new LinkedHashMap<String, QueryRunner>();

	public BatchQueryRunner(String[] args, FileSystem fs) throws ConfigurationException {
		init (args,fs);
	}
	
	public BatchQueryRunner(){
	}
	
	public void init(String[] args, FileSystem fs) throws ConfigurationException {
		Preconditions.checkNotNull(fs);
		Preconditions.checkNotNull(args);

		mFs = fs;
		
		parseParameters(args);
		loadRetrievalEnv();
		try {
			mDocnoMapping = RetrievalEnvironment.loadDocnoMapping(mIndexPath, fs);
		} catch (IOException e) {
			throw new ConfigurationException("Failed to load Docnomapping: "
					+ e.getMessage());
		}

		// Load static concept importance models
		for(Map.Entry<String, Node> n : mImportanceModels.entrySet()) {
			ConceptImportanceModel m = ConceptImportanceModel.get(n.getValue());
			mEnv.addImportanceModel(n.getKey(), m);
		}

		// Load static docscores (e.g., spam score, PageRank, etc.).
		for (Map.Entry<String, Node> n : mDocscores.entrySet()) {
			String type = XMLTools.getAttributeValue(n.getValue(), "type", "");
			String provider = XMLTools.getAttributeValue(n.getValue(), "provider", "");
			String path = n.getValue().getTextContent();

			if (type.equals("") || provider.equals("") || path.equals("")) {
				throw new ConfigurationException("Invalid docscore!");
			}

			LOGGER.info("Loading docscore: type=" + type + ", provider=" + provider + ", path="
					+ path);
			mEnv.loadDocScore(type, provider, path);
		}		
	}
	
	protected void loadRetrievalEnv() throws ConfigurationException{
		try {
			mEnv = new RetrievalEnvironment(mIndexPath, mFs);
			mEnv.initialize(true);
		} catch (IOException e) {
			throw new ConfigurationException("Failed to instantiate RetrievalEnvironment: "
					+ e.getMessage());
		}
	}

	public void runQueries() {
		for (String modelID : mModels.keySet()) {
			Node modelNode = mModels.get(modelID);
			Node expanderNode = mExpanders.get(modelID);

			// Initialize retrieval environment variables.
			QueryRunner runner = null;
			MRFBuilder builder = null;
			MRFExpander expander = null;
			try {
				// Get the MRF builder.
				builder = MRFBuilder.get(mEnv, modelNode.cloneNode(true));

				// Get the MRF expander.
				expander = null;
				if (expanderNode != null) {
					expander = MRFExpander.getExpander(mEnv, expanderNode.cloneNode(true));
				}
				if (mStopwords != null && mStopwords.size() != 0) {
					expander.setStopwordList(mStopwords);
				}

				int numHits = XMLTools.getAttributeValue(modelNode, "hits", 1000);

				LOGGER.info("number of hits: " + numHits);

				// Multi-threaded query evaluation still a bit unstable; setting
				// thread=1 for now.
				runner = new ThreadedQueryRunner(builder, expander, 1, numHits);

				mQueryRunners.put(modelID, runner);
			} catch (Exception e) {
				e.printStackTrace();
			}

			for (String queryID : mQueries.keySet()) {
				String rawQueryText = mQueries.get(queryID);
				String[] queryTokens = mEnv.tokenize(rawQueryText);

				LOGGER.info(String.format("query id: %s, query: \"%s\"", queryID, rawQueryText));
				System.out.println(queryID +"\t" + queryTokens.length + " term(s)");

				// Execute the query.
				runner.runQuery(queryID, queryTokens);
			}

			// Where should we output these results?
			Node model = mModels.get(modelID);
			String fileName = XMLTools.getAttributeValue(model, "output", null);
			boolean compress = XMLTools.getAttributeValue(model, "compress", false);

			try {
				ResultWriter resultWriter = new ResultWriter(fileName, compress, mFs);
				printResults(modelID, runner, resultWriter);
				resultWriter.flush();
			} catch (IOException e) {
				throw new RuntimeException("Error: Unable to write results!");
			}
		}

	}

	public Set<String> getModels() {
		return mModels.keySet();
	}

	public Node getModel(String modelName) {
		return mModels.get(modelName);
	}

	public RetrievalEnvironment getRetrievalEnvironment() {
		return mEnv;
	}

	public Map<String, String> getQueries() {
		return mQueries;
	}

	public Map<String, Double> getJudgmentSet(String qid) {
		return mJudgments.get(qid);
	}

	public Map<String, Accumulator[]> getResults(String model) {
		return mQueryRunners.get(model).getResults();
	}

	public DocnoMapping getDocnoMapping() {
		return mDocnoMapping;
	}

	private void printResults(String modelID, QueryRunner runner, ResultWriter resultWriter)
			throws IOException {

		for (String queryID : mQueries.keySet()) {
			// Get the ranked list for this query.
			Accumulator[] list = runner.getResults(queryID);
			if (list == null) {
				LOGGER.info("null results for: " + queryID);
				continue;
			}

			if (mDocnoMapping == null) {
				// Print results with internal docnos if unable to translate to
				// external docids.
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + list[i].docno + " " + (i + 1) + " "
							+ list[i].score + " " + modelID);
				}
			} else {
				// Translate internal docnos to external docids.
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + mDocnoMapping.getDocid(list[i].docno)
							+ " " + (i + 1) + " " + list[i].score + " " + modelID);
				}
			}
		}
	}

	private void parseParameters(String[] args) throws ConfigurationException {
		for (int i = 0; i < args.length; i++) {
			String element = args[i];
			Document d = null;

			try {
				d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
							mFs.open(new Path(element)));
			} catch (SAXException e) {
				throw new ConfigurationException(e.getMessage());
			} catch (IOException e) {
				throw new ConfigurationException(e.getMessage());
			} catch (ParserConfigurationException e) {
				throw new ConfigurationException(e.getMessage());
			}

			parseQueries(d);
			parseImportanceModels(d);
			parseModels(d);
			parseStopwords(d);
			parseIndexLocation(d);
			parseDocscores(d);
			parseJudgments(d);
		}

		// Make sure we have some queries to run.
		if (mQueries.isEmpty()) {
			throw new ConfigurationException("Must specify at least one query!");
		}

		// Make sure there are models that need evaluated.
		if (mModels.isEmpty()) {
			throw new ConfigurationException("Must specify at least one model!");
		}

		// Make sure we have an index to run against.
		if (mIndexPath == null) {
			throw new ConfigurationException("Must specify an index!");
		}
	}

	private void parseQueries(Document d) throws ConfigurationException {
		NodeList queries = d.getElementsByTagName("query");

		for (int i = 0; i < queries.getLength(); i++) {
			// Get query XML node.
			Node node = queries.item(i);

			// Get query id.
			String queryID = XMLTools.getAttributeValue(node, "id", null);
			if (queryID == null) {
				throw new ConfigurationException(
						"Must specify a query id attribute for every query!");
			}

			// Get query text.
			String queryText = node.getTextContent();

			// Add query to internal map.
			if (mQueries.get(queryID) != null) {
				throw new ConfigurationException(
						"Duplicate query ids not allowed! Already parsed query with id=" + queryID);
			}
			mQueries.put(queryID, queryText);
		}
	}

	private void parseImportanceModels(Document d) throws ConfigurationException {
		NodeList importanceModels = d.getElementsByTagName("importancemodel");
		
		for(int i = 0; i < importanceModels.getLength(); i++) {
			// Get model XML node
			Node node = importanceModels.item(i);
			
			String modelId = XMLTools.getAttributeValue(node, "id", "");
			if (modelId.equals("")) {
				throw new ConfigurationException("Must specify an id for every importancemodel!");
			}

			// Add model to internal map.
			if (mImportanceModels.get(modelId) != null) {
				throw new ConfigurationException(
						"Duplicate importancemodel ids not allowed! Already parsed model with id="
								+ modelId);
			}
			mImportanceModels.put(modelId, node);			
		}
	}
	
	private void parseModels(Document d) throws ConfigurationException {
		NodeList models = d.getElementsByTagName("model");

		for (int i = 0; i < models.getLength(); i++) {
			// Get model XML node.
			Node node = models.item(i);

			// Get model id
			String modelID = XMLTools.getAttributeValue(node, "id", null);
			if (modelID == null) {
				throw new ConfigurationException("Must specify a model id for every model!");
			}

			// Parse parent nodes.
			NodeList children = node.getChildNodes();
			for (int j = 0; j < children.getLength(); j++) {
				Node child = children.item(j);
				if ("expander".equals(child.getNodeName())) {
					if (mExpanders.containsKey(modelID)) {
						throw new ConfigurationException("Only one expander allowed per model!");
					}
					mExpanders.put(modelID, child);
				}
			}

			// Add model to internal map.
			if (mModels.get(modelID) != null) {
				throw new ConfigurationException(
						"Duplicate model ids not allowed! Already parsed model with id=" + modelID);
			}
			mModels.put(modelID, node);
		}
	}

	private void parseStopwords(Document d) {
		NodeList stopwords = d.getElementsByTagName("stopword");

		for (int i = 0; i < stopwords.getLength(); i++) {
			Node node = stopwords.item(i);
			String stopword = node.getTextContent();

			// Add stopword to internal map.
			mStopwords.add(stopword);
		}
	}

	private void parseIndexLocation(Document d) throws ConfigurationException {
		NodeList index = d.getElementsByTagName("index");

		if (index.getLength() > 0) {
			if (mIndexPath != null) {
				throw new ConfigurationException(
						"Must specify only one index! There is no support for multiple indexes!");
			}
			mIndexPath = index.item(0).getTextContent();
		}
	}

	private void parseDocscores(Document d) throws ConfigurationException {
		NodeList docscores = d.getElementsByTagName("docscore");

		for (int i = 0; i < docscores.getLength(); i++) {
			Node node = docscores.item(i);

			String docscoreType = XMLTools.getAttributeValue(node, "type", "");
			if (docscoreType.equals("")) {
				throw new ConfigurationException("Must specify a type for every docscore!");
			}

			// Add model to internal map.
			if (mDocscores.get(docscoreType) != null) {
				throw new ConfigurationException(
						"Duplicate docscore types not allowed! Already parsed model with id="
								+ docscoreType);
			}
			mDocscores.put(docscoreType, node);
		}
	}

	private void parseJudgments(Document d) throws ConfigurationException {
		NodeList judgments = d.getElementsByTagName("judgment");

		for (int i = 0; i < judgments.getLength(); i++) {
			// Get XML node.
			Node node = judgments.item(i);

			// Get query, document, and judgment.
			String qid = XMLTools.getAttributeValue(node, "qid", null);
			String docname = XMLTools.getAttributeValue(node, "doc", null);
			String grade = XMLTools.getAttributeValue(node, "grade", null);

			if (qid == null) {
				throw new ConfigurationException("Each judgment must specify a qid attribute!");
			}

			if (docname == null) {
				throw new ConfigurationException("Each judgment must specify a doc attribute!");
			}

			if (grade == null) {
				throw new ConfigurationException("Each judgment must specify a grade attribute!");
			}

			Map<String, Double> queryJudgments = mJudgments.get(qid);
			if (queryJudgments == null) {
				queryJudgments = new HashMap<String, Double>();
				mJudgments.put(qid, queryJudgments);
			}

			queryJudgments.put(docname, Double.parseDouble(grade));
		}
	}
}
