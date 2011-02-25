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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.umd.cloud9.collection.DocnoMapping;

public class BatchQueryRunner {
	private static final Logger LOG = Logger.getLogger(BatchQueryRunner.class);

	private DocnoMapping docnoMapping = null;
	protected FileSystem fs = null;
	protected String indexPath = null;
	protected RetrievalEnvironment env = null;

	private final Map<String, String> queries = Maps.newLinkedHashMap();
	private final Map<String, Node> importanceModels = Maps.newLinkedHashMap();
	private final Map<String, Node> models = Maps.newLinkedHashMap();
	private final Map<String, Node> docscores = Maps.newLinkedHashMap();
	private final Map<String, Node> expanders = Maps.newHashMap();
	private final Map<String, Map<String, Double>> judgments = Maps.newHashMap();
	private final Map<String, QueryRunner> queryRunners = Maps.newLinkedHashMap();
	private final Set<String> stopwords = Sets.newHashSet();

	public BatchQueryRunner(String[] args, FileSystem fs) throws ConfigurationException {
		init(args,fs);
	}

	public void init(String[] args, FileSystem fs) throws ConfigurationException {
		this.fs = Preconditions.checkNotNull(fs);
		Preconditions.checkNotNull(args);

		parseParameters(args);
		loadRetrievalEnv();
		try {
			docnoMapping = RetrievalEnvironment.loadDocnoMapping(indexPath, fs);
		} catch (IOException e) {
			throw new ConfigurationException("Failed to load Docnomapping: "
					+ e.getMessage());
		}

		// Load static concept importance models
		for(Map.Entry<String, Node> n : importanceModels.entrySet()) {
			ConceptImportanceModel m = ConceptImportanceModel.get(n.getValue());
			env.addImportanceModel(n.getKey(), m);
		}

		// Load static docscores (e.g., spam score, PageRank, etc.).
		for (Map.Entry<String, Node> n : docscores.entrySet()) {
			String type = XMLTools.getAttributeValue(n.getValue(), "type", "");
			String provider = XMLTools.getAttributeValue(n.getValue(), "provider", "");
			String path = n.getValue().getTextContent();

			if (type.equals("") || provider.equals("") || path.equals("")) {
				throw new ConfigurationException("Invalid docscore!");
			}

			LOG.info("Loading docscore: type=" + type + ", provider=" + provider + ", path="
					+ path);
			env.loadDocScore(type, provider, path);
		}		
	}
	
	protected void loadRetrievalEnv() throws ConfigurationException{
		try {
			env = new RetrievalEnvironment(indexPath, fs);
			env.initialize(true);
		} catch (IOException e) {
			throw new ConfigurationException("Failed to instantiate RetrievalEnvironment: "
					+ e.getMessage());
		}
	}

	public void runQueries() {
		for (String modelID : models.keySet()) {
			Node modelNode = models.get(modelID);
			Node expanderNode = expanders.get(modelID);

			// Initialize retrieval environment variables.
			QueryRunner runner = null;
			MRFBuilder builder = null;
			MRFExpander expander = null;
			try {
				// Get the MRF builder.
				builder = MRFBuilder.get(env, modelNode.cloneNode(true));

				// Get the MRF expander.
				expander = null;
				if (expanderNode != null) {
					expander = MRFExpander.getExpander(env, expanderNode.cloneNode(true));
				}
				if (stopwords != null && stopwords.size() != 0) {
					expander.setStopwordList(stopwords);
				}

				int numHits = XMLTools.getAttributeValue(modelNode, "hits", 1000);

				LOG.info("number of hits: " + numHits);

				// Multi-threaded query evaluation still a bit unstable; setting
				// thread=1 for now.
				runner = new ThreadedQueryRunner(builder, expander, 1, numHits);

				queryRunners.put(modelID, runner);
			} catch (Exception e) {
				e.printStackTrace();
			}

			for (String queryID : queries.keySet()) {
				String rawQueryText = queries.get(queryID);
				String[] queryTokens = env.tokenize(rawQueryText);

				LOG.info(String.format("query id: %s, query: \"%s\"", queryID, rawQueryText));

				// Execute the query.
				runner.runQuery(queryID, queryTokens);
			}

			// Where should we output these results?
			Node model = models.get(modelID);
			String fileName = XMLTools.getAttributeValue(model, "output", null);
			boolean compress = XMLTools.getAttributeValue(model, "compress", false);

			try {
				ResultWriter resultWriter = new ResultWriter(fileName, compress, fs);
				printResults(modelID, runner, resultWriter);
				resultWriter.flush();
			} catch (IOException e) {
				throw new RuntimeException("Error: Unable to write results!");
			}
		}

	}

	public Set<String> getModels() {
		return models.keySet();
	}

	public Node getModel(String modelName) {
		return models.get(modelName);
	}

	public RetrievalEnvironment getRetrievalEnvironment() {
		return env;
	}

	public Map<String, String> getQueries() {
		return queries;
	}

	public Map<String, Double> getJudgmentSet(String qid) {
		return judgments.get(qid);
	}

	public Map<String, Accumulator[]> getResults(String model) {
		return queryRunners.get(model).getResults();
	}

	public DocnoMapping getDocnoMapping() {
		return docnoMapping;
	}

	private void printResults(String modelID, QueryRunner runner, ResultWriter resultWriter)
			throws IOException {

		for (String queryID : queries.keySet()) {
			// Get the ranked list for this query.
			Accumulator[] list = runner.getResults(queryID);
			if (list == null) {
				LOG.info("null results for: " + queryID);
				continue;
			}

			if (docnoMapping == null) {
				// Print results with internal docnos if unable to translate to
				// external docids.
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + list[i].docno + " " + (i + 1) + " "
							+ list[i].score + " " + modelID);
				}
			} else {
				// Translate internal docnos to external docids.
				for (int i = 0; i < list.length; i++) {
					resultWriter.println(queryID + " Q0 " + docnoMapping.getDocid(list[i].docno)
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
							fs.open(new Path(element)));
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
		if (queries.isEmpty()) {
			throw new ConfigurationException("Must specify at least one query!");
		}

		// Make sure there are models that need evaluated.
		if (models.isEmpty()) {
			throw new ConfigurationException("Must specify at least one model!");
		}

		// Make sure we have an index to run against.
		if (indexPath == null) {
			throw new ConfigurationException("Must specify an index!");
		}
	}

	private void parseQueries(Document d) throws ConfigurationException {
		NodeList queryNodes = d.getElementsByTagName("query");

		for (int i = 0; i < queryNodes.getLength(); i++) {
			// Get query XML node.
			Node node = queryNodes.item(i);

			// Get query id.
			String qid = XMLTools.getAttributeValueOrThrowException(node, "id",
						"Must specify a query id attribute for every query!");

			// Get query text.
			String queryText = node.getTextContent();

			// Add query to internal map.
			if (queries.get(qid) != null) {
				throw new ConfigurationException("Duplicate query ids not allowed! Already parsed query with id=" + qid);
			}
			queries.put(qid, queryText);
		}
	}

	private void parseImportanceModels(Document d) throws ConfigurationException {
		NodeList models = d.getElementsByTagName("importancemodel");
		
		for(int i = 0; i < models.getLength(); i++) {
			Node node = models.item(i);
			
			String id = XMLTools.getAttributeValueOrThrowException(node, "id",
				"Must specify an id for every importancemodel!");

			// Add model to internal map.
			if (importanceModels.get(id) != null) {
				throw new ConfigurationException(
						"Duplicate importancemodel ids not allowed! Already parsed model with id="
								+ id);
			}
			importanceModels.put(id, node);			
		}
	}
	
	private void parseModels(Document d) throws ConfigurationException {
		NodeList modelList = d.getElementsByTagName("model");

		for (int i = 0; i < modelList.getLength(); i++) {
			// Get model XML node.
			Node node = modelList.item(i);

			// Get model id.
			String id = XMLTools.getAttributeValueOrThrowException(node, "id",
			    "Must specify a model id for every model!");

			// Parse parent nodes.
			NodeList children = node.getChildNodes();
			for (int j = 0; j < children.getLength(); j++) {
				Node child = children.item(j);
				if ("expander".equals(child.getNodeName())) {
					if (expanders.containsKey(id)) {
						throw new ConfigurationException("Only one expander allowed per model!");
					}
					expanders.put(id, child);
				}
			}

			// Add model to internal map.
			if (models.get(id) != null) {
				throw new ConfigurationException(
						"Duplicate model ids not allowed! Already parsed model with id=" + id);
			}
			models.put(id, node);
		}
	}

	private void parseStopwords(Document d) {
		NodeList stopwordsList = d.getElementsByTagName("stopword");

		for (int i = 0; i < stopwordsList.getLength(); i++) {
			Node node = stopwordsList.item(i);
			String stopword = node.getTextContent();
			stopwords.add(stopword);
		}
	}

	private void parseIndexLocation(Document d) throws ConfigurationException {
		NodeList indexList = d.getElementsByTagName("index");

		if (indexList.getLength() > 0) {
			if (indexPath != null) {
				throw new ConfigurationException("Must specify only one index! There is no support for multiple indexes!");
			}
			indexPath = indexList.item(0).getTextContent();
		}
	}

	private void parseDocscores(Document d) throws ConfigurationException {
		NodeList docscoresList = d.getElementsByTagName("docscore");

		for (int i = 0; i < docscoresList.getLength(); i++) {
			Node node = docscoresList.item(i);

			String docscoreType = XMLTools.getAttributeValueOrThrowException(node, "type",
			    "Must specify a type for every docscore!");

			// Add model to internal map.
			if (docscores.get(docscoreType) != null) {
				throw new ConfigurationException(
						"Duplicate docscore types not allowed! Already parsed model with id="
								+ docscoreType);
			}
			docscores.put(docscoreType, node);
		}
	}

	private void parseJudgments(Document d) throws ConfigurationException {
		NodeList judgmentsList = d.getElementsByTagName("judgment");

		for (int i = 0; i < judgmentsList.getLength(); i++) {
			// Get XML node.
			Node node = judgmentsList.item(i);

			// Get query, document, and judgment.
			String qid = XMLTools.getAttributeValueOrThrowException(node, "qid",
			    "Each judgment must specify a qid attribute!");
			String docname = XMLTools.getAttributeValueOrThrowException(node, "doc",
			    "Each judgment must specify a doc attribute!");
			String grade = XMLTools.getAttributeValueOrThrowException(node, "grade",
			    "Each judgment must specify a grade attribute!");

			Map<String, Double> queryJudgments = judgments.get(qid);
			if (queryJudgments == null) {
				queryJudgments = Maps.newHashMap();
				judgments.put(qid, queryJudgments);
			}

			queryJudgments.put(docname, Double.parseDouble(grade));
		}
	}
}
