package ivory.sqe.retrieval;

import ivory.core.exception.ConfigurationException;
import ivory.core.util.ResultWriter;
import ivory.core.util.XMLTools;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.QueryRunner;
import ivory.sqe.querygenerator.ClQueryGenerator;
import ivory.sqe.querygenerator.DefaultBagOfWordQueryGenerator;
import java.io.IOException;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.DocnoMapping;

public class QueryEngine {
  private static final Logger LOG = Logger.getLogger(QueryEngine.class);
  private static JSONObject x = new JSONObject();
  private StructuredQueryRanker ranker;
  private Map<String, String> queries;
  private FileSystem fs;
  private Configuration conf;
  
  public QueryEngine(String[] args, FileSystem fs, Configuration conf) {
	  try {
		this.fs = fs;
		this.conf = conf;
		ranker = new StructuredQueryRanker(args[0], fs, 1000);
		queries = parseQueries(args[1], fs);
	} catch (IOException e) {
		e.printStackTrace();
	} catch (ConfigurationException e) {
		e.printStackTrace();
	}
  }

  private static Map<String, String> parseQueries(String qfile, FileSystem fs) throws ConfigurationException {
	  Document d = null;

	  try {
		  d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(fs.open(new Path(qfile)));
	  } catch (SAXException e) {
		  throw new ConfigurationException(e.getMessage());
	  } catch (IOException e) {
		  throw new ConfigurationException(e.getMessage());
	  } catch (ParserConfigurationException e) {
		  throw new ConfigurationException(e.getMessage());
	  }

	  Map<String, String> queries = Maps.newLinkedHashMap();

	  NodeList queryNodes = d.getElementsByTagName("query");

//	  JSONObject x1 = new JSONObject();
//	  JSONObject x2 = new JSONObject();
//	  JSONObject x11 = new JSONObject();
	  
	  
	  for (int i = 0; i < queryNodes.getLength(); i++) {
		  // Get query XML node.
		  Node node = queryNodes.item(i);

		  // Get query id.
		  String qid = XMLTools.getAttributeValueOrThrowException(node, "id",
		  "Must specify a query id attribute for every query!");

		  // Get query text.
		  String queryText = node.getTextContent();

//		  if(i==0){
//			  x1.put("#combine", new JSONArray(queryText.split(" ")));
//		  }
//		  if(i==1){
//			  x2.put("#combine", new JSONArray(queryText.split(" ")));
//		  }
//		  if(i==2){
//			  x11.append("#syn", queryText.split(" ")[0]);
//			  x11.append("#syn", queryText.split(" ")[1]);		  
//		  }
		  
		  // Add query to internal map.
		  if (queries.get(qid) != null) {
			  throw new ConfigurationException(
					  "Duplicate query ids not allowed! Already parsed query with id=" + qid);
		  }
		  queries.put(qid, queryText);
	  }
//	  x1.append("#combine", x11);
//	  x.append("#or", x1);
//	  x.append("#or", x2);

	  
	  return queries;
  }

  private void printResults(StructuredQueryRanker ranker, ResultWriter resultWriter) throws IOException {
	  DocnoMapping mapping = ranker.getDocnoMapping();
	  for (String queryID : queries.keySet()) {
		  // Get the ranked list for this query.
		  Accumulator[] list = ranker.getResults(queryID);
		  if (list == null) {
			  LOG.info("null results for: " + queryID);
			  continue;
		  }
		  for ( int i=0; i<list.length; i++) {
			  resultWriter.println(queryID + " Q0 " + mapping.getDocid(list[i].docno) + " " + (i + 1) + " "
					  + list[i].score + " Ivory");
		  }
	  }
  }

  public void runQueries() {
	  try {
		  ClQueryGenerator generator = new ClQueryGenerator(fs, conf	);

		  LOG.info("Parsed "+queries.size()+" queries");
		  
		  for ( String qid : queries.keySet()) {
			  String query = queries.get(qid);
			  LOG.info("Query "+qid+" = "+query);

			  JSONObject structuredQuery = generator.parseQuery(query);
			  
			  LOG.info(structuredQuery);
			  
			  long start = System.currentTimeMillis();
			  ranker.rank(qid, structuredQuery, generator.getQueryLength());

			  long end = System.currentTimeMillis();
			  LOG.info("Ranking " + qid + ": " + ( end - start) + "ms");
		  }
		  
		  // Where should we output these results?
//	      Node model = models.get(modelID);
//	      String fileName = XMLTools.getAttributeValue(model, "output", null);
//	      boolean compress = XMLTools.getAttributeValue(model, "compress", false);

	        ResultWriter resultWriter = new ResultWriter("sqe-bow-trec.txt", false, fs);
	        printResults(ranker, resultWriter);
	        resultWriter.flush();
	  } catch (IOException e) {
		  e.printStackTrace();
	  }
  }

}
