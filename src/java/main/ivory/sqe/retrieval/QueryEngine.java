package ivory.sqe.retrieval;

import ivory.core.exception.ConfigurationException;
import ivory.core.util.XMLTools;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.querygenerator.DefaultBagOfWordQueryGenerator;
import java.io.IOException;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.google.common.collect.Maps;

public class QueryEngine {
	private static JSONObject x = new JSONObject();

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

  public static void main(String args[]){
	  try {
		  FileSystem fs = FileSystem.get(new Configuration());
		  DefaultBagOfWordQueryGenerator generator = new DefaultBagOfWordQueryGenerator();
		  StructuredQueryRanker ranker = new StructuredQueryRanker(args[0], fs, 1000);

		  Map<String, String> queries = parseQueries(args[1], fs);
		  System.out.println("Parsed "+queries.size()+" queries");
		  
		  for ( String qid : queries.keySet()) {
			  String query = queries.get(qid);
			  System.out.println("Query "+qid+" = "+query);

			  JSONObject structuredQuery = generator.parseQuery(query);

			  long start = System.currentTimeMillis();
			  Accumulator[] results = ranker.rank(structuredQuery, generator.getQueryLength());

			  long end = System.currentTimeMillis();
			  System.out.println("Ranking " + qid + ": " + ( end - start) + "ms");

			  for ( int i=0; i<results.length; i++) {
				  System.out.println(qid + " Q0 " + ranker.getDocnoMapping().getDocid(results[i].docno) + " " + (i + 1) + " "
						  + results[i].score + " Ivory");
			  }
		  }
	  } catch (IOException e) {
		  e.printStackTrace();
	  } catch (ConfigurationException e) {
		  e.printStackTrace();
	  }	
	  System.exit(0);
  }

}
