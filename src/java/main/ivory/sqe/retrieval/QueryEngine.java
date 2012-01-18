package ivory.sqe.retrieval;

import ivory.core.exception.ConfigurationException;
import ivory.core.util.ResultWriter;
import ivory.core.util.XMLTools;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.querygenerator.CLPhraseQueryGenerator;
import ivory.sqe.querygenerator.CLWordAndPhraseQueryGenerator;
import ivory.sqe.querygenerator.CLWordQueryGenerator;
import ivory.sqe.querygenerator.DefaultBagOfWordQueryGenerator;
import ivory.sqe.querygenerator.QueryGenerator;
import java.io.IOException;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.google.common.collect.Maps;
import edu.umd.cloud9.collection.DocnoMapping;

public class QueryEngine {
  private static final Logger LOG = Logger.getLogger(QueryEngine.class);
  private StructuredQueryRanker ranker;
  private Map<String, String> queries;
  private FileSystem fs;
  private QueryGenerator generator;
  private DocnoMapping mapping;

  public QueryEngine(Configuration conf, FileSystem fs) {
    try {
      this.fs = fs;
      LOG.info(conf.get(Constants.Language));
      LOG.info(conf.get(Constants.IndexPath));
      ranker = new StructuredQueryRanker(conf.get(Constants.IndexPath), fs, 1000);
      mapping = ranker.getDocnoMapping();
      queries = parseQueries(conf.get(Constants.QueriesPath), fs);
      if (conf.get(Constants.QueryType).equals(Constants.CLIR)) {
        generator = new CLWordQueryGenerator();
      } else if (conf.get(Constants.QueryType).equals(Constants.PhraseCLIR)) {
        generator = new CLPhraseQueryGenerator();
      } else if (conf.get(Constants.QueryType).equals(Constants.PhraseCLIRv2)) {
        generator = new CLWordAndPhraseQueryGenerator();
      } else {
        generator = new DefaultBagOfWordQueryGenerator();
      }

      generator.init(fs, conf);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
  }

  private static Map<String, String> parseQueries(String qfile, FileSystem fs)
      throws ConfigurationException {
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

    // JSONObject x1 = new JSONObject();
    // JSONObject x2 = new JSONObject();
    // JSONObject x11 = new JSONObject();

    LOG.info("Parsing " + queryNodes.getLength() + " nodes...");
    for (int i = 0; i < queryNodes.getLength(); i++) {
      // Get query XML node.
      Node node = queryNodes.item(i);

      // Get query id.
      String qid = XMLTools.getAttributeValueOrThrowException(node, "id",
          "Must specify a query id attribute for every query!");

      // Get query text.
      String queryText = node.getTextContent();

      // if(i==0){
      // x1.put("#combine", new JSONArray(queryText.split(" ")));
      // }
      // if(i==1){
      // x2.put("#combine", new JSONArray(queryText.split(" ")));
      // }
      // if(i==2){
      // x11.append("#syn", queryText.split(" ")[0]);
      // x11.append("#syn", queryText.split(" ")[1]);
      // }

      // Add query to internal map.
      if (queries.get(qid) != null) {
        throw new ConfigurationException(
            "Duplicate query ids not allowed! Already parsed query with id=" + qid);
      }
      queries.put(qid, queryText);
    }
    // x1.append("#combine", x11);
    // x.append("#or", x1);
    // x.append("#or", x2);

    return queries;
  }

  private void printResults(String queryID, StructuredQueryRanker ranker, ResultWriter resultWriter)
      throws IOException {
    // for (String queryID : queries.keySet()) {
    // Get the ranked list for this query.
    Accumulator[] list = ranker.getResults(queryID);
    if (list == null) {
      LOG.info("null results for: " + queryID);
      return;
    }
    for (int i = 0; i < list.length; i++) {
      resultWriter.println(queryID + " Q0 " + mapping.getDocid(list[i].docno) + " " + (i + 1) + " "
          + list[i].score + " sqe-bow-robust04");
    }
    // }
  }

  public void runQueries(Configuration conf) {
    try {

      LOG.info("Parsed " + queries.size() + " queries");
      ResultWriter resultWriter = new ResultWriter("sqe-" + conf.get(Constants.QueryType)
          + "-trec.txt", false, fs);

      for (String qid : queries.keySet()) {
        String query = queries.get(qid);
        LOG.info("Query " + qid + " = " + query);

        long start = System.currentTimeMillis();
        JSONObject structuredQuery = generator.parseQuery(query);
        long end = System.currentTimeMillis();
        LOG.info("Generating " + qid + ": " + (end - start) + "ms");
        LOG.info("Processing " + structuredQuery);

        start = System.currentTimeMillis();
        ranker.rank(qid, structuredQuery, generator.getQueryLength());
        end = System.currentTimeMillis();
        LOG.info("Ranking " + qid + ": " + (end - start) + "ms");

        printResults(qid, ranker, resultWriter);
      }
      resultWriter.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Map<String, Accumulator[]> getResults() {
    return ranker.getResults();
  }

  public DocnoMapping getDocnoMapping() {
    return mapping;
  }

}
