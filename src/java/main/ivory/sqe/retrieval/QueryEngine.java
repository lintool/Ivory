package ivory.sqe.retrieval;

import ivory.core.ConfigurationException;
import ivory.core.util.ResultWriter;
import ivory.core.util.XMLTools;
import ivory.smrf.retrieval.Accumulator;
import ivory.sqe.querygenerator.CLWordQueryGenerator;
import ivory.sqe.querygenerator.DefaultBagOfWordQueryGenerator;
import ivory.sqe.querygenerator.MtNQueryGenerator;
import ivory.sqe.querygenerator.QueryGenerator;
import ivory.sqe.querygenerator.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
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
  private Set<String> modelSet;
  private QueryGenerator generator;
  private DocnoMapping mapping;
  private String runName;
  private Map<String, Map<String, Accumulator[]>> allResults;
  
  public QueryEngine() {
    allResults = Maps.newHashMap();
    modelSet = new HashSet<String>();
  }
  
  public QueryEngine(Configuration conf, FileSystem fs) {
    allResults = Maps.newHashMap();
    modelSet = new HashSet<String>();
    init(conf, fs);
  }

  public void init(Configuration conf, FileSystem fs){
    try {
      this.fs = fs;
      if (conf.getBoolean(Constants.Quiet, false)) {
        LOG.setLevel(Level.OFF);
      }
      runName = Utils.getSetting(conf);
      modelSet.add(runName);
      
      ranker = new StructuredQueryRanker(conf.get(Constants.IndexPath), fs, 1000);
      mapping = ranker.getDocnoMapping();
      queries = parseQueries(conf.get(Constants.QueriesPath), fs);
      if (conf.get(Constants.QueryType).equals(Constants.CLIR)) {
        generator = new CLWordQueryGenerator();
      }else if (conf.get(Constants.QueryType).equals(Constants.MTN)) {
        generator = new MtNQueryGenerator();
      }else {
        generator = new DefaultBagOfWordQueryGenerator();
      }    
      generator.init(fs, conf);

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (ConfigurationException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void init(Configuration conf){
    try {
      generator.init(conf);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
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

    LOG.info("Parsing "+queryNodes.getLength()+" nodes...");
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
        throw new ConfigurationException(
            "Duplicate query ids not allowed! Already parsed query with id=" + qid);
      }
      queries.put(qid, queryText);
    }
   
    return queries;
  }

  public Map<String, Map<String, Accumulator[]>> getAllResults() {   
    return allResults;
  }
  
  private void printResults(String queryID, String runName, StructuredQueryRanker ranker, ResultWriter resultWriter) throws IOException {
    // Get the ranked list for this query.
    Accumulator[] list = ranker.getResults(queryID);
    if (list == null) {
      LOG.info("null allResults for: " + queryID);
      return;
    }
    if ( list.length == 0 ) {
      resultWriter.println(queryID + " Q0 x 1 0 " + runName);
    }
    for ( int i=0; i<list.length; i++) {
      resultWriter.println(queryID + " Q0 " + mapping.getDocid(list[i].docno) + " " + (i + 1) + " "
          + list[i].score + " " + runName);
    }
  }

  public void runQueries(Configuration conf) {
    runName = Utils.getSetting(conf);
    
    try {
      LOG.info("Parsed "+queries.size()+" queries");

      ResultWriter resultWriter = new ResultWriter("ranking." + runName + ".txt", false, fs);
      float rankTime = 0, generateTime = 0;
      for ( String qid : queries.keySet()) {
        String query = queries.get(qid);

        long start = System.currentTimeMillis();
        JSONObject structuredQuery = generator.parseQuery(query);
        long end = System.currentTimeMillis();
        LOG.info("Generating " + qid + ": " + ( end - start) + "ms");
        generateTime += ( end - start ) ;
        LOG.info("<Processed>:::" + runName + ":::" + qid + ":::" + structuredQuery);

        start = System.currentTimeMillis();
        ranker.rank(qid, structuredQuery, generator.getQueryLength());
        end = System.currentTimeMillis();
        LOG.info("Ranking " + qid + ": " + ( end - start) + "ms");
        rankTime += ( end - start ) ;
        printResults(qid, runName, ranker, resultWriter);
        
        // save allResults
        allResults.put(runName, getResults());
      }   
      resultWriter.flush();
      LOG.info("<TIME>:::" + runName + ":::" + generateTime + ":::" + rankTime);
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

  
  public Set<String> getModels() {
    return modelSet;
  }

}
