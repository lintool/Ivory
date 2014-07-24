package ivory.core.util;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional.PostingsReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.collect.Maps;

public class ExtractPostingsForQueries {
  static {
//    List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//    loggers.add(LogManager.getRootLogger());
//    for ( Logger logger : loggers ) {
//        logger.setLevel(Level.OFF);
//    }
    
    // Turn off all logging
    Logger.getRootLogger().removeAllAppenders();
  }
  
  public static void main(String[] args) throws Exception {
//    List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//    loggers.add(LogManager.getRootLogger());
//    for ( Logger logger : loggers ) {
//        logger.setLevel(Level.OFF);
//    }
    
    // Turn off all logging
    //Logger.getRootLogger().removeAllAppenders();
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(ch.qos.logback.classic.Level.OFF);
    
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    Document d = null;
    String element = "data/trec/queries.robust04.xml";

    try {
      d = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(fs.open(new Path(element)));
    } catch (SAXException e) {
      throw new ConfigurationException(e.getMessage());
    } catch (IOException e) {
      throw new ConfigurationException(e.getMessage());
    } catch (ParserConfigurationException e) {
      throw new ConfigurationException(e.getMessage());
    }

    Map<String, String> queries = Maps.newLinkedHashMap();
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
        throw new ConfigurationException(
            "Duplicate query ids not allowed! Already parsed query with id=" + qid);
      }
      queries.put(qid, queryText);
    }

    String indexPath = "/scratch0/indexes/adhoc/trec/";
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    System.out.println("# Postings dump of " + indexPath + " for queries " + element + ": " + new Date());
    System.out.println("# docs: " + env.getDocumentCount());
    System.out.println("# collection size: " + env.getCollectionSize());
    System.out.println("# vocab size: " + env.getDictionary().size());

    SortedMap<String, Integer> terms = new TreeMap<String, Integer>();
    for (String queryID : queries.keySet()) {
      String rawQueryText = queries.get(queryID);
      String[] queryTokens = env.tokenize(rawQueryText);

      for (int i = 0; i < queryTokens.length; i++) {
        terms.put(queryTokens[i], env.getIdFromTerm(queryTokens[i]));
      }
    }

    for (Map.Entry<String, Integer> e : terms.entrySet()) {
      PostingsList postings = env.getPostingsList(e.getKey());
      PostingsReader r = (PostingsReader) postings.getPostingsReader();
      Posting p = new Posting();
      System.out.print(e.getValue() + "=");
      while (r.nextPosting(p)) {
        System.out.print(p.getDocno() + ":" + p.getTf() + ",");
      }
      System.out.println("");
    }
  }
}
