package ivory.core.util;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional.PostingsReader;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.collect.Maps;

public class ExtractPostingsForQueries {
  public static void main(String[] args) throws Exception {
    // Turn off all logging
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(ch.qos.logback.classic.Level.OFF);

    if (args.length != 2 ) {
      System.err.println("usage: [indexPath] [queries]");
    }

    String qFile = args[0];
    String indexPath = args[1];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    Document d = null;

    try {
      d = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(fs.open(new Path(qFile)));
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

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    System.out.println("# Postings dump of " + indexPath + " for queries " + qFile + ": " + new Date());
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
