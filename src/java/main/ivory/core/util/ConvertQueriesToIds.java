package ivory.core.util;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;

import java.util.Arrays;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

public class ConvertQueriesToIds {
  public static void main(String[] args) throws Exception {
    // Turn off all logging
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(ch.qos.logback.classic.Level.OFF);

    if (args.length < 2 ) {
      System.err.println("usage: [indexPath] [queries]...");
    }

    String indexPath = args[0];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    Map<String, String> queries = Maps.newLinkedHashMap();

    for (int n = 1; n < args.length; n++) {
      String qFile = args[n];

      Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(fs.open(new Path(qFile)));

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
    }

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    env.initialize(true);

    for (String queryID : queries.keySet()) {
      String rawQueryText = queries.get(queryID);
      String[] queryTokens = env.tokenize(rawQueryText);
      Integer[] termIds = new Integer[queryTokens.length]; 

      for (int i = 0; i < queryTokens.length; i++) {
        termIds[i] = env.getIdFromTerm(queryTokens[i]);
      }

      System.out.println("// " + queryID + " " + rawQueryText);
      System.out.println(queryID + ":" + Joiner.on(",").join(Arrays.asList(termIds)));
    }
  }
}
