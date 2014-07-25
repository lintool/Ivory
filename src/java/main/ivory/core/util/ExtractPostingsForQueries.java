package ivory.core.util;

import ivory.core.ConfigurationException;
import ivory.core.RetrievalEnvironment;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsListDocSortedPositional.PostingsReader;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ExtractPostingsForQueries {
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
    List<String> files = Lists.newArrayList();
    for (int n = 1; n < args.length; n++) {
      String qFile = args[n];
      files.add(qFile);
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

    System.out.println("# Postings dump of " + indexPath + " for queries " +
        Joiner.on(",").join(files) + ": " + new Date());
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
      if (postings == null) {
        continue;
      }

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
