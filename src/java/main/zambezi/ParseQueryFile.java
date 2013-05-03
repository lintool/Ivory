package zambezi;

import ivory.core.tokenize.GalagoTokenizer;
import ivory.core.util.XMLTools;

import java.util.Map;
import java.util.SortedMap;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

public class ParseQueryFile {

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    SortedMap<Integer, String> map = Maps.newTreeMap();
    Document d = null;

    FileSystem fs = FileSystem.getLocal(new Configuration());
    d = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(fs.open(new Path("data/cacm/queries.cacm.xml")));

    NodeList queryNodes = d.getElementsByTagName("query");

    for (int i = 0; i < queryNodes.getLength(); i++) {
      // Get query XML node.
      Node node = queryNodes.item(i);

      // Get query id.
      String qid = XMLTools.getAttributeValueOrThrowException(node, "id",
          "Must specify a query id attribute for every query!");

      // Get query text.
      String queryText = node.getTextContent();
      map.put(Integer.parseInt(qid), queryText);
    }
    
    GalagoTokenizer tokenizer = new GalagoTokenizer();

    System.out.println(map.size());
    for (Map.Entry<Integer, String> entry : map.entrySet()) {
      String[] terms = tokenizer.processContent(entry.getValue());
      System.out.println(entry.getKey() + " " + terms.length + " " + Joiner.on(" ").join(terms));
    }
  }

}
