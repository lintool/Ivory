package ivory.ffg.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import tl.lin.data.map.HMapIV;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

/**
 * Provides auxiliary functions for parsing qrel files.
 *
 * @author Nima Asadi
 */
public class QrelUtility {
  public static HMapIV<int[]> parseQrelsFromXML(String qrelPath)
    throws Exception {
    Preconditions.checkNotNull(qrelPath);

    return QrelUtility.loadQrelsFromXML(Files.newInputStreamSupplier(new File(qrelPath)));
  }

  public static HMapIV<int[]> parseQrelsFromTabDelimited(String qrelPath)
    throws Exception {
    Preconditions.checkNotNull(qrelPath);

    return QrelUtility.loadQrelsFromTabDelimited(Files.newReaderSupplier(new File(qrelPath),
                                                                          Charset.defaultCharset()));
  }

  public static HMapIV<long[]> parseLongQrelsFromTabDelimited(String qrelPath)
    throws Exception {
    Preconditions.checkNotNull(qrelPath);

    return QrelUtility.loadLongQrelsFromTabDelimited(Files.newReaderSupplier(new File(qrelPath),
                                                                             Charset.defaultCharset()));
  }

  /**
   * Reads a qrel set in XML format as follows:
   * &lt;parameters&gt;
   * &lt;judgment qid="Query_ID" docid="Document_ID" /&gt;
   * &lt;/parameters&gt;
   *
   * @param qrelInputSupplier An input supplier that provides the qrels
   * @return A map of query id to a list of document ids
   */
  public static HMapIV<int[]> loadQrelsFromXML(InputSupplier<? extends InputStream> qrelInputSupplier)
      throws ParserConfigurationException, SAXException, IOException {
    Preconditions.checkNotNull(qrelInputSupplier);

    Document dom = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(qrelInputSupplier.getInput());
    NodeList nodeList = dom.getDocumentElement().getElementsByTagName("judgment");

    if(nodeList == null) {
      return null;
    }

    HMapIV<List<Integer>> tempQrels = new HMapIV<List<Integer>>();
    for(int i = 0; i < nodeList.getLength(); i++) {
        Element element = (Element) nodeList.item(i);
        int qid = Integer.parseInt(element.getAttribute("qid"));
        int docid = Integer.parseInt(element.getAttribute("doc"));

        if(!tempQrels.containsKey(qid)) {
          List<Integer> list = Lists.newArrayList();
          tempQrels.put(qid, list);
        }
        tempQrels.get(qid).add(docid);
    }

    HMapIV<int[]> qrels = new HMapIV<int[]>();
    for(int key: tempQrels.keySet()) {
      List<Integer> list = tempQrels.get(key);
      int[] value = new int[list.size()];
      for(int i = 0; i < value.length; i++) {
        value[i] = list.get(i);
      }
      qrels.put(key, value);
    }

    return qrels;
  }

  public static HMapIV<int[]> loadQrelsFromTabDelimited(InputSupplier<? extends InputStreamReader> qrelInputSupplier)
      throws IOException {
    Preconditions.checkNotNull(qrelInputSupplier);

    HMapIV<List<Integer>> tempQrels = new HMapIV<List<Integer>>();
    BufferedReader reader = new BufferedReader(qrelInputSupplier.getInput());

    String line;
    while((line = reader.readLine()) != null) {
      String[] parts = line.split("\\s+");
      int qid = Integer.parseInt(parts[0]);
      int docid = Integer.parseInt(parts[1]);

      if(!tempQrels.containsKey(qid)) {
        List<Integer> list = Lists.newArrayList();
        tempQrels.put(qid, list);
      }
      tempQrels.get(qid).add(docid);
    }

    HMapIV<int[]> qrels = new HMapIV<int[]>();
    for(int key: tempQrels.keySet()) {
      List<Integer> list = tempQrels.get(key);
      int[] value = new int[list.size()];
      for(int i = 0; i < value.length; i++) {
        value[i] = list.get(i);
      }
      qrels.put(key, value);
    }

    return qrels;
  }

  public static HMapIV<long[]> loadLongQrelsFromTabDelimited(InputSupplier<? extends InputStreamReader> qrelInputSupplier)
      throws IOException {
    Preconditions.checkNotNull(qrelInputSupplier);

    HMapIV<List<Long>> tempQrels = new HMapIV<List<Long>>();
    BufferedReader reader = new BufferedReader(qrelInputSupplier.getInput());

    String line;
    while((line = reader.readLine()) != null) {
      String[] parts = line.split("\\s+");
      int qid = Integer.parseInt(parts[0]);
      long docid = Long.parseLong(parts[1]);

      if(!tempQrels.containsKey(qid)) {
        List<Long> list = Lists.newArrayList();
        tempQrels.put(qid, list);
      }
      tempQrels.get(qid).add(docid);
    }

    HMapIV<long[]> qrels = new HMapIV<long[]>();
    for(int key: tempQrels.keySet()) {
      List<Long> list = tempQrels.get(key);
      long[] value = new long[list.size()];
      for(int i = 0; i < value.length; i++) {
        value[i] = list.get(i);
      }
      qrels.put(key, value);
    }

    return qrels;
  }
}
