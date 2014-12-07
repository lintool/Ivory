package ivory.bloomir.util;

import ivory.core.RetrievalEnvironment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import tl.lin.data.map.HMapIF;
import tl.lin.data.map.HMapIV;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

/**
 * Provides auxiliary functions for query processing.
 *
 * @author Nima Asadi
 */
public class QueryUtility {
  /**
   * Loads and tokenizes a set of queries.
   *
   * @param queryPath Path to the file containing the queries
   * @return Map of query id to an array of strings (i.e., the tokens
   * of the original query)
   */
  public static HMapIV<String[]> parseQueries(RetrievalEnvironment env, String queryPath)
      throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryPath);

    HMapIV<String> queries = QueryUtility.loadQueries(Files.newInputStreamSupplier(new File(queryPath)));
    HMapIV<String[]> parsedQueries = new HMapIV<String[]>();

    for (int qid: queries.keySet()) {
      parsedQueries.put(qid, env.tokenize(queries.get(qid)));
    }
    return parsedQueries;
  }

  /**
   * Loads IDF values for the set of terms extracted from a query file.
   *
   * @param env RetrievalEnvironment object associated with the index
   * @param queryPath Path to the query file
   * @return A Map of term ids to its idf.
   */
  public static HMapIF loadIdf(RetrievalEnvironment env, String  queryPath)
      throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryPath);

    HMapIF idfs = new HMapIF();
    HMapIV<String> parsedQueries = QueryUtility.loadQueries(Files.newInputStreamSupplier(new File(queryPath)));

    for(int qid: parsedQueries.keySet()) {
      String[] tokens = env.tokenize(parsedQueries.get(qid));

      if(tokens == null) {
        continue;
      }

      for(String term: tokens) {
        float idf = (float) Math.log((env.getDocumentCount() - env.documentFrequency(term) + 0.5f) /
            (env.documentFrequency(term) + 0.5f));
        idfs.put(env.getIdFromTerm(term), idf);
      }
    }

    return idfs;
  }

  public static HMapIF loadIdf(RetrievalEnvironment env, HMapIV<String> parsedQueries)
    throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(parsedQueries);

    HMapIF idfs = new HMapIF();

    for(int qid: parsedQueries.keySet()) {
      String[] tokens = env.tokenize(parsedQueries.get(qid));

      if(tokens == null) {
        continue;
      }

      for(String term: tokens) {
        float idf = (float) Math.log((env.getDocumentCount() - env.documentFrequency(term) + 0.5f) /
                                     (env.documentFrequency(term) + 0.5f));
        idfs.put(env.getIdFromTerm(term), idf);
      }
    }

    return idfs;
  }

  /**
   * Loads CF values for the set of terms extracted from a query file.
   *
   * @param env RetrievalEnvironment object associated with the index
   * @param queryPath Path to the query file
   * @return A Map of term ids to its cf values.
   */
  public static HMapIF loadCf(RetrievalEnvironment env, String  queryPath)
    throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryPath);

    HMapIF cfs = new HMapIF();
    HMapIV<String> parsedQueries = QueryUtility.loadQueries(Files.newInputStreamSupplier(new File(queryPath)));

    for(int qid: parsedQueries.keySet()) {
      String[] tokens = env.tokenize(parsedQueries.get(qid));

      if(tokens == null) {
        continue;
      }

      for(String term: tokens) {
        float cf = (float) env.collectionFrequency(term);
        cfs.put(env.getIdFromTerm(term), cf);
      }
    }

    return cfs;
  }

  public static HMapIF loadCf(RetrievalEnvironment env, HMapIV<String> parsedQueries)
    throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(parsedQueries);

    HMapIF cfs = new HMapIF();

    for(int qid: parsedQueries.keySet()) {
      String[] tokens = env.tokenize(parsedQueries.get(qid));

      if(tokens == null) {
        continue;
      }

      for(String term: tokens) {
        float cf = (float) env.collectionFrequency(term);
        cfs.put(env.getIdFromTerm(term), cf);
      }
    }

    return cfs;
  }

  /**
   * Reads and converts queries into their integer forms.
   *
   * @param env RetrievalEnvironment object associated with the index
   * @param queryPath Path to the query file
   * @return A Map of query ids to a list of integers. Each integer i is the code
   * for term i in the query. Note that depending on the tokenizer, stopwords may
   * be removed and/or stemming might apply to the given query phrase.
   */
  public static HMapIV<int[]> queryToIntegerCode(RetrievalEnvironment env, String queryPath)
      throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(queryPath);

    HMapIV<int[]> queries = new HMapIV<int[]>();
    HMapIV<String> parsedQueries = QueryUtility.loadQueries(Files.newInputStreamSupplier(new File(queryPath)));

    for(int qid: parsedQueries.keySet()) {
      String[] tokens = env.tokenize(parsedQueries.get(qid));

      if(tokens == null) {
        continue;
      }

      List<Integer> qList = Lists.newArrayList();
      for(int i = 0; i < tokens.length; i++) {
        int code = env.getIdFromTerm(tokens[i]);
        if(code > 0) {
          qList.add(code);
        }
      }

      int[] tempTerms = new int[qList.size()];
      for(int i = 0; i < qList.size(); i++) {
        tempTerms[i] = qList.get(i);
      }

      queries.put(qid, tempTerms);
    }

    return queries;
  }

  public static HMapIV<int[]> queryToIntegerCode(RetrievalEnvironment env, HMapIV<String> parsedQueries)
    throws Exception {
    Preconditions.checkNotNull(env);
    Preconditions.checkNotNull(parsedQueries);

    HMapIV<int[]> queries = new HMapIV<int[]>();

    for(int qid: parsedQueries.keySet()) {
      String[] tokens = env.tokenize(parsedQueries.get(qid));

      if(tokens == null) {
        continue;
      }

      List<Integer> qList = Lists.newArrayList();
      for(int i = 0; i < tokens.length; i++) {
        int code = env.getIdFromTerm(tokens[i]);
        if(code > 0) {
          qList.add(code);
        }
      }

      int[] tempTerms = new int[qList.size()];
      for(int i = 0; i < qList.size(); i++) {
        tempTerms[i] = qList.get(i);
      }

      queries.put(qid, tempTerms);
    }

    return queries;
  }

  /**
   * Loads a set of queries from an input file.
   *
   * @param queryPath Path to the query file
   * @return A Map of query id to query text
   */
  public static HMapIV<String> loadQueries(String queryPath)
    throws Exception {
    Preconditions.checkNotNull(queryPath);
    return QueryUtility.loadQueries(Files.newInputStreamSupplier(new File(queryPath)));
  }

  /**
   * Reads a query set in an XML format as follows:
   * &lt;parameters&gt;
   * &lt;query qid="Query_ID"&gt;Query Text&lt;/query&gt;
   * &lt;/parameters&gt;
   *
   * @param queryInputSupplier An input supplier that provides the queries
   * @return A map of query id to query text
   */
  public static HMapIV<String> loadQueries(InputSupplier<? extends InputStream> queryInputSupplier)
      throws ParserConfigurationException, SAXException, IOException {
    Preconditions.checkNotNull(queryInputSupplier);

    HMapIV<String> queries = new HMapIV<String>();
    Document dom = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(queryInputSupplier.getInput());
    NodeList nodeList = dom.getDocumentElement().getElementsByTagName("query");

    if(nodeList == null) {
      return null;
    }

    for(int i = 0; i < nodeList.getLength(); i++) {
        Element element = (Element) nodeList.item(i);
        int qid = Integer.parseInt(element.getAttribute("id"));
        String text = element.getFirstChild().getNodeValue();
        queries.put(qid, text);
    }

    return queries;
  }
}
