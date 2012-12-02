package ivory.bloomir.util;

import junit.framework.JUnit4TestAdapter;

import com.google.common.io.ByteStreams;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import edu.umd.cloud9.util.map.HMapIV;

public class QueryUtilityTest {
  @Test public void testEmpty() throws Exception {
    String queriesXML = "<parameters></parameters>";
    HMapIV<String> returned = QueryUtility.
      loadQueries(ByteStreams.newInputStreamSupplier(queriesXML.getBytes()));
    assertEquals(returned.size(), 0);
  }

  @Test public void testBasic() throws Exception {
    String queriesXML = "<parameters>\n" +
      "<query id=\"1\">text 1</query>\n" +
      "<query id=\"10\">text 10</query>\n" +
      "</parameters>";
    HMapIV<String> expected = new HMapIV<String>();
    expected.put(1, "text 1");
    expected.put(10, "text 10");

    HMapIV<String> returned = QueryUtility.
      loadQueries(ByteStreams.newInputStreamSupplier(queriesXML.getBytes()));

    assertEquals(returned.size(), expected.size());
    for(int qid: expected.keySet()) {
      assertTrue(returned.containsKey(qid));
      assertEquals(expected.get(qid), returned.get(qid));
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(QueryUtilityTest.class);
  }
}
