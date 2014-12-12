package ivory.ffg.data;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.BeforeClass;
import org.junit.Test;

import tl.lin.data.map.HMapII;

public class DocumentVectorUtilityTest {
  private static final int[] document = new int[500];
  private static final HMapII counts = new HMapII();
  private static int[] terms;

  @BeforeClass public static void setUp() {
    for(int i = 0; i < document.length; i++) {
      document[i] = (int) (Math.random() * 100);
      if(!counts.containsKey(document[i])) {
        counts.put(document[i], 0);
      }
      counts.put(document[i], counts.get(document[i]) + 1);
    }

    terms = new int[counts.size()];
    int i = 0;
    for(int key: counts.keySet()) {
      terms[i++] = key;
    }
  }

  @Test public void testGetPositions() throws Exception {
    int[][] positions = DocumentVectorUtility.getPositions(document, terms);
    for(int i = 0; i < positions.length; i++) {
      assertEquals(positions[i].length, counts.get(terms[i]));
      for(int j = 0; j < positions[i].length; j++) {
        assertEquals(terms[i], document[positions[i][j] - 1]);
      }
    }
  }

  @Test public void testIO() throws Exception {
    int[][] positions = DocumentVectorUtility.getPositions(document, terms);
    for(int i = 0; i < positions.length; i++) {
      int[] pCopy = DocumentVectorUtility.
        deserializePositions(DocumentVectorUtility.serializePositions(positions[i]));
      assertEquals(positions[i].length, pCopy.length);
      for(int j = 0; j < positions[i].length; j++) {
        assertEquals(positions[i][j], pCopy[j]);
      }
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(DocumentVectorUtilityTest.class);
  }
}
