package ivory.ffg.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import junit.framework.JUnit4TestAdapter;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ivory.core.data.document.IntDocVector;
import ivory.core.data.document.LazyIntDocVector;

public class DocumentVectorTest {
  private static final String[] documentVectorClass = new String[] {
    "ivory.ffg.data.DocumentVectorHashedArray",
    "ivory.ffg.data.DocumentVectorMiniInvertedIndex",
    "ivory.ffg.data.DocumentVectorPForDeltaArray",
    "ivory.ffg.data.DocumentVectorVIntArray"
  };

  private static IntDocVector intDocVector;
  private static final int[] document = new int[500];
  private static final SortedMap<Integer, int[]> indexedDocument =
    new TreeMap<Integer, int[]>();
  private static int[] terms;

  @BeforeClass public static void setUp() throws Exception {
    Map<Integer, List<Integer>> map = Maps.newHashMap();
    for(int i = 0; i < document.length; i++) {
      document[i] = (int) (Math.random() * 70000) + 1;
      if(!map.containsKey(document[i])) {
        List<Integer> list = Lists.newArrayList();
        map.put(document[i], list);
      }
      map.get(document[i]).add(i + 1);
    }

    for(int key: map.keySet()) {
      int[] positions = new int[map.get(key).size()];
      int i = 0;
      for(int pos: map.get(key)) {
        positions[i++] = pos;
      }
      indexedDocument.put(key, positions);
    }

    terms = new int[map.size()];
    int i = 0;
    for(int key: map.keySet()) {
      terms[i++] = key;
    }

    intDocVector = new LazyIntDocVector(indexedDocument);

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    intDocVector.write(dataOut);
    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);
    intDocVector = new LazyIntDocVector();
    intDocVector.readFields(dataIn);
  }

  @Test public void testDecompressDocuments() throws Exception {
    for(String dvclass: documentVectorClass) {
      DocumentVector dv = DocumentVectorUtility.newInstance(dvclass, intDocVector);
      assertEquals(document.length, dv.getDocumentLength());

      try {
        int[] transformedDoc = dv.decompressDocument();
        int[] transformedTerms = dv.transformTerms(terms);

        int[][] positions = DocumentVectorUtility.
          getPositions(transformedDoc, transformedTerms);

        for(int i = 0; i < positions.length; i++) {
          assertEquals(indexedDocument.get(terms[i]).length, positions[i].length);
          for(int j = 0; j < positions[i].length; j++) {
            assertEquals(terms[i], document[positions[i][j] - 1]);
          }
        }
      } catch(UnsupportedOperationException e) {
        continue;
      }
    }
  }

  @Test public void testDecompressPositions() throws Exception {
    for(String dvclass: documentVectorClass) {
      DocumentVector dv = DocumentVectorUtility.newInstance(dvclass, intDocVector);

      try {
        int[][] positions = dv.decompressPositions(terms);
        if(dvclass.contains("mini")) {
          for(int i = 0; i < positions.length; i++) {
            for(int j = 0; j < positions[i].length; j++) {
              System.out.print(positions[i][j] + " ");
            }
            System.out.println();
          }
        }

        for(int i = 0; i < positions.length; i++) {
          assertEquals(indexedDocument.get(terms[i]).length, positions[i].length);
          for(int j = 0; j < positions[i].length; j++) {
            assertEquals(terms[i], document[positions[i][j] - 1]);
          }
        }
      } catch(UnsupportedOperationException e) {
        continue;
      }
    }
  }

  @Test public void testIO() throws Exception {
    for(String dvclass: documentVectorClass) {
      DocumentVector dv = DocumentVectorUtility.newInstance(dvclass, intDocVector);

      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(byteOut);
      dv.write(dataOut);
      dataOut.close();

      ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
      DataInputStream dataIn = new DataInputStream(byteIn);
      DocumentVector dvCopy = DocumentVectorUtility.readInstance(dvclass, dataIn);

      assertEquals(dv, dvCopy);
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(DocumentVectorTest.class);
  }
}
