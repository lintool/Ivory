package ivory.ffg.feature;

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
import ivory.ffg.data.DocumentVector;
import ivory.ffg.data.DocumentVectorUtility;
import ivory.ffg.score.TfScoringFunction;

public class FeatureTest {
  private static final String[] documentVectorClass = new String[] {
    "ivory.ffg.data.DocumentVectorHashedArray",
    "ivory.ffg.data.DocumentVectorMiniInvertedIndex",
    "ivory.ffg.data.DocumentVectorPForDeltaArray",
    "ivory.ffg.data.DocumentVectorVIntArray"
  };

  private static final Map<Feature, Map<int[], Float>> features =
    Maps.newHashMap();

  private static IntDocVector intDocVector;
  private static int[] document = null;
  private static final SortedMap<Integer, int[]> indexedDocument =
    new TreeMap<Integer, int[]>();

  @BeforeClass public static void setUpFeatures() {
    Map<int[], Float> termFeatures = Maps.newHashMap();
    termFeatures.put(new int[] {100}, 4.0f);
    termFeatures.put(new int[] {101}, 3.0f);
    termFeatures.put(new int[] {429}, 1.0f);
    features.put(new TermFeature(), termFeatures);

    Map<int[], Float> od1SdFeatures = Maps.newHashMap();
    od1SdFeatures.put(new int[] {100, 101}, 1f);
    od1SdFeatures.put(new int[] {32, 100}, 1f);
    od1SdFeatures.put(new int[] {15, 380}, 0f);
    od1SdFeatures.put(new int[] {101, 100}, 2f);
    od1SdFeatures.put(new int[] {100, 4}, 1f);
    od1SdFeatures.put(new int[] {100, 4, 43}, 2f);
    features.put(new OrderedWindowSequentialDependenceFeature(1), od1SdFeatures);

    Map<int[], Float> od8SdFeatures = Maps.newHashMap();
    od8SdFeatures.put(new int[] {100, 101}, 4f);
    od8SdFeatures.put(new int[] {32, 100}, 1f);
    od8SdFeatures.put(new int[] {15, 380}, 1f);
    od8SdFeatures.put(new int[] {101, 100}, 2f);
    od8SdFeatures.put(new int[] {100, 4}, 3f);
    od8SdFeatures.put(new int[] {100, 4, 43}, 4f);
    features.put(new OrderedWindowSequentialDependenceFeature(8), od8SdFeatures);

    Map<int[], Float> uw1SdFeatures = Maps.newHashMap();
    uw1SdFeatures.put(new int[] {100, 101}, 3f);
    uw1SdFeatures.put(new int[] {32, 100}, 1f);
    uw1SdFeatures.put(new int[] {15, 380}, 0f);
    uw1SdFeatures.put(new int[] {101, 100}, 3f);
    uw1SdFeatures.put(new int[] {100, 4}, 0f);
    uw1SdFeatures.put(new int[] {100, 4, 43}, 0f);
    features.put(new UnorderedWindowSequentialDependenceFeature(1), uw1SdFeatures);

    Map<int[], Float> uw8SdFeatures = Maps.newHashMap();
    uw8SdFeatures.put(new int[] {100, 101}, 6f);
    uw8SdFeatures.put(new int[] {32, 100}, 3f);
    uw8SdFeatures.put(new int[] {15, 380}, 1f);
    uw8SdFeatures.put(new int[] {101, 100}, 6f);
    uw8SdFeatures.put(new int[] {100, 4}, 4f);
    uw8SdFeatures.put(new int[] {100, 4, 43}, 5f);
    features.put(new UnorderedWindowSequentialDependenceFeature(8), uw8SdFeatures);
  }

  @BeforeClass public static void setUpIntDocVector() throws Exception {
    document = new int[] {
      100, 73500, 429, 101, 100,
      32, 48, 100, 101, 100,
      7300, 4, 11, 43, 101,
      15, 1, 12, 380, 400
    };

    Map<Integer, List<Integer>> map = Maps.newHashMap();
    for(int i = 0; i < document.length; i++) {
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

  @Test public void testFeaturesSlidingWindow() throws Exception {
    for(String dvclass: documentVectorClass) {
      DocumentVector dv = DocumentVectorUtility.newInstance(dvclass, intDocVector);

      try {
        int[] transformedDoc = dv.decompressDocument();

        for(Feature f: features.keySet()) {
          f.initialize(new TfScoringFunction());

          for(int[] query: features.get(f).keySet()) {
            int[] transformedTerms = dv.transformTerms(query);
            float fValue = f.
              computeScoreWithSlidingWindow(transformedDoc, query, transformedTerms, null);
            assertEquals(features.get(f).get(query), fValue, 1e-10);
          }
        }
      } catch(UnsupportedOperationException e) {
        continue;
      }
    }
  }

  @Test public void testFeaturesWithMiniIndexing() throws Exception {
    for(String dvclass: documentVectorClass) {
      DocumentVector dv = DocumentVectorUtility.newInstance(dvclass, intDocVector);

      try {
        for(Feature f: features.keySet()) {
          f.initialize(new TfScoringFunction());

          for(int[] query: features.get(f).keySet()) {
            int[][] positions = dv.decompressPositions(query);
            float fValue = f.
              computeScoreWithMiniIndexes(positions, query, dv.getDocumentLength(), null);
            assertEquals(features.get(f).get(query), fValue, 1e-10);
          }
        }
      } catch(UnsupportedOperationException e) {
        continue;
      }
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(FeatureTest.class);
  }
}
