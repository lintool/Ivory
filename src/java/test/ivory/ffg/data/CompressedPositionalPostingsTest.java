package ivory.ffg.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Set;

import junit.framework.JUnit4TestAdapter;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import ivory.core.data.index.TermPositions;

public class CompressedPositionalPostingsTest {
  private static final int[] smallDataset = new int[]{
    10, 34, 36, 87, 436, 439, 783, 5643
  };
  private static final List<TermPositions> smallTermPositions =
    Lists.newArrayList();
  private static final int[] largeDataset = new int[200];
  private static final List<TermPositions> largeTermPositions =
    Lists.newArrayList();

  @BeforeClass public static void setUpSmallDataset() {
    Set<Integer> positions = Sets.newHashSet();

    for(int d: smallDataset) {
      int[] pos = new int[(int) (Math.random() * 3) + 1];
      for(int i = 0; i < pos.length; i++) {
        pos[i] = (int) (Math.random() * smallDataset.length * 100);
        if(positions.contains(pos[i])) {
          i--;
          continue;
        }
        if(i > 0) {
          if(pos[i] < pos[i - 1]) {
            i--;
            continue;
          }
        }
        positions.add(pos[i]);
      }
      smallTermPositions.add(new TermPositions(pos, (short) pos.length));
    }
  }

  @BeforeClass public static void setUpLargeDataset() {
    for(int i = 0; i < largeDataset.length; i++) {
      largeDataset[i] = i * 3 + 1;
    }

    Set<Integer> positions = Sets.newHashSet();
    for(int d: largeDataset) {
      int[] pos = new int[(int) (Math.random() * 3) + 1];
      for(int i = 0; i < pos.length; i++) {
        pos[i] = (int) (Math.random() * largeDataset.length * 100);
        if(positions.contains(pos[i])) {
          i--;
          continue;
        }
        if(i > 0) {
          if(pos[i] < pos[i - 1]) {
            i--;
            continue;
          }
        }
        positions.add(pos[i]);
      }
      largeTermPositions.add(new TermPositions(pos, (short) pos.length));
    }
  }

  @Test public void testSmallDataset() throws Exception {
    CompressedPositionalPostings postings =
      CompressedPositionalPostings.newInstance(smallDataset, smallTermPositions);

    for(int i = 0; i < smallDataset.length; i++) {
      int[] pos = postings.decompressPositions(i);
      assertEquals(pos.length, smallTermPositions.get(i).getPositions().length);
      for(int j = 0; j < pos.length; j++) {
        assertEquals(pos[j], smallTermPositions.get(i).getPositions()[j]);
      }
    }
  }

  @Test public void testLargeDataset() throws Exception {
    CompressedPositionalPostings postings =
      CompressedPositionalPostings.newInstance(largeDataset, largeTermPositions);

    for(int i = 0; i < largeDataset.length; i++) {
      int[] pos = postings.decompressPositions(i);
      assertEquals(pos.length, largeTermPositions.get(i).getPositions().length);
      for(int j = 0; j < pos.length; j++) {
        assertEquals(pos[j], largeTermPositions.get(i).getPositions()[j]);
      }
    }
  }

  @Test public void testIO() throws Exception {
    CompressedPositionalPostings postings =
      CompressedPositionalPostings.newInstance(largeDataset, largeTermPositions);

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    postings.write(dataOut);
    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);
    CompressedPositionalPostings postingsCopy =
      CompressedPositionalPostings.readInstance(dataIn);

    assertEquals(postings, postingsCopy);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(CompressedPositionalPostingsTest.class);
  }
}
