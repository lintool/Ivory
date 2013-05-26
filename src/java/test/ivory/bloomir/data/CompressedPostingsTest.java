package ivory.bloomir.data;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.JUnit4TestAdapter;

import org.junit.BeforeClass;
import org.junit.Test;

public class CompressedPostingsTest {
  private static final int[] smallDataset = new int[]{
    10, 34, 36, 87, 436, 439, 783, 5643
  };

  private static final int[] largeDataset = new int[3000];
  @BeforeClass public static void setUp() {
    for(int i = 0; i < largeDataset.length; i++) {
      largeDataset[i] = i * 3 + 1;
    }
  }

  @Test 
  public void testSmallDataset() throws Exception {
    CompressedPostings postings = CompressedPostings.newInstance(smallDataset);

    assertEquals(postings.getBlockCount(),
                 (int) Math.ceil(((float) smallDataset.length) / CompressedPostings.getBlockSize()));
    assertEquals(postings.getBlockStartIndex(0), 0);
    assertEquals(postings.isFirstElementInBlock(0), true);
    assertEquals(postings.isFirstElementInBlock(1), false);

    int[] decomp = new int[CompressedPostings.getBlockSize()];
    int size = postings.decompressBlock(decomp, 0);
    assertEquals(size, smallDataset.length);
    int docid = 0;
    for (int i = 0; i < size; i++) {
      docid += decomp[i];
      assertEquals(docid, smallDataset[i]);
      assertEquals(postings.getBlockNumber(i), 0);
      assertEquals(postings.getPositionInBlock(i), i);
    }
  }

  @Test
  public void testLargeDataset() throws Exception {
    CompressedPostings postings = CompressedPostings.newInstance(largeDataset);

    assertEquals(postings.getBlockCount(),
        (int) Math.ceil(((float) largeDataset.length) / CompressedPostings.getBlockSize()));
    assertEquals(postings.getBlockStartIndex(0), 0);
    assertEquals(postings.getBlockStartIndex(1), CompressedPostings.getBlockSize());
    assertEquals(postings.isFirstElementInBlock(CompressedPostings.getBlockSize()), true);
    assertEquals(postings.isFirstElementInBlock(CompressedPostings.getBlockSize() + 1), false);

    int[] decomp = new int[CompressedPostings.getBlockSize()];
    for (int i = 0; i < postings.getBlockCount(); i++) {
      int size = postings.decompressBlock(decomp, i);
      int docid = 0;
      for (int j = 0; j < size; j++) {
        docid += decomp[j];
        assertEquals(docid, largeDataset[i * CompressedPostings.getBlockSize() + j]);
        assertEquals(postings.getBlockNumber(i * CompressedPostings.getBlockSize() + j), i);
        assertEquals(postings.getPositionInBlock(i * CompressedPostings.getBlockSize() + j), j);
      }
    }
  }

  @Test
  public void testIO() throws Exception {
    CompressedPostings postings = CompressedPostings.newInstance(largeDataset);

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    postings.write(dataOut);
    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);
    CompressedPostings postingsCopy = CompressedPostings.readInstance(dataIn);

    assertEquals(postings, postingsCopy);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(CompressedPostingsTest.class);
  }
}
