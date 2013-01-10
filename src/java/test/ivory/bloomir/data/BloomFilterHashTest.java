package ivory.bloomir.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BloomFilterHashTest {
  private static final int[] members = new int[]{
    10, 34, 3, 900, 832, 17, 436
  };

  @Test public void testOneHash() throws Exception {
    BloomFilterHash bloom = new BloomFilterHash(members.length * 8, 1);
    for(int i: members) {
      bloom.add(i);
    }

    for(int i: members) {
      assertTrue(bloom.membershipTest(i));
    }
  }

  @Test public void testTwoHash() throws Exception {
    BloomFilterHash bloom = new BloomFilterHash(members.length * 8, 2);
    for(int i: members) {
      bloom.add(i);
    }

    for(int i: members) {
      assertTrue(bloom.membershipTest(i));
    }
  }

  @Test public void testThreeHash() throws Exception {
    BloomFilterHash bloom = new BloomFilterHash(members.length * 8, 3);
    for(int i: members) {
      bloom.add(i);
    }

    for(int i: members) {
      assertTrue(bloom.membershipTest(i));
    }
  }

  @Test public void testIO() throws Exception {
    BloomFilterHash bloom = new BloomFilterHash(members.length * 16, 3);
    for(int i: members) {
      bloom.add(i);
    }

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    bloom.write(dataOut);
    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);
    BloomFilterHash bloomCopy = BloomFilterHash.readInstance(dataIn);

    assertEquals(bloom, bloomCopy);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(BloomFilterHashTest.class);
  }
}
