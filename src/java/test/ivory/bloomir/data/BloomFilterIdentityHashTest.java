package ivory.bloomir.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BloomFilterIdentityHashTest {
  private static final int[] members = new int[]{
    10, 34, 3, 900, 832, 17, 436
  };

  @Test public void testMembershipTest() throws Exception {
    BloomFilterIdentityHash bloom = new BloomFilterIdentityHash(1000);
    for(int i: members) {
      bloom.add(i);
    }

    for(int i: members) {
      assertTrue(bloom.membershipTest(i));
    }
  }

  @Test public void testIO() throws Exception {
    BloomFilterIdentityHash bloom = new BloomFilterIdentityHash(1000);
    for(int i: members) {
      bloom.add(i);
    }

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    bloom.write(dataOut);
    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);
    BloomFilterIdentityHash bloomCopy = BloomFilterIdentityHash.readInstance(dataIn);

    assertEquals(bloom, bloomCopy);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(BloomFilterIdentityHashTest.class);
  }
}
