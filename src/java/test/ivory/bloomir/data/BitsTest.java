package ivory.bloomir.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BitsTest {
  @Test public void testString() throws Exception {
    Bits bits = new Bits(3);
    assertEquals(bits.size(), 3);
    assertEquals(bits.toString(), "000");

    bits.set(1);
    assertEquals(bits.toString(), "010");

    bits.set(2);
    bits.clear(1);
    assertEquals(bits.toString(), "001");

    bits = new Bits(33);
    assertEquals(bits.size(), 33);
    bits.set(32, true);
    assertEquals(bits.toString(), "000000000000000000000000000000001");
  }

  @Test public void testAnd() throws Exception {
    Bits bits1 = new Bits(10, true);
    Bits bits2 = new Bits(10);
    bits2.set(1);

    bits1.and(bits2);
    assertEquals(bits1, bits2);
  }

  @Test public void testOr() throws Exception {
    Bits bits1 = new Bits(10, true);
    Bits bits2 = new Bits(10);
    bits2.set(1);

    bits2.or(bits1);
    assertEquals(bits1, bits2);
  }

  @Test public void testGet() throws Exception {
    Bits bits1 = new Bits(10);
    bits1.set(8);
    bits1.set(2);

    assertFalse(bits1.get(1));
    assertTrue(bits1.get(2));
    assertTrue(bits1.get(8));
  }

  @Test public void testGetBits() throws Exception {
    Bits bits = new Bits(40);
    bits.set(31);
    bits.set(32);
    bits.set(34);

    assertEquals(bits.getBits(34, 34), 1);
    assertEquals(bits.getBits(32, 34), 5);
    assertEquals(bits.getBits(30, 35), 0x1A);
  }

  @Test public void testCount() throws Exception {
    Bits bits = new Bits(100);
    assertEquals(bits.count(), 0);

    bits.set(99);
    assertEquals(bits.count(), 1);

    bits.clear(99);
    assertEquals(bits.count(), 0);
  }

  @Test public void testGetOnes() throws Exception {
    Bits bits = new Bits(100);
    assertEquals(bits.getOnes().length, 0);

    int[] indexes = new int[] {0, 24, 99};
    for(int i = 0; i < indexes.length; i++) {
      bits.set(indexes[i]);
    }

    int[] ones = bits.getOnes();
    assertEquals(ones.length, indexes.length);
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(ones[i], indexes[i]);
    }
  }

  @Test public void testEquals() throws Exception {
    Bits bits1 = new Bits(40);
    Bits bits2 = new Bits(42);
    assertFalse(bits1.equals(bits2));

    bits1.set(9);
    bits2 = new Bits(40);
    bits2.set(10);
    assertFalse(bits1.equals(bits2));

    bits1.clear(9);
    bits1.set(10);
    assertEquals(bits1, bits2);
  }

  @Test public void testIO() throws Exception {
    Bits bits = new Bits(40);
    int[] indexes = new int[]{0, 5, 10, 38};
    for(int i: indexes) {
      bits.set(i);
    }

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);
    bits.write(dataOut);
    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);
    Bits bitsCopy = new Bits();
    bitsCopy.readFields(dataIn);

    assertEquals(bits, bitsCopy);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(BitsTest.class);
  }
}
