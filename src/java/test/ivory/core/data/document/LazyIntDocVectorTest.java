/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.core.data.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import ivory.core.data.document.IntDocVector;
import ivory.core.data.document.LazyIntDocVector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.TreeMap;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;


import com.google.common.collect.Maps;

public class LazyIntDocVectorTest {

  @Test
  public void test1() throws IOException {
    TreeMap<Integer, int[]> terms = Maps.newTreeMap();
    terms.put(41083525, new int[] {9});
    terms.put(1, new int[] {2, 4, 6, 8});
    terms.put(6, new int[] {1});
    terms.put(29, new int[] {7});
    terms.put(5, new int[] {3, 5});

    IntDocVector v1 = new LazyIntDocVector(terms);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    v1.write(dataOut);

    IntDocVector v2 = new LazyIntDocVector();
    v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
    
    IntDocVector.Reader reader = v2.getReader();
    assertEquals(5, reader.getNumberOfTerms());
    assertTrue(reader.hasMoreTerms());

    assertEquals(1, reader.nextTerm());
    assertEquals(4, reader.getTf());

    assertEquals(5, reader.nextTerm());
    assertEquals(2, reader.getTf());

    assertEquals(6, reader.nextTerm());
    assertEquals(1, reader.getTf());

    assertEquals(29, reader.nextTerm());
    assertEquals(1, reader.getTf());

    assertEquals(41083525, reader.nextTerm());
    assertEquals(1, reader.getTf());

    assertFalse(reader.hasMoreTerms());

    // If I reset the reader, I should be able to start reading from the beginning again.
    reader.reset();
    assertEquals(5, reader.getNumberOfTerms());
    assertTrue(reader.hasMoreTerms());

    assertEquals(1, reader.nextTerm());
    assertEquals(4, reader.getTf());

    assertEquals(5, reader.nextTerm());
    assertEquals(2, reader.getTf());

    assertEquals(6, reader.nextTerm());
    assertEquals(1, reader.getTf());

    assertEquals(29, reader.nextTerm());
    assertEquals(1, reader.getTf());

    assertEquals(41083525, reader.nextTerm());
    assertEquals(1, reader.getTf());

    assertFalse(reader.hasMoreTerms());
  }

  @Test
  public void test2() throws IOException {
    TreeMap<Integer, int[]> terms = Maps.newTreeMap();
    IntDocVector v1 = new LazyIntDocVector(terms);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    v1.write(dataOut);

    IntDocVector v2 = new LazyIntDocVector();
    v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
    
    IntDocVector.Reader reader = v2.getReader();
    assertEquals(0, reader.getNumberOfTerms());
    assertFalse(reader.hasMoreTerms());
  }

  @Test
  public void test3() throws IOException {
    TreeMap<Integer, int[]> terms = Maps.newTreeMap();
    terms.put(41083525, new int[] {9});
    terms.put(1, new int[] {2, 4, 6, 8});
    terms.put(6, new int[] {1});
    terms.put(29, new int[] {7});
    terms.put(5, new int[] {3, 5});

    IntDocVector v1 = new LazyIntDocVector(terms);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    v1.write(dataOut);

    IntDocVector v2 = new LazyIntDocVector();
    v2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertEquals("[(1, 4, [2, 4, 6, 8])(5, 2, [3, 5])(6, 1, [1])(29, 1, [7])(41083525, 1, [9])]",
        v2.toString());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(LazyIntDocVectorTest.class);
  }
}
