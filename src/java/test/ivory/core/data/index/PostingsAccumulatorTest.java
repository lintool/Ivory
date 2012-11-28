/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

package ivory.core.data.index;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class PostingsAccumulatorTest {
  @Test
  public void testSerialize() throws IOException {
    PostingsAccumulator p1 = new PostingsAccumulator();

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);

    int docno;
    int[] pos;

    docno = 1;
    pos = new int[2];
    pos[0] = 10;
    pos[1] = 20;
    p1.add(docno, pos);

    docno = 14;
    pos = new int[5];
    pos[0] = 10;
    pos[1] = 20;
    pos[2] = 30;
    pos[3] = 41;
    pos[4] = 55;
    p1.add(docno, pos);

    docno = 3;
    pos = new int[8];
    pos[0] = 11;
    pos[1] = 22;
    pos[2] = 33;
    pos[3] = 44;
    pos[4] = 53;
    pos[5] = 130;
    pos[6] = 141;
    pos[7] = 155;
    p1.add(docno, pos);

    p1.write(dataOut);

    assertEquals(3, p1.size());
    assertEquals(2, p1.getPositions()[0].length);
    assertEquals(20, p1.getPositions()[0][1]);

    assertEquals(5, p1.getPositions()[1].length);
    assertEquals(10, p1.getPositions()[1][0]);
    assertEquals(20, p1.getPositions()[1][1]);
    assertEquals(30, p1.getPositions()[1][2]);
    assertEquals(41, p1.getPositions()[1][3]);
    assertEquals(55, p1.getPositions()[1][4]);

    assertEquals(8, p1.getPositions()[2].length);
    assertEquals(11, p1.getPositions()[2][0]);
    assertEquals(22, p1.getPositions()[2][1]);
    assertEquals(33, p1.getPositions()[2][2]);
    assertEquals(44, p1.getPositions()[2][3]);
    assertEquals(53, p1.getPositions()[2][4]);
    assertEquals(130, p1.getPositions()[2][5]);
    assertEquals(141, p1.getPositions()[2][6]);
    assertEquals(155, p1.getPositions()[2][7]);

    PostingsAccumulator p2 = new PostingsAccumulator();
    p2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertEquals(3, p2.size());
    assertEquals(2, p2.getPositions()[0].length);
    assertEquals(20, p2.getPositions()[0][1]);

    assertEquals(5, p2.getPositions()[1].length);
    assertEquals(10, p2.getPositions()[1][0]);
    assertEquals(20, p2.getPositions()[1][1]);
    assertEquals(30, p2.getPositions()[1][2]);
    assertEquals(41, p2.getPositions()[1][3]);
    assertEquals(55, p2.getPositions()[1][4]);

    assertEquals(8, p2.getPositions()[2].length);
    assertEquals(11, p2.getPositions()[2][0]);
    assertEquals(22, p2.getPositions()[2][1]);
    assertEquals(33, p2.getPositions()[2][2]);
    assertEquals(44, p2.getPositions()[2][3]);
    assertEquals(53, p2.getPositions()[2][4]);
    assertEquals(130, p2.getPositions()[2][5]);
    assertEquals(141, p2.getPositions()[2][6]);
    assertEquals(155, p2.getPositions()[2][7]);

    p1.add(p2);
    assertEquals(6, p1.size());
    assertEquals(2, p1.getPositions()[3].length);
    assertEquals(20, p1.getPositions()[3][1]);

    bytesOut = new ByteArrayOutputStream();
    dataOut = new DataOutputStream(bytesOut);

    p1.write(dataOut);

    p2 = new PostingsAccumulator();
    p2.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertEquals(6, p2.size());
    assertEquals(2, p2.getPositions()[3].length);
    assertEquals(20, p2.getPositions()[3][1]);

    assertEquals(5, p2.getPositions()[4].length);
    assertEquals(10, p2.getPositions()[4][0]);
    assertEquals(20, p2.getPositions()[4][1]);
    assertEquals(30, p2.getPositions()[4][2]);
    assertEquals(41, p2.getPositions()[4][3]);
    assertEquals(55, p2.getPositions()[4][4]);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PostingsAccumulatorTest.class);
  }
}