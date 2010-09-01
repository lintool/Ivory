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

package ivory.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import edu.umd.cloud9.debug.WritableComparatorTestHarness;

public class TermScoreDocnoTest {

	@Test
	public void testComparison1() throws IOException {
		TermScoreDocno[] p = new TermScoreDocno[9];
		
		p[0] = new TermScoreDocno("book", (short) 2, 10003);
		p[1] = new TermScoreDocno("book", (short) 2, 10004);
		p[2] = new TermScoreDocno("zebra", (short) 20, 23);
		p[3] = new TermScoreDocno("apple", (short) 1, 3234);
		p[4] = new TermScoreDocno("apple", (short) 3, 9);
		p[5] = new TermScoreDocno("apple", (short) 3, 9);
		p[6] = new TermScoreDocno("book", (short) 5, 1);
		p[7] = new TermScoreDocno("zebra", (short) 3, 53);
		p[8] = new TermScoreDocno("book", (short) 1, 100);
		
		Arrays.sort(p);
				
		assertEquals("apple", p[0].getTerm());
		assertEquals(3, p[0].getScore());
		assertEquals(9, p[0].getDocno());
		
		assertEquals("apple", p[1].getTerm());
		assertEquals(3, p[1].getScore());
		assertEquals(9, p[1].getDocno());

		assertEquals("apple", p[2].getTerm());
		assertEquals(1, p[2].getScore());
		assertEquals(3234, p[2].getDocno());

		assertEquals("book", p[3].getTerm());
		assertEquals(5, p[3].getScore());
		assertEquals(1, p[3].getDocno());

		assertEquals("book", p[4].getTerm());
		assertEquals(2, p[4].getScore());
		assertEquals(10003, p[4].getDocno());

		assertEquals("book", p[5].getTerm());
		assertEquals(2, p[5].getScore());
		assertEquals(10004, p[5].getDocno());

		assertEquals("book", p[6].getTerm());
		assertEquals(1, p[6].getScore());
		assertEquals(100, p[6].getDocno());

		assertEquals("zebra", p[7].getTerm());
		assertEquals(20, p[7].getScore());
		assertEquals(23, p[7].getDocno());

		assertEquals("zebra", p[8].getTerm());
		assertEquals(3, p[8].getScore());
		assertEquals(53, p[8].getDocno());
	}

	@Test
	public void testComparison2() throws IOException {
		TermScoreDocno p1 = new TermScoreDocno("book", (short) 2, 10003);
		TermScoreDocno p2 = new TermScoreDocno("book", (short) 2, 10004);
		TermScoreDocno p3 = new TermScoreDocno("zebra", (short) 20, 23);
		TermScoreDocno p4 = new TermScoreDocno("apple", (short) 1, 3234);
		TermScoreDocno p5 = new TermScoreDocno("apple", (short) 3, 9);
		TermScoreDocno p6 = new TermScoreDocno("apple", (short) 3, 9);
		TermScoreDocno p7 = new TermScoreDocno("book", (short) 5, 1);
		TermScoreDocno p8 = new TermScoreDocno("zebra", (short) 3, 53);
		TermScoreDocno p9 = new TermScoreDocno("book", (short) 1, 100);
		
		assertTrue(p1.compareTo(p2) < 0);
		assertTrue(p1.compareTo(p3) < 0);
		assertTrue(p1.compareTo(p4) > 0);
		assertTrue(p1.compareTo(p5) > 0);
		assertTrue(p5.compareTo(p6) == 0);
		assertTrue(p5.compareTo(p7) < 0);
		assertTrue(p5.compareTo(p8) < 0);
		assertTrue(p5.compareTo(p9) < 0);
		assertTrue(p8.compareTo(p3) > 0);
		assertTrue(p8.compareTo(p1) > 0);
		assertTrue(p9.compareTo(p1) > 0);
		assertTrue(p9.compareTo(p2) > 0);		
	}

	@Test
	public void testComparison3() throws IOException {
		WritableComparator comparator = new TermScoreDocno.Comparator();

		TermScoreDocno p1 = new TermScoreDocno("book", (short) 2, 10003);
		TermScoreDocno p2 = new TermScoreDocno("book", (short) 2, 10004);
		TermScoreDocno p3 = new TermScoreDocno("zebra", (short) 20, 23);
		TermScoreDocno p4 = new TermScoreDocno("apple", (short) 1, 3234);
		TermScoreDocno p5 = new TermScoreDocno("apple", (short) 3, 9);
		TermScoreDocno p6 = new TermScoreDocno("apple", (short) 3, 9);
		TermScoreDocno p7 = new TermScoreDocno("book", (short) 5, 1);
		TermScoreDocno p8 = new TermScoreDocno("zebra", (short) 3, 53);
		TermScoreDocno p9 = new TermScoreDocno("book", (short) 1, 100);
		
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p2) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p3) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p5) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p5, p6) == 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p5, p7) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p5, p8) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p5, p9) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p8, p3) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p8, p1) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p9, p1) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p9, p2) > 0);		
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TermScoreDocnoTest.class);
	}

}
