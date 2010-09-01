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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import edu.umd.cloud9.debug.WritableComparatorTestHarness;

public class IntTermDocnoTest {

	@Test
	public void testComparison1() throws IOException {
		IntTermDocno p1 = new IntTermDocno(2, 10003);
		IntTermDocno p2 = new IntTermDocno(2, 10004);
		IntTermDocno p3 = new IntTermDocno(2, 23);
		IntTermDocno p4 = new IntTermDocno(1, 4);
		IntTermDocno p5 = new IntTermDocno(10, 3);

		assertTrue(p1.compareTo(p2) < 0);
		assertTrue(p1.compareTo(p3) > 0);
		assertTrue(p1.compareTo(p4) > 0);
		assertTrue(p1.compareTo(p5) < 0);
	}

	@Test
	public void testComparison2() throws IOException {
		WritableComparator comparator = new IntTermDocno.Comparator();

		IntTermDocno p1 = new IntTermDocno(2, 10003);
		IntTermDocno p2 = new IntTermDocno(2, 10004);
		IntTermDocno p3 = new IntTermDocno(2, 23);
		IntTermDocno p4 = new IntTermDocno(1, 4);
		IntTermDocno p5 = new IntTermDocno(10, 3);

		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p2) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p3) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, p1, p5) < 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(IntTermDocnoTest.class);
	}

}
