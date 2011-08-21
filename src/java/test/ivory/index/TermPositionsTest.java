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


import ivory.core.index.TermPositions;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;


public class TermPositionsTest {

	@Test
	public void testBasic1() throws IOException {
		TermPositions tp = new TermPositions();
		int[] pos = { 1, 4, 5, 10, 23 };
		tp.set(pos, (short) 5);

		assertEquals(tp.getTf(), 5);
	}

	@Test
	public void testSerial1() throws IOException {
		TermPositions tp = new TermPositions();
		int[] pos = { 1, 4, 5, 10, 23 };
		tp.set(pos, (short) 5);

		assertEquals(5, tp.getTf());

		TermPositions tp2 = TermPositions.create(tp.serialize());

		assertEquals(5, tp2.getTf());
		int[] pos2 = tp2.getPositions();

		assertEquals(1, pos2[0]);
		assertEquals(4, pos2[1]);
		assertEquals(5, pos2[2]);
		assertEquals(10, pos2[3]);
		assertEquals(23, pos2[4]);

		assertEquals(17, tp.getEncodedSize());
	}

	@Test
	public void testSerial2() throws IOException {
		TermPositions tp = new TermPositions();
		int[] pos = { 1, 4, 5, 10, 23, 324 };

		// if the int array is longer than tf, ignore the extra ints
		tp.set(pos, (short) 5);

		assertEquals(5, tp.getTf());

		byte[] bytes = tp.serialize();
		TermPositions tp2 = TermPositions.create(bytes);

		assertEquals(5, tp2.getTf());
		int[] pos2 = tp2.getPositions();

		assertEquals(1, pos2[0]);
		assertEquals(4, pos2[1]);
		assertEquals(5, pos2[2]);
		assertEquals(10, pos2[3]);
		assertEquals(23, pos2[4]);
		assertEquals(5, pos2.length);
	}

	@Test
	public void testSerial3() throws IOException {
		// Check large term positions for possible overflow. This would show up
		// if we were using shorts somewhere in the code for term positions.
		TermPositions tp = new TermPositions();
		int[] pos = { 1, 4, 5311782, 5311783, 98921257 };
		tp.set(pos, (short) 5);

		assertEquals(5, tp.getTf());

		byte[] bytes = tp.serialize();
		TermPositions tp2 = TermPositions.create(bytes);

		assertEquals(5, tp2.getTf());
		int[] pos2 = tp2.getPositions();

		assertEquals(1, pos2[0]);
		assertEquals(4, pos2[1]);
		assertEquals(5311782, pos2[2]);
		assertEquals(5311783, pos2[3]);
		assertEquals(98921257, pos2[4]);
	}

	@Test(expected = RuntimeException.class)
	public void tesDuplicateDocnos() throws IOException {
		TermPositions tp = new TermPositions();
		// illegal positions
		int[] pos = { 1, 4, 5, 5, 23 };

		tp.set(pos, (short) 5);
		assertEquals(5, tp.getTf());

		tp.serialize();
	}

	@Test(expected = RuntimeException.class)
	public void testIllegalPositions() throws IOException {
		TermPositions tp = new TermPositions();
		// illegal positions
		int[] pos = { 1, 4, 5, -2, -1 };

		tp.set(pos, (short) 5);
		assertEquals(5, tp.getTf());

		tp.serialize();
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TermPositionsTest.class);
	}

}
