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

package ivory.data;

import static org.junit.Assert.assertEquals;
import ivory.index.TermPositions;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class PostingsListDocSortedPositionalTest {

	@Test
	public void testBasic1() throws IOException {
		PostingsListDocSortedPositional postings = new PostingsListDocSortedPositional();
		postings.setCollectionDocumentCount(10);
		postings.setNumberOfPostings(3);

		TermPositions tp1 = new TermPositions();
		int[] pos1 = { 1, 4, 5, 10, 23 };
		tp1.set(pos1, (short) 5);
		postings.add(13, (short) 5, tp1);

		TermPositions tp2 = new TermPositions();
		int[] pos2 = { 2, 23 };
		tp2.set(pos2, (short) 2);
		postings.add(14, (short) 2, tp2);

		TermPositions tp3 = new TermPositions();
		int[] pos3 = { 1 };
		tp3.set(pos3, (short) 1);
		postings.add(24, (short) 1, tp3);

		PostingsListDocSortedPositional decodedPostings = PostingsListDocSortedPositional
				.create(postings.serialize());
		decodedPostings.setCollectionDocumentCount(10);
		decodedPostings.setNumberOfPostings(3);

		assertEquals(3, decodedPostings.getDf());
		assertEquals(8, decodedPostings.getCf());

		Posting posting = new Posting();

		PostingsReader reader = decodedPostings.getPostingsReader();

		int arr[] = null;

		reader.nextPosting(posting);
		arr = reader.getPositions();
		assertEquals(13, posting.getDocno());
		assertEquals(5, posting.getScore());
		assertEquals(1, arr[0]);
		assertEquals(4, arr[1]);
		assertEquals(5, arr[2]);
		assertEquals(10, arr[3]);
		assertEquals(23, arr[4]);

		reader.nextPosting(posting);
		arr = reader.getPositions();
		assertEquals(14, posting.getDocno());
		assertEquals(2, posting.getScore());
		assertEquals(2, arr[0]);
		assertEquals(23, arr[1]);

		reader.nextPosting(posting);
		arr = reader.getPositions();
		assertEquals(24, posting.getDocno());
		assertEquals(1, posting.getScore());
		assertEquals(1, arr[0]);
	}

	// test for not setting collection size

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PostingsListDocSortedPositionalTest.class);
	}

}
