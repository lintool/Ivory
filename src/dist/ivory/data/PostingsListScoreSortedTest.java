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

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class PostingsListScoreSortedTest {

	@Test
	public void testBasic1() throws IOException {
		PostingsListScoreSorted postings = new PostingsListScoreSorted();
		postings.setCollectionDocumentCount(50);
		postings.setNumberOfPostings(9);

		postings.add(13, (short) 5, null);
		postings.add(1, (short) 2, null);
		postings.add(3, (short) 2, null);
		postings.add(10, (short) 2, null);
		postings.add(11, (short) 2, null);
		postings.add(5, (short) 1, null);
		postings.add(18, (short) 1, null);
		postings.add(20, (short) 1, null);
		postings.add(22, (short) 1, null);

		PostingsListScoreSorted decodedPostings = PostingsListScoreSorted.create(postings
				.serialize());
		decodedPostings.setCollectionDocumentCount(50);
		decodedPostings.setNumberOfPostings(9);

		Posting posting = new Posting();

		PostingsReader reader = decodedPostings.getPostingsReader();

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(13, posting.getDocno());
		assertEquals(5, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(1, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(3, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(10, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(11, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(5, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(18, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(20, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(22, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(false, reader.hasMorePostings());
		assertEquals(false, reader.nextPosting(posting));
	}

	@Test
	public void testPeeking() throws IOException {
		PostingsListScoreSorted postings = new PostingsListScoreSorted();
		postings.setCollectionDocumentCount(50);
		postings.setNumberOfPostings(9);

		postings.add(13, (short) 5, null);
		postings.add(1, (short) 2, null);
		postings.add(3, (short) 2, null);
		postings.add(10, (short) 2, null);
		postings.add(11, (short) 2, null);
		postings.add(5, (short) 1, null);
		postings.add(18, (short) 1, null);
		postings.add(20, (short) 1, null);
		postings.add(22, (short) 1, null);

		PostingsListScoreSorted decodedPostings = PostingsListScoreSorted.create(postings
				.serialize());
		decodedPostings.setCollectionDocumentCount(50);
		decodedPostings.setNumberOfPostings(9);

		Posting posting = new Posting();

		PostingsReader reader = decodedPostings.getPostingsReader();

		assertEquals(true, reader.hasMorePostings());
		
		// you should be able to peek as many times as you want
		assertEquals(13, reader.peekNextDocno());
		assertEquals(5, reader.peekNextScore());
		assertEquals(13, reader.peekNextDocno());
		assertEquals(5, reader.peekNextScore());
		assertEquals(13, reader.peekNextDocno());
		assertEquals(5, reader.peekNextScore());
		assertEquals(13, reader.peekNextDocno());
		assertEquals(5, reader.peekNextScore());

		reader.nextPosting(posting);
		assertEquals(13, posting.getDocno());
		assertEquals(5, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(1, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(3, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(10, reader.peekNextDocno());
		assertEquals(2, reader.peekNextScore());
		assertEquals(10, reader.peekNextDocno());
		assertEquals(2, reader.peekNextScore());
		
		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(10, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(11, posting.getDocno());
		assertEquals(2, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(5, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(18, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(20, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(22, reader.peekNextDocno());
		assertEquals(1, reader.peekNextScore());
		
		assertEquals(true, reader.hasMorePostings());
		reader.nextPosting(posting);
		assertEquals(22, posting.getDocno());
		assertEquals(1, posting.getScore());

		assertEquals(false, reader.hasMorePostings());
		assertEquals(false, reader.nextPosting(posting));
	}

	// test for not setting collection size

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PostingsListScoreSortedTest.class);
	}

}
