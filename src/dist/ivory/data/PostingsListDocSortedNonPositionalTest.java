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

public class PostingsListDocSortedNonPositionalTest {

	@Test
	public void testBasic1() throws IOException {
		PostingsListDocSortedNonPositional postings = new PostingsListDocSortedNonPositional();
		postings.setCollectionDocumentCount(10);
		postings.setNumberOfPostings(3);

		postings.add(13, (short) 5, null);
		postings.add(14, (short) 2, null);
		postings.add(24, (short) 1, null);

		PostingsListDocSortedNonPositional decodedPostings = PostingsListDocSortedNonPositional.create(postings.serialize());
		decodedPostings.setCollectionDocumentCount(10);
		decodedPostings.setNumberOfPostings(3);

		Posting posting = new Posting();

		PostingsReader reader = decodedPostings.getPostingsReader();

		reader.nextPosting(posting);
		assertEquals(13, posting.getDocno());
		assertEquals(5, posting.getScore());

		reader.nextPosting(posting);
		assertEquals(14, posting.getDocno());
		assertEquals(2, posting.getScore());

		reader.nextPosting(posting);
		assertEquals(24, posting.getDocno());
	}

	// test for not setting collection size

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PostingsListDocSortedNonPositionalTest.class);
	}

}
