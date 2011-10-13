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

import java.io.IOException;

import junit.framework.TestCase;

import org.junit.Test;

public class PostingsListDocSortedNonPositionalTest extends TestCase {

  @Test
  public void testBasic1() throws IOException {
    PostingsListDocSortedNonPositional postings = new PostingsListDocSortedNonPositional();
    postings.setCollectionDocumentCount(10);
    postings.setNumberOfPostings(3);

    postings.add(13, (short) 5, null);
    postings.add(14, (short) 2, null);
    postings.add(24, (short) 1, null);

    PostingsListDocSortedNonPositional postings2 =
        PostingsListDocSortedNonPositional.create(postings.serialize());
    postings2.setCollectionDocumentCount(10);

    Posting posting = new Posting();

    PostingsReader reader = postings2.getPostingsReader();

    reader.nextPosting(posting);
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(24, posting.getDocno());
  }

  @Test
  public void testBasic2() throws IOException {
    PostingsListDocSortedNonPositional postings = new PostingsListDocSortedNonPositional();
    postings.setCollectionDocumentCount(20);
    postings.setNumberOfPostings(3);

    postings.add(13, (short) 5, null);
    postings.add(14, (short) 2, null);
    postings.add(24, (short) 1, null);

    PostingsListDocSortedNonPositional postings2 =
        PostingsListDocSortedNonPositional.create(postings.serialize());
    // verify cf and df
    assertEquals(8, postings2.getCf());
    assertEquals(3, postings2.getDf());
    
    postings2.setCollectionDocumentCount(20);

    // try decoding
    Posting posting = new Posting();
    PostingsReader reader = postings2.getPostingsReader();

    reader.nextPosting(posting);
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(24, posting.getDocno());
    
    // modify df and cf
    postings2.setCf(16);
    postings2.setDf(6);
    
    assertEquals(16, postings2.getCf());
    assertEquals(6, postings2.getDf());
    
    // try decoding again, should have an impact
    reader.reset();

    reader.nextPosting(posting);
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(24, posting.getDocno());
    
    PostingsListDocSortedNonPositional postings3 =
      PostingsListDocSortedNonPositional.create(postings2.serialize());
    // verify cf, df, and number of postings
    assertEquals(16, postings3.getCf());
    assertEquals(6, postings3.getDf());
    assertEquals(3, postings3.getNumberOfPostings());

    // try decoding
    postings3.setCollectionDocumentCount(20);
    
    reader = postings3.getPostingsReader();

    reader.nextPosting(posting);
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());

    reader.nextPosting(posting);
    assertEquals(24, posting.getDocno());
  }
  
  // TODO: test for not setting collection size
}
