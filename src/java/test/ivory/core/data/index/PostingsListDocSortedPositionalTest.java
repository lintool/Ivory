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

import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsListDocSortedPositional;
import ivory.core.data.index.PostingsReader;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class PostingsListDocSortedPositionalTest {

  @Test
  public void testBasic1() throws IOException {
    PostingsListDocSortedPositional postings = new PostingsListDocSortedPositional();
    postings.setCollectionDocumentCount(10);
    postings.setNumberOfPostings(3);

    postings.add(13, (short) 5, new TermPositions(new int[] { 1, 4, 5, 10, 23 }, (short) 5));
    postings.add(14, (short) 2, new TermPositions(new int[] { 2, 23 }, (short) 2));
    postings.add(24, (short) 1, new TermPositions(new int[] { 1 }, (short) 1));

    PostingsListDocSortedPositional postings2 =
        PostingsListDocSortedPositional.create(postings.serialize());
    postings2.setCollectionDocumentCount(10);

    assertEquals(3, postings2.getDf());
    assertEquals(8, postings2.getCf());

    Posting posting = new Posting();

    PostingsReader reader = postings2.getPostingsReader();

    int arr[] = null;

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());
    assertEquals(1, arr[0]);
    assertEquals(4, arr[1]);
    assertEquals(5, arr[2]);
    assertEquals(10, arr[3]);
    assertEquals(23, arr[4]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());
    assertEquals(2, arr[0]);
    assertEquals(23, arr[1]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(24, posting.getDocno());
    assertEquals(1, posting.getTf());
    assertEquals(1, arr[0]);
  }

  @Test
  public void testBasic2() throws IOException {
    PostingsListDocSortedPositional postings = new PostingsListDocSortedPositional();
    postings.setCollectionDocumentCount(30);
    postings.setNumberOfPostings(3);

    postings.add(13, (short) 5, new TermPositions(new int[] { 1, 4, 5, 10, 23 }, (short) 5));
    postings.add(14, (short) 2, new TermPositions(new int[] { 2, 23 }, (short) 2));
    postings.add(24, (short) 1, new TermPositions(new int[] { 1 }, (short) 1));

    PostingsListDocSortedPositional postings2 =
        PostingsListDocSortedPositional.create(postings.serialize());
    postings2.setCollectionDocumentCount(30);

    // Verify tf and cf.
    assertEquals(3, postings2.getDf());
    assertEquals(8, postings2.getCf());

    Posting posting = new Posting();

    PostingsReader reader = postings2.getPostingsReader();

    int arr[] = null;

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());
    assertEquals(1, arr[0]);
    assertEquals(4, arr[1]);
    assertEquals(5, arr[2]);
    assertEquals(10, arr[3]);
    assertEquals(23, arr[4]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());
    assertEquals(2, arr[0]);
    assertEquals(23, arr[1]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(24, posting.getDocno());
    assertEquals(1, posting.getTf());
    assertEquals(1, arr[0]);

    // Set new tf and cf.
    postings2.setDf(6);
    postings2.setCf(16);

    // Verify tf and cf.
    assertEquals(6, postings2.getDf());
    assertEquals(16, postings2.getCf());

    reader = postings2.getPostingsReader();
    arr = null;

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());
    assertEquals(1, arr[0]);
    assertEquals(4, arr[1]);
    assertEquals(5, arr[2]);
    assertEquals(10, arr[3]);
    assertEquals(23, arr[4]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());
    assertEquals(2, arr[0]);
    assertEquals(23, arr[1]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(24, posting.getDocno());
    assertEquals(1, posting.getTf());
    assertEquals(1, arr[0]);

    PostingsListDocSortedPositional postings3 =
        PostingsListDocSortedPositional.create(postings2.serialize());
    postings3.setCollectionDocumentCount(20);

    // Verify tf and cf.
    assertEquals(6, postings2.getDf());
    assertEquals(16, postings2.getCf());

    reader = postings2.getPostingsReader();
    arr = null;

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(13, posting.getDocno());
    assertEquals(5, posting.getTf());
    assertEquals(1, arr[0]);
    assertEquals(4, arr[1]);
    assertEquals(5, arr[2]);
    assertEquals(10, arr[3]);
    assertEquals(23, arr[4]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(14, posting.getDocno());
    assertEquals(2, posting.getTf());
    assertEquals(2, arr[0]);
    assertEquals(23, arr[1]);

    reader.nextPosting(posting);
    arr = reader.getPositions();
    assertEquals(24, posting.getDocno());
    assertEquals(1, posting.getTf());
    assertEquals(1, arr[0]);
  }

  @Test
  public void testMerge1() throws IOException {
    // Create postings list 1.
    PostingsListDocSortedPositional postings1a = new PostingsListDocSortedPositional();
    postings1a.setCollectionDocumentCount(30);
    postings1a.setNumberOfPostings(3);

    postings1a.add(13, (short) 5, new TermPositions(new int[] { 1, 4, 5, 10, 23 }, (short) 5));
    postings1a.add(14, (short) 2, new TermPositions(new int[] { 2, 23 }, (short) 2));
    postings1a.add(24, (short) 1, new TermPositions(new int[] { 1 }, (short) 1));

    PostingsListDocSortedPositional postings1b =
        PostingsListDocSortedPositional.create(postings1a.serialize());

    postings1b.setCollectionDocumentCount(30);

    // Create postings list 2.
    PostingsListDocSortedPositional postings2a = new PostingsListDocSortedPositional();
    postings2a.setCollectionDocumentCount(30);
    postings2a.setNumberOfPostings(4);

    postings2a.add(2, (short) 3, new TermPositions(new int[] { 2, 10, 11 }, (short) 3));
    postings2a.add(11, (short) 4, new TermPositions(new int[] { 2, 23, 43, 69 }, (short) 4));
    postings2a.add(19, (short) 1, new TermPositions(new int[] { 1 }, (short) 1));
    postings2a.add(25, (short) 2, new TermPositions(new int[] { 10, 57 }, (short) 2));

    PostingsListDocSortedPositional postings2b =
        PostingsListDocSortedPositional.create(postings2a.serialize());

    postings2b.setCollectionDocumentCount(30);

    // Now merge and test.
    Posting p = new Posting();

    PostingsListDocSortedPositional merged;
    PostingsReader mergedReader;

    merged = PostingsListDocSortedPositional.create(PostingsListDocSortedPositional.merge(
        postings1b, postings2b, 30).serialize());

    merged.setCollectionDocumentCount(30);
    mergedReader = merged.getPostingsReader();

    assertEquals(7, merged.getNumberOfPostings());
    mergedReader.nextPosting(p);
    assertEquals(2, p.getDocno());
    assertEquals(3, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(11, p.getDocno());
    assertEquals(4, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(13, p.getDocno());
    assertEquals(5, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(14, p.getDocno());
    assertEquals(2, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(19, p.getDocno());
    assertEquals(1, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(24, p.getDocno());
    assertEquals(1, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(25, p.getDocno());
    assertEquals(2, p.getTf());

    merged = PostingsListDocSortedPositional.create(PostingsListDocSortedPositional.merge(
        postings2b, postings1b, 30).serialize());

    merged.setCollectionDocumentCount(30);
    mergedReader = merged.getPostingsReader();

    assertEquals(7, merged.getNumberOfPostings());
    mergedReader.nextPosting(p);
    assertEquals(2, p.getDocno());
    assertEquals(3, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(11, p.getDocno());
    assertEquals(4, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(13, p.getDocno());
    assertEquals(5, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(14, p.getDocno());
    assertEquals(2, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(19, p.getDocno());
    assertEquals(1, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(24, p.getDocno());
    assertEquals(1, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(25, p.getDocno());
    assertEquals(2, p.getTf());
  }

  @Test
  public void testMerge2() throws IOException {
    // Create postings list 1.
    PostingsListDocSortedPositional postings1a = new PostingsListDocSortedPositional();
    postings1a.setCollectionDocumentCount(30);
    postings1a.setNumberOfPostings(1);

    postings1a.add(13, (short) 5, new TermPositions(new int[] { 1, 4, 5, 10, 23 }, (short) 5));

    PostingsListDocSortedPositional postings1b =
        PostingsListDocSortedPositional.create(postings1a.serialize());

    postings1b.setCollectionDocumentCount(30);

    // Create postings list 2.
    PostingsListDocSortedPositional postings2a = new PostingsListDocSortedPositional();
    postings2a.setCollectionDocumentCount(30);
    postings2a.setNumberOfPostings(1);

    postings2a.add(2, (short) 3, new TermPositions(new int[] { 2, 10, 11 }, (short) 3));

    PostingsListDocSortedPositional postings2b =
        PostingsListDocSortedPositional.create(postings2a.serialize());

    postings2b.setCollectionDocumentCount(30);

    // Now merge and test.
    Posting p = new Posting();

    PostingsListDocSortedPositional merged;
    PostingsReader mergedReader;

    merged = PostingsListDocSortedPositional.create(PostingsListDocSortedPositional.merge(
        postings1b, postings2b, 30).serialize());

    merged.setCollectionDocumentCount(30);
    mergedReader = merged.getPostingsReader();

    assertEquals(2, merged.getNumberOfPostings());
    mergedReader.nextPosting(p);
    assertEquals(2, p.getDocno());
    assertEquals(3, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(13, p.getDocno());
    assertEquals(5, p.getTf());

    merged = PostingsListDocSortedPositional.create(PostingsListDocSortedPositional.merge(
        postings2b, postings1b, 30).serialize());

    merged.setCollectionDocumentCount(30);
    mergedReader = merged.getPostingsReader();

    assertEquals(2, merged.getNumberOfPostings());
    mergedReader.nextPosting(p);
    assertEquals(2, p.getDocno());
    assertEquals(3, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(13, p.getDocno());
    assertEquals(5, p.getTf());
  }

  @Test
  public void testMerge3() throws IOException {
    // Create postings list 1.
    PostingsListDocSortedPositional postings1a = new PostingsListDocSortedPositional();
    postings1a.setCollectionDocumentCount(30);
    postings1a.setNumberOfPostings(1);

    postings1a.add(13, (short) 5, new TermPositions(new int[] { 1, 4, 5, 10, 23 }, (short) 5));

    PostingsListDocSortedPositional postings1b =
        PostingsListDocSortedPositional.create(postings1a.serialize());

    postings1b.setCollectionDocumentCount(30);

    // Create postings list 2.
    PostingsListDocSortedPositional postings2a = new PostingsListDocSortedPositional();
    postings2a.setCollectionDocumentCount(30);
    postings2a.setNumberOfPostings(4);

    postings2a.add(2, (short) 3, new TermPositions(new int[] { 2, 10, 11 }, (short) 3));
    postings2a.add(11, (short) 4, new TermPositions(new int[] { 2, 23, 43, 69 }, (short) 4));
    postings2a.add(19, (short) 1, new TermPositions(new int[] { 1 }, (short) 1));
    postings2a.add(25, (short) 2, new TermPositions(new int[] { 10, 57 }, (short) 2));

    PostingsListDocSortedPositional postings2b =
        PostingsListDocSortedPositional.create(postings2a.serialize());

    postings2b.setCollectionDocumentCount(30);

    // Now merge and test.
    Posting p = new Posting();

    PostingsListDocSortedPositional merged;
    PostingsReader mergedReader;

    merged = PostingsListDocSortedPositional.create(PostingsListDocSortedPositional.merge(
        postings1b, postings2b, 30).serialize());

    merged.setCollectionDocumentCount(30);
    mergedReader = merged.getPostingsReader();

    assertEquals(5, merged.getNumberOfPostings());
    mergedReader.nextPosting(p);
    assertEquals(2, p.getDocno());
    assertEquals(3, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(11, p.getDocno());
    assertEquals(4, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(13, p.getDocno());
    assertEquals(5, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(19, p.getDocno());
    assertEquals(1, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(25, p.getDocno());
    assertEquals(2, p.getTf());

    merged = PostingsListDocSortedPositional.create(PostingsListDocSortedPositional.merge(
        postings2b, postings1b, 30).serialize());

    merged.setCollectionDocumentCount(30);
    mergedReader = merged.getPostingsReader();

    assertEquals(5, merged.getNumberOfPostings());
    mergedReader.nextPosting(p);
    assertEquals(2, p.getDocno());
    assertEquals(3, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(11, p.getDocno());
    assertEquals(4, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(13, p.getDocno());
    assertEquals(5, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(19, p.getDocno());
    assertEquals(1, p.getTf());

    mergedReader.nextPosting(p);
    assertEquals(25, p.getDocno());
    assertEquals(2, p.getTf());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PostingsListDocSortedPositionalTest.class);
  }
}
