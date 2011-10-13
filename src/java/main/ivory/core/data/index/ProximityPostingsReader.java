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

import com.google.common.base.Preconditions;

/**
 * @author Don Metzler
 */
public abstract class ProximityPostingsReader implements PostingsReader {
  protected final PostingsReader[] readers;   // Readers for terms that make up ordered window.
  protected final int size;                   // Size of ordered window.

  public ProximityPostingsReader(PostingsReader[] readers, int size) {
    Preconditions.checkArgument(size > 0);
    this.readers = Preconditions.checkNotNull(readers);
    this.size = size;
  }

  public int getWindowSize() {
    return size;
  }

  /**
   * Returns {@code true} if current reader configuration represents a match.
   */
  private boolean isMatching() {
    int target = -1;
    for (PostingsReader reader : readers) {
      if (target == -1) {
        target = reader.getDocno();
      }

      // Did we match our target docid?
      if (reader.getDocno() != target) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int[] getPositions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getPositions(TermPositions tp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasMorePostings() {
    for (PostingsReader reader : readers) {
      if (!reader.hasMorePostings()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean nextPosting(Posting posting) {
    // Advance the reader at the minimum docno.
    PostingsReader minReader = null;
    PostingsReader maxReader = null;
    for (PostingsReader reader : readers) {
      int docno = reader.getDocno();
      if (minReader == null || docno < minReader.getDocno()) {
        minReader = reader;
      }
      if (maxReader == null || docno > maxReader.getDocno()) {
        maxReader = reader;
      }
    }
    if (minReader != null && minReader.hasMorePostings() && minReader.nextPosting(posting)) {
      if (posting.getDocno() > maxReader.getDocno()) {
        maxReader = minReader;
      }
    } else {
      return false;
    }

    posting.setDocno(maxReader.getDocno());
    if (isMatching()) {
      posting.setTf(countMatches());
    } else {
      posting.setTf((short) 0);
    }

    return true;
  }

  @Override
  public int peekNextDocno() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short peekNextTf() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDocno() {
    int maxDocno = Integer.MIN_VALUE;
    for (PostingsReader reader : readers) {
      int docno = reader.getDocno();
      if (docno > maxDocno) {
        maxDocno = docno;
      }
    }

    return maxDocno;
  }

  @Override
  public short getTf() {
    if (isMatching()) {
      return countMatches();
    }
    return 0;
  }

  @Override
  public void reset() {
    for (PostingsReader reader : readers) {
      reader.reset();
    }
  }

  @Override
  public int getNumberOfPostings() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PostingsList getPostingsList() {
    throw new UnsupportedOperationException();
  }

  abstract protected short countMatches();

  abstract protected int countMatches(int[] positions, int[] ids);
}
