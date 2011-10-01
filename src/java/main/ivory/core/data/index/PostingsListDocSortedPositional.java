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

import ivory.core.compression.BitInputStream;
import ivory.core.compression.BitOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Object representing a document-sorted postings list that holds positional information for terms.
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class PostingsListDocSortedPositional implements PostingsList {
  private static final Logger LOG = Logger.getLogger(PostingsListDocSortedPositional.class);
  private static final int MAX_DOCNO_BITS = 32;

  static {
    LOG.setLevel(Level.WARN);
  }

  private int collectionDocumentCount = -1;
  private int numPostings = -1;
  private int golombParam;
  private int prevDocno;
  private byte[] rawBytes;
  private int postingsAdded;
  private long sumOfPostingsScore;

  private int df;
  private long cf;

  transient private ByteArrayOutputStream bytesOut;
  transient private BitOutputStream bitsOut;

  public PostingsListDocSortedPositional() {
    this.sumOfPostingsScore = 0;
    this.postingsAdded = 0;
    this.df = 0;
    this.cf = 0;
    this.prevDocno = -1;

    try {
      bytesOut = new ByteArrayOutputStream();
      bitsOut = new BitOutputStream(bytesOut);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    sumOfPostingsScore = 0;
    postingsAdded = 0;
    df = 0;
    cf = 0;
    prevDocno = -1;
    numPostings = -1;
    rawBytes = null;
    try {
      bytesOut = new ByteArrayOutputStream();
      bitsOut = new BitOutputStream(bytesOut);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void add(int docno, short tf, TermPositions pos) {
    Preconditions.checkArgument(pos.getPositions().length != 0);
    Preconditions.checkArgument(tf == pos.getTf());

    try {
      if (postingsAdded == 0) {
        // Write out the first docno.
        bitsOut.writeBinary(MAX_DOCNO_BITS, docno);
        bitsOut.writeGamma(tf);
        writePositions(bitsOut, pos, docno, tf);

        prevDocno = docno;
      } else {
        // Use d-gaps for subsequent docnos.
        int dgap = docno - prevDocno;

        if (dgap <= 0) {
          throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
        }

        bitsOut.writeGolomb(dgap, golombParam);
        bitsOut.writeGamma(tf);
        writePositions(bitsOut, pos, docno, tf);

        prevDocno = docno;
      }
    } catch (IOException e) {
      throw new RuntimeException("Error adding postings.");
    } catch (ArithmeticException e) {
      throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
          + "\": check to see if collection size or df is set properly. docno=" + docno
          + ", tf=" + tf + ", previous docno=" + prevDocno + ", df=" + numPostings
          + ", collection size=" + collectionDocumentCount + ", Golomb param=" + golombParam);
    }

    postingsAdded++;
    sumOfPostingsScore += tf;
  }

  // passing in docno and tf basically for error checking purposes
  private static void writePositions(BitOutputStream t, TermPositions p, int docno, short tf)
      throws IOException {
    int[] pos = p.getPositions();

    if (tf != p.getTf()) {
      throw new RuntimeException(String.format(
          "Error: tf and number of positions don't match. docno=%d, tf=%d, positions=%s",
          docno, tf, pos.toString()));
    }

    if (p.getTf() == 1) {
      // If tf=1, just write out the single term position.
      t.writeGamma(pos[0]);
    } else {
      // If tf > 1, write out skip information if we want to bypass the positional information
      // during decoding.
      t.writeGamma(p.getEncodedSize());

      // Keep track of where we are in the stream.
      int skip1 = (int) t.getByteOffset() * 8 + t.getBitOffset();

      if (pos[0] <= 0) {
        throw new RuntimeException(String.format(
            "Error: invalid term positions. docno=%d, tf=%d, positions=%s",
            docno, tf, pos.toString()));
      }
      // Write out first position.
      t.writeGamma(pos[0]);
      // Write out rest of positions using p-gaps (first order positional differences).
      for (int c = 1; c < p.getTf(); c++) {
        int pgap = pos[c] - pos[c - 1];
        if (pos[c] <= 0 || pgap == 0) {
          throw new RuntimeException(String.format(
              "Error: invalid term positions. docno=%d, tf=%d, positions=%s",
              docno, tf, pos.toString()));
        }
        t.writeGamma(pgap);
      }

      // Find out where we are in the stream.
      int skip2 = (int) t.getByteOffset() * 8 + t.getBitOffset();

      // Verify that the skip information is indeed valid.
      if (skip1 + p.getEncodedSize() != skip2) {
        throw new RuntimeException("Ivalid skip information: skip1=" + skip1
            + ", skip2=" + skip2 + ", size=" + p.getEncodedSize());
      }
    }
  }

  @Override
  public int size() {
    return postingsAdded;
  }

  @Override
  public PostingsReader getPostingsReader() {
    Preconditions.checkNotNull(rawBytes);
    Preconditions.checkArgument(collectionDocumentCount > 0);
    Preconditions.checkArgument(postingsAdded > 0);

    try {
      return new PostingsReader(rawBytes, postingsAdded, collectionDocumentCount, this);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public byte[] getRawBytes() {
    return rawBytes;
  }

  @Override
  public void setCollectionDocumentCount(int docs) {
    Preconditions.checkArgument(docs > 0);

    collectionDocumentCount = docs;
    recomputeGolombParameter();
  }

  @Override
  public int getCollectionDocumentCount() {
    return collectionDocumentCount;
  }

  @Override
  public void setNumberOfPostings(int n) {
    numPostings = n;
    recomputeGolombParameter();
  }

  @Override
  public int getNumberOfPostings() {
    return numPostings;
  }

  private void recomputeGolombParameter() {
    golombParam = (int) Math.ceil(
        0.69 * ((float) collectionDocumentCount) / (float) numPostings);
  }

  @Override
  public int getDf() {
    return df;
  }

  @Override
  public void setDf(int df) {
    this.df = df;
  }

  @Override
  public long getCf() {
    return cf;
  }

  @Override
  public void setCf(long cf) {
    this.cf = cf;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    postingsAdded = WritableUtils.readVInt(in);
    numPostings = postingsAdded;

    df = WritableUtils.readVInt(in);
    cf = WritableUtils.readVLong(in);
    sumOfPostingsScore = cf;

    rawBytes = new byte[WritableUtils.readVInt(in)];
    in.readFully(rawBytes);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (rawBytes != null) {
      // This would happen if we're reading in an already-encoded postings; if that's the case,
      // simply write out the byte array.
      WritableUtils.writeVInt(out, postingsAdded);
      WritableUtils.writeVInt(out, df == 0 ? postingsAdded : df);
      WritableUtils.writeVLong(out, cf == 0 ? sumOfPostingsScore : cf);
      WritableUtils.writeVInt(out, rawBytes.length);
      out.write(rawBytes);
    } else {
      try {
        bitsOut.padAndFlush();
        bitsOut.close();

        if (numPostings != postingsAdded) {
          throw new RuntimeException(
              "Error: number of postings added doesn't match number of expected postings. Expected "
                  + numPostings + ", got " + postingsAdded);
        }

        WritableUtils.writeVInt(out, postingsAdded);
        WritableUtils.writeVInt(out, df == 0 ? postingsAdded : df);
        WritableUtils.writeVLong(out, cf == 0 ? sumOfPostingsScore : cf);
        byte[] bytes = bytesOut.toByteArray();
        WritableUtils.writeVInt(out, bytes.length);
        out.write(bytes);
      } catch (ArithmeticException e) {
        throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
            + "\": check to see if collection size or df is set properly.");
      }

      LOG.info("writing postings: cf=" + sumOfPostingsScore + ", df=" + numPostings);
    }
  }

  public byte[] serialize() throws IOException {
    Preconditions.checkArgument(postingsAdded > 0);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  public static PostingsListDocSortedPositional create(DataInput in) throws IOException {
    PostingsListDocSortedPositional p = new PostingsListDocSortedPositional();
    p.readFields(in);

    return p;
  }

  public static PostingsListDocSortedPositional create(byte[] bytes) throws IOException {
    return PostingsListDocSortedPositional.create(
        new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public static String positionsToString(int[] pos) {
    StringBuffer sb = new StringBuffer();
    sb.append("[");

    for (int i = 0; i < pos.length; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(pos[i]);
    }
    sb.append("]");

    return sb.toString();
  }

  /**
   * {@code PostingsReader} for {@code PostingsListDocSortedPositional}.
   *
   * @author Jimmy Lin
   */
  public static class PostingsReader implements ivory.core.data.index.PostingsReader {
    private ByteArrayInputStream bytesIn;
    private BitInputStream bitsIn;
    private int cnt = 0;
    private short prevTf;
    private int[] curPositions;
    private int innerPrevDocno;
    private int innerNumPostings;
    private int innerGolombParam;
    private int innerCollectionSize;
    private boolean needToReadPositions = false;
    private PostingsList postingsList;

    protected PostingsReader(byte[] bytes, int numPostings, int collectionSize,
        PostingsListDocSortedPositional list) throws IOException {
      Preconditions.checkNotNull(bytes);
      Preconditions.checkArgument(numPostings > 0);
      Preconditions.checkArgument(collectionSize > 0);

      bytesIn = new ByteArrayInputStream(bytes);
      bitsIn = new BitInputStream(bytesIn);

      innerNumPostings = numPostings;
      innerCollectionSize = collectionSize;
      innerGolombParam = (int) Math.ceil(0.69 * ((float) innerCollectionSize)
          / (float) innerNumPostings);
      postingsList = list;
      needToReadPositions = false;
    }

    @Override
    public int getNumberOfPostings() {
      return innerNumPostings;
    }

    @Override
    public void reset() {
      try {
        bytesIn.reset();
        bitsIn = new BitInputStream(bytesIn);
        cnt = 0;
        needToReadPositions = false;
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error resetting postings.");
      }
    }

    @Override
    public boolean nextPosting(Posting p) {
      if (!hasMorePostings()) {
        return false;
      }

      try {
        if (needToReadPositions) {
          skipPositions(prevTf);
          needToReadPositions = false;
        }

        if (cnt == 0) {
          p.setDocno(bitsIn.readBinary(MAX_DOCNO_BITS));
          p.setTf((short) bitsIn.readGamma());
        } else {
          p.setDocno(innerPrevDocno + bitsIn.readGolomb(innerGolombParam));
          p.setTf((short) bitsIn.readGamma());
        }
      } catch (IOException e) {
        throw new RuntimeException("Error in reading posting: cnt=" + cnt
            + ", innerNumPostings=" + innerNumPostings + ", " + e);
      }

      cnt++;
      innerPrevDocno = p.getDocno();
      prevTf = p.getTf();
      curPositions = null;
      needToReadPositions = true;

      return true;
    }

    @Override
    public int[] getPositions() {
      if (curPositions != null) {
        return curPositions;
      }

      int[] pos = null;
      try {
        if (prevTf == 1) {
          pos = new int[1];
          pos[0] = bitsIn.readGamma();
        } else {
          bitsIn.readGamma();
          pos = new int[prevTf];
          pos[0] = bitsIn.readGamma();
          for (int i = 1; i < prevTf; i++) {
            pos[i] = (pos[i - 1] + bitsIn.readGamma());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("A problem in reading bits!", e);
      }

      needToReadPositions = false;
      curPositions = pos;

      return pos;
    }

    @Override
    public boolean getPositions(TermPositions tp) {
      int[] pos = getPositions();

      if (pos == null) {
        return false;
      }

      tp.set(pos, (short) pos.length);

      return true;
    }

    @Override
    public boolean hasMorePostings() {
      return !(cnt >= innerNumPostings);
    }

    @Override
    public short peekNextTf() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int peekNextDocno() {
      throw new UnsupportedOperationException();
    }

    private void skipPositions(int tf) throws IOException {
      if (tf == 1) {
        bitsIn.readGamma();
      } else {
        bitsIn.skipBits(bitsIn.readGamma());
      }
    }

    @Override
    public PostingsList getPostingsList() {
      return postingsList;
    }

    @Override
    public int getDocno() {
      return innerPrevDocno;
    }

    @Override
    public short getTf() {
      return prevTf;
    }
  }

  public static PostingsListDocSortedPositional merge(PostingsListDocSortedPositional plist1,
      PostingsListDocSortedPositional plist2, int docs) {
    Preconditions.checkNotNull(plist1);
    Preconditions.checkNotNull(plist2);

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    PostingsListDocSortedPositional newPostings = new PostingsListDocSortedPositional();
    newPostings.setCollectionDocumentCount(docs);
    newPostings.setNumberOfPostings(numPostings1 + numPostings2);

    Posting posting1 = new Posting();
    PostingsReader reader1 = plist1.getPostingsReader();

    Posting posting2 = new Posting();
    PostingsReader reader2 = plist2.getPostingsReader();

    reader1.nextPosting(posting1);
    reader2.nextPosting(posting2);

    TermPositions tp1 = new TermPositions();
    TermPositions tp2 = new TermPositions();

    reader1.getPositions(tp1);
    reader2.getPositions(tp2);

    while (true) {
      if (posting1 == null) {
        newPostings.add(posting2.getDocno(), posting2.getTf(), tp2);

        // Read the rest from reader 2.
        while (reader2.nextPosting(posting2)) {
          reader2.getPositions(tp2);
          newPostings.add(posting2.getDocno(), posting2.getTf(), tp2);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), tp1);

        // Read the rest from reader 1.
        while (reader1.nextPosting(posting1)) {
          reader1.getPositions(tp1);
          newPostings.add(posting1.getDocno(), posting1.getTf(), tp1);
        }

        break;

      } else if (posting1.getDocno() < posting2.getDocno()) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), tp1);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        } else {
          reader1.getPositions(tp1);
        }
      } else {
        newPostings.add(posting2.getDocno(), posting2.getTf(), tp2);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        } else {
          reader2.getPositions(tp2);
        }
      }
    }

    return newPostings;
  }

  public static PostingsListDocSortedPositional merge(PostingsList plist1,
      PostingsList plist2, int docs) {
    Preconditions.checkNotNull(plist1);
    Preconditions.checkNotNull(plist2);

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    PostingsListDocSortedPositional newPostings = new PostingsListDocSortedPositional();
    newPostings.setCollectionDocumentCount(docs);
    newPostings.setNumberOfPostings(numPostings1 + numPostings2);

    Posting posting1 = new Posting();
    ivory.core.data.index.PostingsReader reader1 = plist1.getPostingsReader();

    Posting posting2 = new Posting();
    ivory.core.data.index.PostingsReader reader2 = plist2.getPostingsReader();

    reader1.nextPosting(posting1);
    reader2.nextPosting(posting2);

    TermPositions tp1 = new TermPositions();
    TermPositions tp2 = new TermPositions();

    reader1.getPositions(tp1);
    reader2.getPositions(tp2);

    while (true) {
      if (posting1 == null) {
        newPostings.add(posting2.getDocno(), posting2.getTf(), tp2);

        // Read the rest from reader 2.
        while (reader2.nextPosting(posting2)) {
          reader2.getPositions(tp2);
          newPostings.add(posting2.getDocno(), posting2.getTf(), tp2);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), tp1);

        // Read the rest from reader 1.
        while (reader1.nextPosting(posting1)) {
          reader1.getPositions(tp1);
          newPostings.add(posting1.getDocno(), posting1.getTf(), tp1);
        }

        break;
      } else if (posting1.getDocno() < posting2.getDocno()) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), tp1);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        } else {
          reader1.getPositions(tp1);
        }
      } else {
        newPostings.add(posting2.getDocno(), posting2.getTf(), tp2);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        } else {
          reader2.getPositions(tp2);
        }
      }
    }

    return newPostings;
  }

  public static void mergeList(PostingsList newPostings, List<PostingsList> list, int nCollDocs) {
    Preconditions.checkNotNull(list);
    int nLists = list.size();

    // A reader for each postings list.
    ivory.core.data.index.PostingsReader[] reader = new PostingsReader[nLists];

    Posting[] posting = new Posting[nLists];        // The cur posting of each list.
    TermPositions[] tp = new TermPositions[nLists]; // The cur positions of each list.

    // min-heap for merging
    PriorityQueue<DocList> heap = new PriorityQueue<DocList>(nLists, comparator);

    int totalPostings = 0;
    int i = 0;
    for (PostingsList pl : list) {
      pl.setCollectionDocumentCount(nCollDocs);

      totalPostings += pl.getNumberOfPostings();

      reader[i] = pl.getPostingsReader();

      posting[i] = new Posting();
      reader[i].nextPosting(posting[i]);

      tp[i] = new TermPositions();
      reader[i].getPositions(tp[i]);
      heap.add(new DocList(posting[i].getDocno(), i));

      i++;
    }

    newPostings.setCollectionDocumentCount(nCollDocs);
    newPostings.setNumberOfPostings(totalPostings);

    DocList dl;
    while (heap.size() > 0) {
      dl = heap.remove();
      i = dl.listIndex;
      newPostings.add(dl.id, posting[i].getTf(), tp[i]);

      if (reader[i].nextPosting(posting[i])) {
        reader[i].getPositions(tp[i]);
        dl.set(posting[i].getDocno(), i);
        heap.add(dl);
      }
    }
  }

  private static class DocList {
    public int id;
    public int listIndex;

    public DocList(int id, int listIndex) {
      this.id = id;
      this.listIndex = listIndex;
    }

    public void set(int id, int listIndex) {
      this.id = id;
      this.listIndex = listIndex;
    }

    @Override
    public String toString() {
      return "{" + id + " - " + listIndex + "}";
    }
  }

  public static class DocListComparator implements Comparator<DocList> {
    public int compare(DocList t1, DocList t2) {
      if (t1.id < t2.id) {
        return -1;
      } else if (t1.id > t2.id) {
        return 1;
      }
      return 0;
    }
  }

  private static final DocListComparator comparator = new DocListComparator();
}
