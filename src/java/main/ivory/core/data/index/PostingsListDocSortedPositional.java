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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


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
      e.printStackTrace();
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
      e.printStackTrace();
    }
  }

  @Override
  public void add(int docno, short score, TermPositions pos) {
    LOG.info("adding posting: " + docno + ", " + score + ", " + pos.toString());
    if (pos.getPositions().length == 0) {
      throw new RuntimeException("Error: encountered invalid number of positions = 0");
    }
    if (score != pos.getTf()) {
      throw new RuntimeException("Error: tf and number of positions don't match. docno="
          + docno + ", tf=" + score + ", positions=" + pos.toString());
    }
    try {
      if (postingsAdded == 0) {
        // write out the first docno
        bitsOut.writeBinary(MAX_DOCNO_BITS, docno);
        bitsOut.writeGamma(score);
        writePositions(bitsOut, pos, docno, score);

        prevDocno = docno;
      } else {
        // use d-gaps for subsequent docnos
        int dgap = docno - prevDocno;

        if (dgap <= 0) {
          throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
        }

        bitsOut.writeGolomb(dgap, golombParam);
        bitsOut.writeGamma(score);
        writePositions(bitsOut, pos, docno, score);

        prevDocno = docno;
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Error adding postings.");
    } catch (ArithmeticException e) {
      e.printStackTrace();
      throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
          + "\": check to see if collection size or df is set properly. docno=" + docno
          + ", tf=" + score + ", previous docno=" + prevDocno + ", df=" + numPostings
          + ", collection size=" + collectionDocumentCount + ", Golomb param=" + golombParam);
    }

    postingsAdded++;
    sumOfPostingsScore += score;
  }

  // passing in docno and tf basically for error checking purposes
  private static void writePositions(BitOutputStream t, TermPositions p, int docno, short tf)
      throws IOException {
    int[] pos = p.getPositions();

    if (tf != p.getTf()) {
      throw new RuntimeException("Error: tf and number of positions don't match. docno="
          + docno + ", tf=" + tf + ", positions=" + p.toString());
    }

    if (p.getTf() == 1) {
      // if tf=1, just write out the single term position
      t.writeGamma(pos[0]);
    } else {
      // if tf > 1, write out skip information if we want to bypass the
      // positional information during decoding
      t.writeGamma(p.getEncodedSize());

      // keep track of where we are in the stream
      int skip_pos1 = (int) t.getByteOffset() * 8 + t.getBitOffset();

      if (pos[0] <= 0) {
        throw new RuntimeException("Error: invalid term positions. positions="
            + p.toString() + ", docno=" + docno + ", tf=" + tf);
      }
      // write out first position
      t.writeGamma(pos[0]);
      // write out rest of positions using p-gaps (first order positional
      // differences)
      for (int c = 1; c < p.getTf(); c++) {
        int pgap = pos[c] - pos[c - 1];
        if (pos[c] <= 0 || pgap == 0) {
          throw new RuntimeException("Error: invalid term positions. positions="
              + p.toString() + ", docno=" + docno + ", tf=" + tf);
        }
        t.writeGamma(pgap);
      }

      // find out where we are in the stream no
      int skip_pos2 = (int) t.getByteOffset() * 8 + t.getBitOffset();

      // verify that the skip information is indeed valid
      if (skip_pos1 + p.getEncodedSize() != skip_pos2) {
        throw new RuntimeException("Ivalid skip information: skip_pos1=" + skip_pos1
            + ", skip_pos2=" + skip_pos2 + ", size=" + p.getEncodedSize());
      }

    }
  }

  @Override
  public int size() {
    return postingsAdded;
  }

  @Override
  public PostingsReader getPostingsReader() {
    try {
      if (collectionDocumentCount <= 0)
        throw new RuntimeException("Invalid Collection Document Count: " + collectionDocumentCount);
      if (rawBytes == null)
        throw new RuntimeException("Invalid rawBytes .. Postings must be serialized!!");
      if (postingsAdded <= 0)
        throw new RuntimeException("Invalid number of postings: " + postingsAdded);
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
    if (docs <= 0) {
      throw new RuntimeException("Invalid Collection Document Count: " + collectionDocumentCount);
    }
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
      // this would happen if we're reading in an already-encoded
      // postings; if that's the case, simply write out the byte array
      WritableUtils.writeVInt(out, postingsAdded);
      WritableUtils.writeVInt(out, df == 0 ? postingsAdded : df); // df
      WritableUtils.writeVLong(out, cf == 0 ? sumOfPostingsScore : cf); // cf
      WritableUtils.writeVInt(out, rawBytes.length);
      out.write(rawBytes);
    } else {
      try {
        bitsOut.padAndFlush();
        bitsOut.close();

        if (numPostings != postingsAdded) {
          throw new RuntimeException(
              "Error, number of postings added doesn't match number of expected postings.  Expected "
                  + numPostings + ", got " + postingsAdded);
        }

        WritableUtils.writeVInt(out, postingsAdded);
        WritableUtils.writeVInt(out, df == 0 ? postingsAdded : df); // df
        WritableUtils.writeVLong(out, cf == 0 ? sumOfPostingsScore : cf); // cf
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
    if (postingsAdded <= 0)
      throw new RuntimeException("Invalid number of added postings: " + postingsAdded
          + " !! nPostings=" + numPostings + ", CollSize=" + collectionDocumentCount);
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
      if (i != 0)
        sb.append(", ");
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

    public PostingsReader(byte[] bytes, int n, int collectionSize,
        PostingsListDocSortedPositional list) throws IOException {
      bytesIn = new ByteArrayInputStream(bytes);
      bitsIn = new BitInputStream(bytesIn);
      if (n <= 0) {
        throw new RuntimeException("Invalid number of postings: " + n);
      }
      innerNumPostings = n;
      if (collectionSize <= 0) {
        throw new RuntimeException("Invalid Collection size: " + collectionSize);
      }
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
          p.setScore((short) bitsIn.readGamma());
        } else {
          p.setDocno(innerPrevDocno + bitsIn.readGolomb(innerGolombParam));
          p.setScore((short) bitsIn.readGamma());
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error in reading posting: mCnt=" + cnt
            + ", mInnerNumPostings=" + innerNumPostings + ", " + e);
      }

      cnt++;
      innerPrevDocno = p.getDocno();
      prevTf = p.getScore();
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
        e.printStackTrace();
        throw new RuntimeException("A problem in reading bits! " + e);
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
        newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);

        // read the rest from reader 2
        while (reader2.nextPosting(posting2)) {
          reader2.getPositions(tp2);
          newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);

        // read the rest from reader 1
        while (reader1.nextPosting(posting1)) {
          reader1.getPositions(tp1);
          newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);
        }

        break;

      } else if (posting1.getDocno() < posting2.getDocno()) {
        newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        } else {
          reader1.getPositions(tp1);
        }
      } else {
        newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);

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
        newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);

        // read the rest from reader 2
        while (reader2.nextPosting(posting2)) {
          reader2.getPositions(tp2);
          newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);

        // read the rest from reader 1
        while (reader1.nextPosting(posting1)) {
          reader1.getPositions(tp1);
          newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);
        }

        break;
      } else if (posting1.getDocno() < posting2.getDocno()) {
        newPostings.add(posting1.getDocno(), posting1.getScore(), tp1);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        } else {
          reader1.getPositions(tp1);
        }
      } else {
        newPostings.add(posting2.getDocno(), posting2.getScore(), tp2);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        } else {
          reader2.getPositions(tp2);
        }
      }
    }

    return newPostings;
  }

  public static void mergeList(PostingsList newPostings, ArrayList<PostingsList> list, int nCollDocs) {
    int nLists = list.size();

    // a reader for each pl
    ivory.core.data.index.PostingsReader[] reader = new PostingsReader[nLists];

    // the cur posting of each list
    Posting[] posting = new Posting[nLists];

    // the cur positions of each list
    TermPositions[] tp = new TermPositions[nLists];

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
    LOG.info(">> merging a list of " + list.size() + " partial lists");
    newPostings.setCollectionDocumentCount(nCollDocs);
    newPostings.setNumberOfPostings(totalPostings);
    LOG.info("\ttotalPostings: " + totalPostings);

    DocList dl;
    while (heap.size() > 0) {
      dl = heap.remove();
      i = dl.listIndex;
      newPostings.add(dl.id, posting[i].getScore(), tp[i]);

      if (reader[i].nextPosting(posting[i])) {
        reader[i].getPositions(tp[i]);
        dl.set(posting[i].getDocno(), i);
        heap.add(dl);
      }
    }
    LOG.info("\tdone.");
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
