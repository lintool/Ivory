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

import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

/**
 * Object representing a document-sorted postings list wit no positional information.
 *
 * @author Jimmy Lin
 */
public class PostingsListDocSortedNonPositional implements PostingsList {
  private static final int MAX_DOCNO_BITS = 32;

  private int collectionSize = -1;
  private int numPostings = -1;
  private int golombParam;
  private int prevDocno;
  private byte[] rawBytes;
  private int postingsAdded;
  private long sumOfPostingsScore;

  private int df;
  private long cf;

  private ByteArrayOutputStream bytesOut;
  private BitOutputStream bitOut;

  public PostingsListDocSortedNonPositional() {
    this.sumOfPostingsScore = 0;
    this.postingsAdded = 0;
    this.df = 0;
    this.cf = 0;
    this.prevDocno = -1;

    try {
      bytesOut = new ByteArrayOutputStream();
      bitOut = new BitOutputStream(bytesOut);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void clear() {
    this.sumOfPostingsScore = 0;
    this.postingsAdded = 0;
    this.df = 0;
    this.cf = 0;
    this.prevDocno = -1;

    try {
      bytesOut = new ByteArrayOutputStream();
      bitOut = new BitOutputStream(bytesOut);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void add(int docno, short tf, TermPositions pos) {
    add(docno, tf);
  }

  public void add(int docno, short tf) {
    try {
      if (postingsAdded == 0) {
        // write out the first docno
        bitOut.writeBinary(MAX_DOCNO_BITS, docno);
        bitOut.writeGamma(tf);

        prevDocno = docno;
      } else {
        // use d-gaps for subsequent docnos
        int dgap = docno - prevDocno;

        if (dgap <= 0) {
          throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
        }

        bitOut.writeGolomb(dgap, golombParam);
        bitOut.writeGamma(tf);

        prevDocno = docno;
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Error adding postings.");
    } catch (ArithmeticException e) {
      e.printStackTrace();
      throw new RuntimeException("ArithmeticException caught \"" + e.getMessage()
          + "\": check to see if collection size or df is set properly. docno=" + docno
          + ", tf=" + tf + ", previous docno=" + prevDocno + ", df=" + numPostings
          + ", collection size=" + collectionSize + ", Golomb param=" + golombParam);
    }

    postingsAdded++;
    sumOfPostingsScore += tf;
  }

  @Override
  public int size() {
    return postingsAdded;
  }

  @Override
  public PostingsReader getPostingsReader() {
    try {
      return new PostingsReader(rawBytes, postingsAdded, collectionSize, this);
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
    collectionSize = docs;
    recomputeGolombParameter();
  }

  @Override
  public int getCollectionDocumentCount() {
    return collectionSize;
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
    golombParam = (int) Math.ceil(0.69 * ((float) collectionSize) / (float) numPostings);
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
        bitOut.padAndFlush();
        bitOut.close();

        if (numPostings != postingsAdded) {
          throw new RuntimeException(
              "Error, number of postings added doesn't match number of expected postings. Expected "
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
    }
  }

  @Override
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  public static PostingsListDocSortedNonPositional create(DataInput in) throws IOException {
    PostingsListDocSortedNonPositional p = new PostingsListDocSortedNonPositional();
    p.readFields(in);

    return p;
  }

  public static PostingsListDocSortedNonPositional create(byte[] bytes) throws IOException {
    return PostingsListDocSortedNonPositional.create(new DataInputStream(
        new ByteArrayInputStream(bytes)));
  }

  @Override
  public String toString() {
    return String.format("[%s df=%d cf=%d numPostings=%d]",
        PostingsListDocSortedNonPositional.class.getSimpleName(), getDf(), getCf(),
        getNumberOfPostings());
  }

  /**
   * {@code PostingsReader} for {@code PostingsListDocSortedNonPositional}.
   * 
   * @author Jimmy Lin
   *
   */
  public static class PostingsReader implements ivory.core.data.index.PostingsReader {
    private ByteArrayInputStream bytesIn;
    private BitInputStream bitsIn;
    private int cnt = 0;
    private short prevTf;
    private int innerPrevDocno;
    private int innerNumPostings;
    private int innerGolombParam;
    private int innerCollectionSize;
    private PostingsList postingsList;

    public PostingsReader(byte[] bytes, int numPostings, int collectionSize,
        PostingsListDocSortedNonPositional list) throws IOException {
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
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error resetting postings.");
      }
    }

    @Override
    public boolean nextPosting(Posting p) {
      try {
        if (cnt == 0) {
          p.setDocno(bitsIn.readBinary(MAX_DOCNO_BITS));
          p.setTf((short) bitsIn.readGamma());
        } else {
          if (cnt >= innerNumPostings) {
            return false;
          }

          p.setDocno(innerPrevDocno + bitsIn.readGolomb(innerGolombParam));
          p.setTf((short) bitsIn.readGamma());
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e.toString());
      }

      cnt++;
      innerPrevDocno = p.getDocno();
      prevTf = p.getTf();

      return true;
    }

    @Override
    public int[] getPositions() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getPositions(TermPositions tp){
      throw new UnsupportedOperationException();
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

    @Override
    public PostingsList getPostingsList() {
      return postingsList;
    }

    @Override
    public short getTf(){
      return prevTf;
    }

    @Override
    public int getDocno(){
      return innerPrevDocno;
    }    
  }
  
  public static PostingsListDocSortedNonPositional merge(PostingsListDocSortedNonPositional plist1,
      PostingsListDocSortedNonPositional plist2, int docs) {

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    PostingsListDocSortedNonPositional newPostings = new PostingsListDocSortedNonPositional();
    newPostings.setCollectionDocumentCount(docs);
    newPostings.setNumberOfPostings(numPostings1 + numPostings2);

    Posting posting1 = new Posting();
    PostingsReader reader1 = plist1.getPostingsReader();

    Posting posting2 = new Posting();
    PostingsReader reader2 = plist2.getPostingsReader();

    reader1.nextPosting(posting1);
    reader2.nextPosting(posting2);

    while (true) {
      if (posting1 == null) {
        newPostings.add(posting2.getDocno(), posting2.getTf(), null);

        // read the rest from reader 2
        while (reader2.nextPosting(posting2)) {
          newPostings.add(posting2.getDocno(), posting2.getTf(), null);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);

        // read the rest from reader 1
        while (reader1.nextPosting(posting1)) {
          newPostings.add(posting1.getDocno(), posting1.getTf(), null);
        }

        break;
      } else if (posting1.getDocno() < posting2.getDocno()) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        }
      } else {
        newPostings.add(posting2.getDocno(), posting2.getTf(), null);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        }
      }
    }

    return newPostings;
  }

  public static PostingsListDocSortedNonPositional merge(PostingsList plist1,
      PostingsList plist2, int docs) {

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    PostingsListDocSortedNonPositional newPostings = new PostingsListDocSortedNonPositional();
    newPostings.setCollectionDocumentCount(docs);
    newPostings.setNumberOfPostings(numPostings1 + numPostings2);

    Posting posting1 = new Posting();
    ivory.core.data.index.PostingsReader reader1 = plist1.getPostingsReader();

    Posting posting2 = new Posting();
    ivory.core.data.index.PostingsReader reader2 = plist2.getPostingsReader();

    reader1.nextPosting(posting1);
    reader2.nextPosting(posting2);

    while (true) {
      if (posting1 == null) {
        newPostings.add(posting2.getDocno(), posting2.getTf(), null);

        // read the rest from reader 2
        while (reader2.nextPosting(posting2)) {
          newPostings.add(posting2.getDocno(), posting2.getTf(), null);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);

        // read the rest from reader 1
        while (reader1.nextPosting(posting1)) {
          newPostings.add(posting1.getDocno(), posting1.getTf(), null);
        }

        break;
      } else if (posting1.getDocno() < posting2.getDocno()) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        }
      } else {
        newPostings.add(posting2.getDocno(), posting2.getTf(), null);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        }
      }
    }

    return newPostings;
  }

  public static class DocList {
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
  
  private static DocListComparator comparator = new DocListComparator();
  
  public static void mergeList(PostingsList newPostings, ArrayList<PostingsList> list, int nCollDocs){
    int nLists = list.size();
    
    // a reader for each pl
    ivory.core.data.index.PostingsReader[] reader = new PostingsReader[nLists];
    
    // the cur posting of each list
    Posting[] posting = new Posting[nLists];
    
    // min-heap for merging
    java.util.PriorityQueue<DocList> heap = new java.util.PriorityQueue<DocList>(nLists, comparator);
    
    int totalPostings = 0;
    int i = 0;
    for(PostingsList pl : list){
      pl.setCollectionDocumentCount(nCollDocs);
      
      totalPostings += pl.getNumberOfPostings();
      
      reader[i] = pl.getPostingsReader();
      
      posting[i] = new Posting();
      reader[i].nextPosting(posting[i]);
      
      heap.add(new DocList(posting[i].getDocno(), i));
      
      i++;
    }

    newPostings.setCollectionDocumentCount(nCollDocs);
    newPostings.setNumberOfPostings(totalPostings);

    DocList dl; 
    while (heap.size() > 0) {
      dl = heap.remove();
      i = dl.listIndex;
      newPostings.add(dl.id, posting[i].getTf(), null);
      if(reader[i].nextPosting(posting[i])){
        dl.set(posting[i].getDocno(), i);
        heap.add(dl);
      }
    }
  }
}
