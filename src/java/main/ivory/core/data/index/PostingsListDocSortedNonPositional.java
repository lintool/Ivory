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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Object representing a document-sorted postings list wit no positional information.
 * 
 * @author Jimmy Lin
 */
public class PostingsListDocSortedNonPositional implements PostingsList {
  private static final Logger LOG = Logger.getLogger(PostingsListDocSortedNonPositional.class);
  private static final int MAX_DOCNO_BITS = 32;

  static {
    LOG.setLevel(Level.WARN);
  }

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

  public void add(int docno, short score, TermPositions pos) {
    add(docno, score);
  }

  public void add(int docno, short score) {
    LOG.info("adding posting: " + docno + ", " + score);

    try {
      if (postingsAdded == 0) {
        // write out the first docno
        bitOut.writeBinary(MAX_DOCNO_BITS, docno);
        bitOut.writeGamma(score);

        prevDocno = docno;
      } else {
        // use d-gaps for subsequent docnos
        int dgap = docno - prevDocno;

        if (dgap <= 0) {
          throw new RuntimeException("Error: encountered invalid d-gap. docno=" + docno);
        }

        bitOut.writeGolomb(dgap, golombParam);
        bitOut.writeGamma(score);

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
          + ", collection size=" + collectionSize + ", Golomb param=" + golombParam);
    }

    postingsAdded++;
    sumOfPostingsScore += score;
  }

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

  public byte[] getRawBytes() {
    return rawBytes;
  }

  public void setCollectionDocumentCount(int docs) {
    collectionSize = docs;
    recomputeGolombParameter();
  }

  public int getCollectionDocumentCount() {
    return collectionSize;
  }

  public void setNumberOfPostings(int n) {
    numPostings = n;
    recomputeGolombParameter();
  }

  public int getNumberOfPostings() {
    return numPostings;
  }

  private void recomputeGolombParameter() {
    golombParam = (int) Math.ceil(0.69 * ((float) collectionSize) / (float) numPostings);
  }

  public int getDf() {
    return df;
  }
  
  public void setDf(int df) {
    this.df = df;
  }

  public long getCf() {
    return cf;
  }
  
  public void setCf(long cf) {
    this.cf = cf;
  }

  public void readFields(DataInput in) throws IOException {
    postingsAdded = WritableUtils.readVInt(in);
    numPostings = postingsAdded;

    df = WritableUtils.readVInt(in);
    cf = WritableUtils.readVLong(in);
    sumOfPostingsScore = cf;

    rawBytes = new byte[WritableUtils.readVInt(in)];
    in.readFully(rawBytes);
  }

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
    }
  }

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
   * {@code PostingsReader} for {@code PostingsListDocSortedNonPositional}.
   * 
   * @author Jimmy Lin
   *
   */
  public static class PostingsReader implements ivory.core.data.index.PostingsReader {
    private ByteArrayInputStream bytesIn;
    private BitInputStream bitsIn;
    private int cnt = 0;
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

    public int getNumberOfPostings() {
      return innerNumPostings;
    }

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

      return true;
    }

    public int[] getPositions() {
      throw new UnsupportedOperationException();
    }
    
    public byte[] getBytePositions(){
      throw new UnsupportedOperationException();
    }
    
    public boolean getPositions(TermPositions tp){
      throw new UnsupportedOperationException();
    }

    public boolean hasMorePostings() {
      return !(cnt >= innerNumPostings);
    }

    public short peekNextTf() {
      throw new UnsupportedOperationException();
    }

    public int peekNextDocno() {
      throw new UnsupportedOperationException();
    }

    public PostingsList getPostingsList() {
      return postingsList;
    }
    
    public short getTf(){
      throw new UnsupportedOperationException();
    }
    
    public int getDocno(){
      throw new UnsupportedOperationException();
    }    
  }
  
  public static PostingsListDocSortedNonPositional merge(PostingsListDocSortedNonPositional plist1,
      PostingsListDocSortedNonPositional plist2, int docs) {

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    //System.out.println("number of postings (1): " + numPostings1);
    //System.out.println("number of postings (2): " + numPostings2);

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
        //System.out.println("2: " + posting2);

        // read the rest from reader 2
        while (reader2.nextPosting(posting2)) {
          newPostings.add(posting2.getDocno(), posting2.getTf(), null);
          //System.out.println("2: " + posting2);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);
        //System.out.println("1: " + posting1);

        // read the rest from reader 1
        while (reader1.nextPosting(posting1)) {
          newPostings.add(posting1.getDocno(), posting1.getTf(), null);
          //System.out.println("1: " + posting1);
        }

        break;

      } else if (posting1.getDocno() < posting2.getDocno()) {
        //System.out.println("1: " + posting1);
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        } else {
        }
      } else {
        //System.out.println("2: " + posting2);
        newPostings.add(posting2.getDocno(), posting2.getTf(), null);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        } else {
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

    //System.out.println("number of postings (1): " + numPostings1);
    //System.out.println("number of postings (2): " + numPostings2);

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
        //System.out.println("2: " + posting2);

        // read the rest from reader 2
        while (reader2.nextPosting(posting2)) {
          newPostings.add(posting2.getDocno(), posting2.getTf(), null);
          //System.out.println("2: " + posting2);
        }

        break;
      } else if (posting2 == null) {
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);
        //System.out.println("1: " + posting1);

        // read the rest from reader 1
        while (reader1.nextPosting(posting1)) {
          newPostings.add(posting1.getDocno(), posting1.getTf(), null);
          //System.out.println("1: " + posting1);
        }

        break;

      } else if (posting1.getDocno() < posting2.getDocno()) {
        //System.out.println("1: " + posting1);
        newPostings.add(posting1.getDocno(), posting1.getTf(), null);

        if (reader1.nextPosting(posting1) == false) {
          posting1 = null;
        } else {
        }
      } else {
        //System.out.println("2: " + posting2);
        newPostings.add(posting2.getDocno(), posting2.getTf(), null);

        if (reader2.nextPosting(posting2) == false) {
          posting2 = null;
        } else {
        }
      }
    }

    return newPostings;
  }

  public static class DocList{
    public int id;
    public int listIndex;
    /**
     * @param id
     * @param listIndex
     */
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
      return "{"+id+" - "+listIndex+"}";
    }

  }
  
  public static class DocListComparator implements Comparator<DocList>{

    public int compare(DocList t1, DocList t2) {
      if(t1.id < t2.id) return -1;
      else if(t1.id > t2.id) return 1;
      return 0;
    }
  }
  
  private static DocListComparator comparator = new DocListComparator();
  
  public static void mergeList(PostingsList newPostings, ArrayList<PostingsList> list, int nCollDocs){
    //sLogger.setLevel(Level.INFO);
    
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
    LOG.info(">> merging a list of "+list.size()+" partial lists");
    newPostings.setCollectionDocumentCount(nCollDocs);
    newPostings.setNumberOfPostings(totalPostings);
    LOG.info("\ttotalPostings: "+totalPostings);
    //sLogger.info("Total # of lists = " + nLists);
    //sLogger.info("Total # of postings = " + totalPostings);

    DocList dl; 
    while (heap.size() > 0) {
      //sLogger.info("Heap size: "+heap.size()+" peek = "+heap.peek());
      dl = heap.remove();
      i = dl.listIndex;
      newPostings.add(dl.id, posting[i].getTf(), null);
      /*k++;
      sLogger.info("==Added posting #"+k);
      sLogger.info("\t"+posting[i]);
      sLogger.info("\t"+tp[i]);*/
      if(reader[i].nextPosting(posting[i])){
        dl.set(posting[i].getDocno(), i);
        heap.add(dl);
      }
    }
    LOG.info("\tdone.");
    //sLogger.setLevel(Level.WARN);
  }
  
  
  public static void main(String[] args) throws Exception{
    
    LOG.setLevel(Level.INFO);
    
    ByteArrayOutputStream mBytesOut;
    DataOutputStream mDataOut;
    
    ByteArrayInputStream mBytesIn;
    DataInputStream mDataIn;

    
    ivory.core.data.index.PostingsReader r;
    Posting p;
    
    PostingsList pl1, pl2, pl3, pl4, pl5, pl6, pl7, pl8, plTotal;
    
    TermPositions tp = new TermPositions();
    
    int[] pos;
    short tf;
    
    pl1 = new PostingsListDocSortedNonPositional();
    pl1.setCollectionDocumentCount(100);
    pl1.setNumberOfPostings(3);
    pos = new int[1];
    pos[0] = 5;
    tf = (short)pos.length; tp.set(pos, tf);
    pl1.add(1, tf, null);
    
    pos = new int[2];
    pos[0] = 2;pos[1] = 3;
    tf = (short)pos.length; tp.set(pos, tf);
    pl1.add(3, tf, null);
    
    pos = new int[1];
    pos[0] = 4;
    tf = (short)pos.length; tp.set(pos, tf);
    pl1.add(5, tf, null);
    
    p = new Posting();
    
    
    mBytesOut = new ByteArrayOutputStream();
    mDataOut = new DataOutputStream(mBytesOut);
    
    pl1.write(mDataOut);
    
    mDataOut.flush();
    
    mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
    mDataIn = new DataInputStream(mBytesIn);
    
    
    pl5 = new PostingsListDocSortedNonPositional();
    pl5.setCollectionDocumentCount(100);
    pl5.readFields(mDataIn);
    r = pl5.getPostingsReader();
    while(r.hasMorePostings()){
      r.nextPosting(p);
      System.out.println(p);
    }
    
    
    pl2 = new PostingsListDocSortedNonPositional();
    pl2.setCollectionDocumentCount(100);
    pl2.setNumberOfPostings(3);
    pos = new int[1];
    pos[0] = 25;
    tf = (short)pos.length; tp.set(pos, tf);
    pl2.add(2, tf, null);
    
    pos = new int[2];
    pos[0] = 22;pos[1] = 23;
    tf = (short)pos.length; tp.set(pos, tf);
    pl2.add(13, tf, null);
    
    pos = new int[1];
    pos[0] = 24;
    tf = (short)pos.length; tp.set(pos, tf);
    pl2.add(20, tf, null);
    
    p = new Posting();
    
    
    mBytesOut = new ByteArrayOutputStream();
    mDataOut = new DataOutputStream(mBytesOut);
    
    pl2.write(mDataOut);
    
    mDataOut.flush();
    
    mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
    mDataIn = new DataInputStream(mBytesIn);
    
    
    pl6 = new PostingsListDocSortedNonPositional();
    pl6.setCollectionDocumentCount(100);
    pl6.readFields(mDataIn);
    r = pl6.getPostingsReader();
    while(r.hasMorePostings()){
      r.nextPosting(p);
      System.out.println(p);
    }

    
    
    pl3 = new PostingsListDocSortedNonPositional();
    pl3.setCollectionDocumentCount(100);
    pl3.setNumberOfPostings(3);
    pos = new int[1];
    pos[0] = 35;
    tf = (short)pos.length; tp.set(pos, tf);
    pl3.add(12, tf, null);
    
    pos = new int[2];
    pos[0] = 32;pos[1] = 33;
    tf = (short)pos.length; tp.set(pos, tf);
    pl3.add(14, tf, null);
    
    pos = new int[1];
    pos[0] = 34;
    tf = (short)pos.length; tp.set(pos, tf);
    pl3.add(26, tf, null);
    
    p = new Posting();
    
    
    mBytesOut = new ByteArrayOutputStream();
    mDataOut = new DataOutputStream(mBytesOut);
    
    pl3.write(mDataOut);
    
    mDataOut.flush();
    
    mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
    mDataIn = new DataInputStream(mBytesIn);
    
    
    pl7 = new PostingsListDocSortedNonPositional();
    pl7.setCollectionDocumentCount(100);
    pl7.readFields(mDataIn);
    r = pl7.getPostingsReader();
    while(r.hasMorePostings()){
      r.nextPosting(p);
      System.out.println(p);
    }

    
    pl4 = new PostingsListDocSortedNonPositional();
    pl4.setCollectionDocumentCount(100);
    pl4.setNumberOfPostings(3);
    pos = new int[1];
    pos[0] = 45;
    tf = (short)pos.length; tp.set(pos, tf);
    pl4.add(7, tf, null);
    
    pos = new int[2];
    pos[0] = 42;pos[1] = 33;
    tf = (short)pos.length; tp.set(pos, tf);
    pl4.add(77, tf, null);
    
    pos = new int[1];
    pos[0] = 44;
    tf = (short)pos.length; tp.set(pos, tf);
    pl4.add(777, tf, null);
    
    p = new Posting();
    
    
    mBytesOut = new ByteArrayOutputStream();
    mDataOut = new DataOutputStream(mBytesOut);
    
    pl4.write(mDataOut);
    
    mDataOut.flush();
    
    mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
    mDataIn = new DataInputStream(mBytesIn);
    
    
    pl8 = new PostingsListDocSortedNonPositional();
    pl8.setCollectionDocumentCount(100);
    pl8.readFields(mDataIn);
    r = pl8.getPostingsReader();
    while(r.hasMorePostings()){
      r.nextPosting(p);
      System.out.println(p);
    }
    
    
    ArrayList<PostingsList> list = new ArrayList<PostingsList>();
    list.add(pl5);
    list.add(pl6);
    list.add(pl7);
    list.add(pl8);
    plTotal = new PostingsListDocSortedNonPositional();
    plTotal.setCollectionDocumentCount(100);
    PostingsListDocSortedNonPositional.mergeList(plTotal, list, 100);
    
    
    mBytesOut = new ByteArrayOutputStream();
    mDataOut = new DataOutputStream(mBytesOut);
    
    plTotal.write(mDataOut);
    
    mDataOut.flush();
    
    mBytesIn = new ByteArrayInputStream(mBytesOut.toByteArray());
    mDataIn = new DataInputStream(mBytesIn);
    
    pl1 = new PostingsListDocSortedNonPositional();
    pl1.setCollectionDocumentCount(100);
    pl1.readFields(mDataIn);
    r = pl1.getPostingsReader();
    while(r.hasMorePostings()){
      r.nextPosting(p);
      System.out.println(p);
    }
  }
}
