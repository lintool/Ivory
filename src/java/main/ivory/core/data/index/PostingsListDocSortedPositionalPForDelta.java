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

import com.kamikaze.pfordelta.PForDelta;

import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Object representing a document-sorted postings list that holds positional information for terms.
 * Compression is done using PForDelta.
 *
 * @author Nima Asadi
 */
public class PostingsListDocSortedPositionalPForDelta implements PostingsList {
  private int[][] docidCompressed; //Docid blocks
  private int[][] offsetCompressed; //Offset blocks
  private int[][] tfCompressed; //Term frequency blocks
  private int lastBlockSize;
  private int[][] positionsCompressed; //Term positions
  private int positionsLastBlockSize;

  private transient PForDeltaUtility util;

  private int collectionDocumentCount = -1;
  private int numPostings = -1;
  private long sumOfPostingsScore;
  private int postingsAdded;
  private int df;
  private long cf;

  public PostingsListDocSortedPositionalPForDelta() {
    this.sumOfPostingsScore = 0;
    this.postingsAdded = 0;
    this.df = 0;
    this.cf = 0;
  }

  @Override
  public void clear() {
    docidCompressed = null;
    offsetCompressed = null;
    tfCompressed = null;
    lastBlockSize = 0;
    positionsCompressed = null;
    positionsLastBlockSize = 0;
    util = null;

    sumOfPostingsScore = 0;
    df = 0;
    cf = 0;
    numPostings = -1;
    postingsAdded = 0;
  }

  @Override
  public void add(int docno, short tf, TermPositions pos) {
    Preconditions.checkArgument(pos.getPositions().length != 0);
    Preconditions.checkArgument(tf == pos.getTf());

    if(util.add(docno, tf, pos)) {
      docidCompressed = util.getCompressedDocids();
      offsetCompressed = util.getCompressedOffsets();
      tfCompressed = util.getCompressedTfs();
      positionsCompressed = util.getCompressedPositions();
      lastBlockSize = util.getLastBlockSize();
      positionsLastBlockSize = util.getPositionsLastBlockSize();
    }
    sumOfPostingsScore += tf;
    postingsAdded++;
  }

  @Override
  public int size() {
    return postingsAdded;
  }

  @Override
  public PostingsReader getPostingsReader() {
    Preconditions.checkNotNull(docidCompressed);
    Preconditions.checkNotNull(tfCompressed);
    Preconditions.checkNotNull(offsetCompressed);
    Preconditions.checkNotNull(positionsCompressed);
    Preconditions.checkArgument(collectionDocumentCount > 0);
    Preconditions.checkArgument(postingsAdded > 0);

    return new PostingsReader(postingsAdded, this);
  }

  @Override
  public byte[] getRawBytes() {
    return null;
  }

  @Override
  public void setCollectionDocumentCount(int docs) {
    Preconditions.checkArgument(docs > 0);

    collectionDocumentCount = docs;
  }

  @Override
  public int getCollectionDocumentCount() {
    return collectionDocumentCount;
  }

  @Override
  public void setNumberOfPostings(int n) {
    numPostings = n;
    util = new PForDeltaUtility(n);
  }

  @Override
  public int getNumberOfPostings() {
    return numPostings;
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

    lastBlockSize = in.readInt();
    docidCompressed = new int[in.readInt()][];
    tfCompressed = new int[docidCompressed.length][];
    offsetCompressed = new int[docidCompressed.length][];
    for(int i = 0; i < docidCompressed.length; i++) {
      docidCompressed[i] = new int[in.readInt()];
      for(int j = 0; j < docidCompressed[i].length; j++) {
        docidCompressed[i][j] = in.readInt();
      }
    }

    for(int i = 0; i < tfCompressed.length; i++) {
      tfCompressed[i] = new int[in.readInt()];
      for(int j = 0; j < tfCompressed[i].length; j++) {
        tfCompressed[i][j] = in.readInt();
      }
    }

    for(int i = 0; i < offsetCompressed.length; i++) {
      offsetCompressed[i] = new int[in.readInt()];
      for(int j = 0; j < offsetCompressed[i].length; j++) {
        offsetCompressed[i][j] = in.readInt();
      }
    }

    positionsLastBlockSize = in.readInt();
    positionsCompressed = new int[in.readInt()][];
    for(int i = 0; i < positionsCompressed.length; i++) {
      positionsCompressed[i] = new int[in.readInt()];
      for(int j = 0; j < positionsCompressed[i].length; j++) {
        positionsCompressed[i][j] = in.readInt();
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, postingsAdded);
    WritableUtils.writeVInt(out, df == 0 ? postingsAdded : df);
    WritableUtils.writeVLong(out, cf == 0 ? sumOfPostingsScore : cf);

    out.writeInt(lastBlockSize);
    out.writeInt(docidCompressed.length);
    for(int i = 0; i < docidCompressed.length; i++) {
      out.writeInt(docidCompressed[i].length);
      for(int j = 0; j < docidCompressed[i].length; j++) {
        out.writeInt(docidCompressed[i][j]);
      }
    }

    for(int i = 0; i < tfCompressed.length; i++) {
      out.writeInt(tfCompressed[i].length);
      for(int j = 0; j < tfCompressed[i].length; j++) {
        out.writeInt(tfCompressed[i][j]);
      }
    }

    for(int i = 0; i < offsetCompressed.length; i++) {
      out.writeInt(offsetCompressed[i].length);
      for(int j = 0; j < offsetCompressed[i].length; j++) {
        out.writeInt(offsetCompressed[i][j]);
      }
    }

    out.writeInt(positionsLastBlockSize);
    out.writeInt(positionsCompressed.length);
    for(int i = 0; i < positionsCompressed.length; i++) {
      out.writeInt(positionsCompressed[i].length);
      for(int j = 0; j < positionsCompressed[i].length; j++) {
        out.writeInt(positionsCompressed[i][j]);
      }
    }
  }

  @Override public byte[] serialize() throws IOException {
    Preconditions.checkArgument(postingsAdded > 0);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  public static PostingsListDocSortedPositionalPForDelta create(DataInput in) throws IOException {
    PostingsListDocSortedPositionalPForDelta p = new PostingsListDocSortedPositionalPForDelta();
    p.readFields(in);
    return p;
  }

  public static PostingsListDocSortedPositionalPForDelta create(byte[] bytes) throws IOException {
    return PostingsListDocSortedPositionalPForDelta.create(
        new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * {@code PostingsReader} for {@code PostingsListDocSortedPositionalPForDelta}.
   *
   * @author Nima Asadi
   */
  public static class PostingsReader implements ivory.core.data.index.PostingsReader {
    private int currentBlock = -1;
    private int currentOffsetBlock = -1;
    private int currentPositionBlock = -1;
    private int[] docidBlock = null;
    private int[] offsetBlock = null;
    private int[] tfBlock = null;
    private int[] positionBlock = null;

    private int cnt = 0;
    private int[] curPositions;
    private short innerPrevTf;
    private int innerPrevDocno;
    private int innerNumPostings;
    private PostingsListDocSortedPositionalPForDelta postingsList;

    protected PostingsReader(int numPostings,
        PostingsListDocSortedPositionalPForDelta list) {
      Preconditions.checkNotNull(list);
      Preconditions.checkArgument(numPostings > 0);

      docidBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      tfBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      offsetBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      positionBlock = new int[PForDeltaUtility.BLOCK_SIZE];

      innerNumPostings = numPostings;
      postingsList = list;
    }

    @Override
    public int getNumberOfPostings() {
      return innerNumPostings;
    }

    @Override
    public void reset() {
      currentBlock = -1;
      currentOffsetBlock = -1;
      currentPositionBlock = -1;
      docidBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      tfBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      offsetBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      positionBlock = new int[PForDeltaUtility.BLOCK_SIZE];
      cnt = 0;
    }

    @Override
    public boolean nextPosting(Posting p) {
      if(!hasMorePostings()) {
        return false;
      }

      int blockNumber = (int) ((double) cnt / (double) PForDeltaUtility.BLOCK_SIZE);
      int inBlockIndex = cnt % PForDeltaUtility.BLOCK_SIZE;

      // If this is the last block, use lastBlockSize instead of the default
      // block size
      if(blockNumber == postingsList.docidCompressed.length - 1 &&
         currentBlock != blockNumber) {
        docidBlock = new int[postingsList.lastBlockSize];
        tfBlock = new int[postingsList.lastBlockSize];
      }

      if(currentBlock != blockNumber) {
        //Docids are gap-compressed
        PForDelta.decompressOneBlock(docidBlock, postingsList.docidCompressed[blockNumber], docidBlock.length);
        for(int i = 1; i < docidBlock.length; i++) {
          docidBlock[i] += docidBlock[i - 1];
        }
        PForDelta.decompressOneBlock(tfBlock, postingsList.tfCompressed[blockNumber], tfBlock.length);
      }

      p.setDocno(docidBlock[inBlockIndex]);
      p.setTf((short) tfBlock[inBlockIndex]);

      currentBlock = blockNumber;
      cnt++;
      innerPrevDocno = p.getDocno();
      innerPrevTf = p.getTf();
      curPositions = null;

      return true;
    }

    @Override
    public int[] getPositions() {
      if (curPositions != null) {
        return curPositions;
      }

      // Grab the offset value for the current posting
      int cnt = this.cnt - 1;
      int blockNumber = (int) ((double) cnt / (double) PForDeltaUtility.BLOCK_SIZE);
      int inBlockIndex = cnt % PForDeltaUtility.BLOCK_SIZE;

      // If this is the last block, use lastBlockSize instead of the default
      // block size
      if(blockNumber == postingsList.offsetCompressed.length - 1 &&
         currentOffsetBlock != blockNumber) {
        offsetBlock = new int[postingsList.lastBlockSize];
      }

      if(currentOffsetBlock != blockNumber) {
        // Offset values are gap-compressed
        PForDelta.decompressOneBlock(offsetBlock, postingsList.offsetCompressed[blockNumber], offsetBlock.length);
        for(int i = 1; i < offsetBlock.length; i++) {
          offsetBlock[i] += offsetBlock[i - 1];
        }
      }

      int[] pos = new int[getTf()];

      // Term positions of a posting can span multiple blocks.
      // Therefore, we have to compute the position (i.e.,
      // block number and offset within the block) where term positions
      // begin and the position where the list ends.
      int beginOffset = offsetBlock[inBlockIndex];
      int endOffset = beginOffset + pos.length - 1;
      int beginBlock = (int) ((double) beginOffset/(double) PForDeltaUtility.BLOCK_SIZE);

      if(beginBlock != currentPositionBlock &&
         beginBlock == postingsList.positionsCompressed.length - 1) {
        positionBlock = new int[postingsList.positionsLastBlockSize];
      }

      // If the block we are going to probe is different from
      // the current block that has been decompressed
      if(beginBlock != currentPositionBlock) {
        PForDelta.decompressOneBlock(positionBlock, postingsList.positionsCompressed[beginBlock], positionBlock.length);
      }

      int posIndex = 0;
      beginOffset %= PForDeltaUtility.BLOCK_SIZE;
      int endBlock = (int) ((double) endOffset / (double) PForDeltaUtility.BLOCK_SIZE);
      endOffset %= PForDeltaUtility.BLOCK_SIZE;

      // If the list of term positions span across mutliple blocks
      if(endBlock != beginBlock) {
        pos[posIndex++] = positionBlock[beginOffset];
        for(int i = beginOffset + 1; i < positionBlock.length; i++) {
          pos[posIndex] = positionBlock[i] + pos[posIndex - 1];
          posIndex++;
        }

        for(int i = beginBlock + 1; i < endBlock; i++) {
          PForDelta.decompressOneBlock(positionBlock, postingsList.positionsCompressed[i], positionBlock.length);
          for(int j = 0; j < positionBlock.length; j++) {
            pos[posIndex] = positionBlock[j] + pos[posIndex - 1];
            posIndex++;
          }
        }

        if(endBlock == postingsList.positionsCompressed.length - 1) {
          positionBlock = new int[postingsList.positionsLastBlockSize];
        }

        PForDelta.decompressOneBlock(positionBlock, postingsList.positionsCompressed[endBlock], positionBlock.length);

        for(int i = 0; i <= endOffset; i++) {
          pos[posIndex] = positionBlock[i] + pos[posIndex - 1];
          posIndex++;
        }
      } else {
        pos[posIndex++] = positionBlock[beginOffset];
        for(int i = beginOffset + 1; i <= endOffset; i++) {
          pos[posIndex] = positionBlock[i] + pos[posIndex - 1];
          posIndex++;
        }
      }

      currentPositionBlock = endBlock;
      currentOffsetBlock = blockNumber;
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
      return innerPrevTf;
    }
  }

  public static PostingsListDocSortedPositionalPForDelta merge(PostingsListDocSortedPositionalPForDelta plist1,
      PostingsListDocSortedPositionalPForDelta plist2, int docs) {
    Preconditions.checkNotNull(plist1);
    Preconditions.checkNotNull(plist2);

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    PostingsListDocSortedPositionalPForDelta newPostings = new PostingsListDocSortedPositionalPForDelta();
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

  public static PostingsListDocSortedPositionalPForDelta merge(PostingsList plist1,
      PostingsList plist2, int docs) {
    Preconditions.checkNotNull(plist1);
    Preconditions.checkNotNull(plist2);

    plist1.setCollectionDocumentCount(docs);
    plist2.setCollectionDocumentCount(docs);

    int numPostings1 = plist1.getNumberOfPostings();
    int numPostings2 = plist2.getNumberOfPostings();

    PostingsListDocSortedPositionalPForDelta newPostings = new PostingsListDocSortedPositionalPForDelta();
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

  private static class PForDeltaUtility {
    public static final int BLOCK_SIZE = 128;

    private int[][] docidCompressed;
    private int[][] offsetCompressed;
    private int[][] tfCompressed;
    private int lastBlockSize;
    private int[][] positionsCompressed;
    private int positionsLastBlockSize;

    private int nbPostings;
    private int[] docids = null;
    private int[] tfs = null;
    private int[] offsets = null;
    private List<Integer> positionsRaw;
    private List<int[]> positions;
    private int index;
    private int positionIndex;
    private int blockIndex;

    public PForDeltaUtility(int nbPostings) {
      this.nbPostings = nbPostings;
      int nbBlocks = (int) Math.ceil(((double) nbPostings) / ((double) BLOCK_SIZE));

      docidCompressed = new int[nbBlocks][];
      offsetCompressed = new int[nbBlocks][];
      tfCompressed = new int[nbBlocks][];
      lastBlockSize = computeLastBlockSize(nbPostings, nbBlocks, BLOCK_SIZE);

      if(nbBlocks > 1) {
        docids = new int[BLOCK_SIZE];
        tfs = new int[BLOCK_SIZE];
        offsets = new int[BLOCK_SIZE];
      } else {
        docids = new int[lastBlockSize];
        tfs = new int[lastBlockSize];
        offsets = new int[lastBlockSize];
      }
      positionsRaw = Lists.newArrayList();
      positions = Lists.newArrayList();
      index = 0;
      positionIndex = 0;
      blockIndex = 0;
    }

    /**
     * Adds a posting.
     *
     * @param docid Document ID
     * @param tf Term frequency
     * @param pos Term positions
     * @return Whether or not all postings have been added
     */
    public boolean add(int docid, int tf, TermPositions pos) {
      Preconditions.checkNotNull(pos);

      // Total number of elements added to this postings list
      int elements = blockIndex * BLOCK_SIZE + index + 1;

      this.docids[index] = docid;
      this.tfs[index] = tf;
      int[] posArray = pos.getPositions();
      this.offsets[index] = positionIndex;
      positionIndex += posArray.length;
      index++;

      // If number of postings accumulated so far is equal to the block size,
      // then compress the current block and store it in the compressed array
      if(index == this.docids.length) {
        docidCompressed[blockIndex] = compressOneBlock(this.docids, this.docids.length, true);
        tfCompressed[blockIndex] = compressOneBlock(this.tfs, this.tfs.length, false);
        offsetCompressed[blockIndex] = compressOneBlock(this.offsets, this.offsets.length, true);

        blockIndex++;
        index = 0;

        if(blockIndex == docidCompressed.length - 1) {
          this.docids = new int[lastBlockSize];
          this.tfs = new int[lastBlockSize];
          this.offsets = new int[lastBlockSize];
        }
      }

      // Add position gaps to the list of accumulated term positions
      positionsRaw.add(posArray[0]);
      for(int j = 1; j < posArray.length; j++) {
        positionsRaw.add(posArray[j] - posArray[j - 1]);
      }

      // If number of accumulated term positions is larger than the block size,
      // compress blocks of term positions until the number of remaining elements
      // is less than the block size
      while(positionsRaw.size() > BLOCK_SIZE ||
            (elements == nbPostings && !positionsRaw.isEmpty())) {
        int blockSize = BLOCK_SIZE;
        if(elements == nbPostings && positionsRaw.size() <= BLOCK_SIZE) {
          blockSize = positionsRaw.size();
          positionsLastBlockSize = blockSize;
        }

        int[] temp = new int[blockSize];
        for(int i = 0; i < temp.length; i++) {
          temp[i] = positionsRaw.get(i);
        }
        for(int i = 0; i < temp.length; i++) {
          positionsRaw.remove(0);
        }
        positions.add(compressOneBlock(temp, temp.length, false));
      }

      // When all postings have been inserted, reformat the compressed
      // term positions
      if(elements == nbPostings) {
        positionsCompressed = new int[positions.size()][];
        for(int i = 0; i < positionsCompressed.length; i++) {
          positionsCompressed[i] = positions.get(i);
        }
        return true;
      }
      return false;
    }

    public int[][] getCompressedDocids() {
      return docidCompressed;
    }

    public int[][] getCompressedOffsets() {
      return offsetCompressed;
    }

    public int[][] getCompressedTfs() {
      return tfCompressed;
    }

    public int getLastBlockSize() {
      return lastBlockSize;
    }

    public int[][] getCompressedPositions() {
      return positionsCompressed;
    }

    public int getPositionsLastBlockSize() {
      return positionsLastBlockSize;
    }

    private static int[] compressOneBlock(int[] data, int blockSize, boolean computeGaps) {
      if(!computeGaps) {
        return PForDelta.compressOneBlockOpt(data, blockSize);
      }

      int[] temp = new int[blockSize];
      temp[0] = data[0];
      for(int j = 1; j < temp.length; j++) {
        temp[j] = data[j] - data[j - 1];
      }
      return PForDelta.compressOneBlockOpt(temp, blockSize);
    }

    private static int computeLastBlockSize(int dataLength, int nbBlocks, int blockSize) {
      return dataLength - ((nbBlocks - 1) * blockSize);
    }
  }
}
