package ivory.ffg.data;

import ivory.bloomir.data.CompressedPostings;
import ivory.core.data.index.TermPositions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.kamikaze.pfordelta.PForDelta;

/**
 * A compressed positional postings list representation. This class uses
 * PForDelta to compress a given set of document ids into
 * blocks of equal size.
 *
 * In this implementation, the positions are compressed using gamma coding.
 *
 * @author Nima Asadi
 */
public class CompressedPositionalPostings extends CompressedPostings {
  private int[][] positionalSkipListCompressedBlock; // Compressed forward index to positions
  private int positionalSkipListLastBlockSize;
  private int[][] positionalCompressedBlock; // Encoded positions
  private int positionalLastBlockSize;
  private int[][] tfCompressedBlock;
  private int tfLastBlockSize;

  private int currentBlock = -1;
  private int currentPositionsBlock = -1;
  private int[] skipList = null;
  private int[] termFrequency = null;
  private int[] positions = null;

  private CompressedPositionalPostings() {
    super();
    setBlockSize(128);
  }

  /**
   * Constructs an instance of this class by encoding the document ids as well as their term positions
   *
   * @param data An array of document ids
   * @param positions List of TermPosition objects, one for each document id
   * @return A compressed positional postings list object
   */
  public static CompressedPositionalPostings newInstance(int[] data, List<TermPositions> positions) throws IOException {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(positions);
    Preconditions.checkArgument(data.length == positions.size());

    CompressedPositionalPostings postings = new CompressedPositionalPostings();

    // Use the super class to encode the document ids
    postings.compressData(data);

    // Create a forward index to the position array and encode the positions using
    int[] skipList = new int[data.length];
    int[] tf = new int[data.length];
    int skipListIndex = 0;

    List<Integer> buffer = Lists.newArrayList();
    for(int i = 0; i < positions.size(); i++) {
      int[] pos = positions.get(i).getPositions();
      tf[skipListIndex] = pos.length;
      skipList[skipListIndex++] = buffer.size();

      if(pos.length > 0) {
        buffer.add(pos[0]);
        for(int j = 1; j < pos.length; j++) {
          buffer.add(pos[j] - pos[j - 1]);
        }
      }
    }

    int[] tempPositions  = new int[buffer.size()];
    for(int i = 0; i < buffer.size(); i++) {
      tempPositions[i] = buffer.get(i);
    }

    postings.positionalCompressedBlock =
      DocumentVectorUtility.compressData(tempPositions, getBlockSize(), false);
    postings.positionalLastBlockSize =
      DocumentVectorUtility.lastBlockSize(tempPositions.length,
                                          postings.positionalCompressedBlock.length,
                                          getBlockSize());

    postings.positionalSkipListCompressedBlock =
      DocumentVectorUtility.compressData(skipList, getBlockSize(), true);
    postings.positionalSkipListLastBlockSize =
      DocumentVectorUtility.lastBlockSize(skipList.length,
                                          postings.positionalSkipListCompressedBlock.length,
                                          getBlockSize());

    postings.tfCompressedBlock =
      DocumentVectorUtility.compressData(tf, getBlockSize(), false);
    postings.tfLastBlockSize =
      DocumentVectorUtility.lastBlockSize(tf.length,
                                          postings.tfCompressedBlock.length,
                                          getBlockSize());


    return postings;
  }

  /**
   * Decompresses the forward index
   */
  private void decompressMetaData(int index) {
    int block = index/getBlockSize();

    if(block == positionalSkipListCompressedBlock.length - 1) {
      skipList = new int[positionalSkipListLastBlockSize];
      termFrequency = new int[tfLastBlockSize];
    } else {
      skipList = new int[getBlockSize()];
      termFrequency = new int[getBlockSize()];
    }

    PForDelta.decompressOneBlock(skipList, positionalSkipListCompressedBlock[block], skipList.length);
    for(int i = 1; i < skipList.length; i++) {
      skipList[i] += skipList[i - 1];
    }

    PForDelta.decompressOneBlock(termFrequency, tfCompressedBlock[block], termFrequency.length);
  }

  /**
   * Retrieves positions for a document given a forward index and the index of the document
   * in the document id vector.
   *
   * @param index Index of the document id in the document id vector
   * @return Positions of the occurrences of the term within a document
   */
  public int[] decompressPositions(int index) throws IOException {
    Preconditions.checkArgument(index >= 0);

    if((index/getBlockSize()) != currentBlock || skipList == null) {
      decompressMetaData(index);
      currentBlock = index/getBlockSize();
    }

    int[] buffer = new int[termFrequency[index % getBlockSize()]];
    int beginOffset = skipList[index%getBlockSize()];
    int endOffset = beginOffset + buffer.length - 1;
    int block = beginOffset/getBlockSize();

    if(block != currentPositionsBlock || positions == null) {
      if(block == positionalCompressedBlock.length - 1) {
        positions = new int[positionalLastBlockSize];
      } else {
        positions = new int[getBlockSize()];
      }
      PForDelta.decompressOneBlock(positions, positionalCompressedBlock[block], positions.length);
      currentPositionsBlock = block;
    }

    beginOffset %= getBlockSize();
    int bufferIndex = 0;
    int endBlock = endOffset/getBlockSize();
    endOffset %= getBlockSize();

    if(endBlock != block) {
      buffer[bufferIndex++] = positions[beginOffset];
      for(int i = beginOffset + 1; i < positions.length; i++) {
        buffer[bufferIndex] = positions[i] + buffer[bufferIndex - 1];
        bufferIndex++;
      }

      for(int i = block + 1; i < endBlock; i++) {
        PForDelta.decompressOneBlock(positions, positionalCompressedBlock[i], positions.length);
        for(int j = 0; j < positions.length; j++) {
          buffer[bufferIndex] = positions[j] + buffer[bufferIndex - 1];
          bufferIndex++;
        }
      }

      if(endBlock == positionalCompressedBlock.length - 1) {
        positions = new int[positionalLastBlockSize];
      } else {
        positions = new int[getBlockSize()];
      }
      PForDelta.decompressOneBlock(positions, positionalCompressedBlock[endBlock], positions.length);
      currentPositionsBlock = endBlock;

      for(int i = 0; i <= endOffset; i++) {
        buffer[bufferIndex] = positions[i] + buffer[bufferIndex - 1];
        bufferIndex++;
      }
    } else {
      buffer[bufferIndex++] = positions[beginOffset];
      for(int i = beginOffset + 1; i <= endOffset; i++) {
        buffer[bufferIndex] = positions[i] + buffer[bufferIndex - 1];
        bufferIndex++;
      }
    }

    return buffer;
  }

  public void close() {
    termFrequency = null;
    skipList = null;
    positions = null;
    currentBlock = -1;
    currentPositionsBlock = -1;
  }

  @Override public void write(DataOutput output) throws IOException {
    Preconditions.checkNotNull(output);

    super.write(output);

    output.writeInt(positionalSkipListLastBlockSize);
    output.writeInt(positionalSkipListCompressedBlock.length);
    for(int i = 0; i < positionalSkipListCompressedBlock.length; i++) {
      output.writeInt(positionalSkipListCompressedBlock[i].length);
      for(int j = 0; j < positionalSkipListCompressedBlock[i].length; j++) {
        output.writeInt(positionalSkipListCompressedBlock[i][j]);
      }
    }

    output.writeInt(tfLastBlockSize);
    output.writeInt(tfCompressedBlock.length);
    for(int i = 0; i < tfCompressedBlock.length; i++) {
      output.writeInt(tfCompressedBlock[i].length);
      for(int j = 0; j < tfCompressedBlock[i].length; j++) {
        output.writeInt(tfCompressedBlock[i][j]);
      }
    }

    output.writeInt(positionalLastBlockSize);
    output.writeInt(positionalCompressedBlock.length);
    for(int i = 0; i < positionalCompressedBlock.length; i++) {
      output.writeInt(positionalCompressedBlock[i].length);
      for(int j = 0; j < positionalCompressedBlock[i].length; j++) {
        output.writeInt(positionalCompressedBlock[i][j]);
      }
    }
  }

  @Override public void readFields(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    super.readFields(input);

    positionalSkipListLastBlockSize = input.readInt();
    positionalSkipListCompressedBlock = new int[input.readInt()][];
    for(int i = 0; i < positionalSkipListCompressedBlock.length; i++) {
      positionalSkipListCompressedBlock[i] = new int[input.readInt()];
      for(int j = 0; j < positionalSkipListCompressedBlock[i].length; j++) {
        positionalSkipListCompressedBlock[i][j] = input.readInt();
      }
    }

    tfLastBlockSize = input.readInt();
    tfCompressedBlock = new int[input.readInt()][];
    for(int i = 0; i < tfCompressedBlock.length; i++) {
      tfCompressedBlock[i] = new int[input.readInt()];
      for(int j = 0; j < tfCompressedBlock[i].length; j++) {
        tfCompressedBlock[i][j] = input.readInt();
      }
    }

    positionalLastBlockSize = input.readInt();
    positionalCompressedBlock = new int[input.readInt()][];
    for(int i = 0; i < positionalCompressedBlock.length; i++) {
      positionalCompressedBlock[i] = new int[input.readInt()];
      for(int j = 0; j < positionalCompressedBlock[i].length; j++) {
        positionalCompressedBlock[i][j] = input.readInt();
      }
    }
  }

  /**
   * Reads and returns an instance of this class from input
   *
   * @param input DataInput
   * @return An instance of the compressed positional postings list
   */
  public static CompressedPositionalPostings readInstance(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    CompressedPositionalPostings postings = new CompressedPositionalPostings();
    postings.readFields(input);
    return postings;
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof CompressedPositionalPostings);

    if(!super.equals(o)) {
      return false;
    }

    CompressedPositionalPostings other = (CompressedPositionalPostings) o;
    if(this.positionalSkipListLastBlockSize != other.positionalSkipListLastBlockSize) {
      return false;
    }
    if(this.positionalSkipListCompressedBlock.length !=
       other.positionalSkipListCompressedBlock.length) {
      return false;
    }
    for(int i = 0; i < this.positionalSkipListCompressedBlock.length; i++) {
      if(this.positionalSkipListCompressedBlock[i].length !=
         other.positionalSkipListCompressedBlock[i].length) {
        return false;
      }
      for(int j = 0; j < positionalSkipListCompressedBlock[i].length; j++) {
        if(this.positionalSkipListCompressedBlock[i][j] !=
           other.positionalSkipListCompressedBlock[i][j]) {
          return false;
        }
      }
    }
    if(this.tfLastBlockSize != other.tfLastBlockSize) {
      return false;
    }
    if(this.tfCompressedBlock.length !=
       other.tfCompressedBlock.length) {
      return false;
    }
    for(int i = 0; i < this.tfCompressedBlock.length; i++) {
      if(this.tfCompressedBlock[i].length !=
         other.tfCompressedBlock[i].length) {
        return false;
      }
      for(int j = 0; j < tfCompressedBlock[i].length; j++) {
        if(this.tfCompressedBlock[i][j] !=
           other.tfCompressedBlock[i][j]) {
          return false;
        }
      }
    }
    if(this.positionalLastBlockSize !=
       other.positionalLastBlockSize) {
      return false;
    }
    if(this.positionalCompressedBlock.length !=
       other.positionalCompressedBlock.length) {
      return false;
    }
    for(int i = 0; i < this.positionalCompressedBlock.length; i++) {
      if(this.positionalCompressedBlock[i].length !=
         other.positionalCompressedBlock[i].length) {
        return false;
      }
      for(int j = 0; j < positionalCompressedBlock[i].length; j++) {
        if(this.positionalCompressedBlock[i][j] !=
           other.positionalCompressedBlock[i][j]) {
          return false;
        }
      }
    }
    return true;
  }
}
