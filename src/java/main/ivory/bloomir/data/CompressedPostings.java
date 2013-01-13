package ivory.bloomir.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

import com.kamikaze.pfordelta.PForDelta;

import org.apache.hadoop.io.Writable;

/**
 * A compressed postings list representation. This class uses
 * PForDelta to compress a given set of document ids into
 * blocks of equal size.
 *
 * @author Nima Asadi
 */
public class CompressedPostings implements Writable {
  private static int blockSize = 128;
  private int[][] compressedBlocks;
  private int lastBlockSize;

  protected CompressedPostings() {
  }

  protected static void setBlockSize(int bSize) {
    Preconditions.checkArgument(bSize > 0);

    blockSize = bSize;
  }

  /**
   * Compresses the given input and generates a new
   * instance of this class.
   *
   * @param data Array of sorted document ids
   * @return an instance of this class which contains the
   * given data in a compresed format.
   */
  public static CompressedPostings newInstance(int[] data) {
    Preconditions.checkNotNull(data);

    CompressedPostings postings = new CompressedPostings();
    postings.compressData(data);
    return postings;
  }

  protected void compressData(int[] data) {
    Preconditions.checkNotNull(data);

    // Data is stored in blocks of equal size..
    int nbBlocks = (int) Math.ceil(((double) data.length) / ((double) blockSize));
    compressedBlocks = new int[nbBlocks][];

    int[] temp = new int[blockSize];

    // Compress all blocks except for the last block which might
    // contain fewer elements.
    for(int i = 0; i < nbBlocks - 1; i++) {
      temp[0] = data[i * blockSize];
      int pre = temp[0];
      for(int j = 1; j < temp.length; j++) {
        temp[j] = data[i * blockSize + j] - pre;
        pre = data[i * blockSize + j];
      }
      compressedBlocks[i] = PForDelta.compressOneBlockOpt(temp, blockSize);
    }

    // Compress the last block
    int remaining = data.length - ((nbBlocks - 1) * blockSize);
    temp = new int[remaining];
    temp[0] = data[(nbBlocks - 1) * blockSize];
    int pre = temp[0];
    for(int j = 1; j < temp.length; j++) {
      temp[j] = data[(nbBlocks - 1) * blockSize + j] - pre;
      pre = data[(nbBlocks - 1) * blockSize + j];
    }
    compressedBlocks[nbBlocks - 1] = PForDelta.compressOneBlockOpt(temp, remaining);
    lastBlockSize = remaining;
  }

  /**
   * Decompresses a block, stores the decompressed data in an array and returns
   * the number of decompressed elements as output.
   *
   * To read the n_th element of this block, add outBlock[n-1] to the current
   * <i>value</i>. That is:
   *
   *     <pre>
   *     original_data[0] = outBlock(0)<br>
   *     original_data[1] = original_data[0] + outBlock[1]<br>
   *     original_data[2] = original_data[1] + outBlock[2]<br>
   *     ...<br>
   *     original_data[n-1] = original_data[n-2] + outBlock[n-1]<br>
   *     </pre>
   *
   * @param outBlock Array to store the decompressed values. Note that the size
   * of this array must be at least equal to the size of each block.
   * @param blockNumber The block index to decompress.
   * @return Number of elements in the decompressed array, starting from index 0.
   */
  public int decompressBlock(int[] outBlock, int blockNumber) {
    Preconditions.checkNotNull(outBlock);
    Preconditions.checkArgument(blockNumber >= 0 && blockNumber < compressedBlocks.length);

    if(blockNumber != compressedBlocks.length - 1) {
      PForDelta.decompressOneBlock(outBlock, compressedBlocks[blockNumber], blockSize);
      return blockSize;
    } else {
      PForDelta.decompressOneBlock(outBlock, compressedBlocks[blockNumber], lastBlockSize);
      return lastBlockSize;
    }
  }

  /**
   * @return The number of blocks in this postings list.
   */
  public int getBlockCount() {
    return compressedBlocks.length;
  }

  /**
   * @return The actual block size.
   */
  public static int getBlockSize() {
    return blockSize;
  }

  /**
   * Computes the block number of a given index. For efficiency
   * purposes we do not check the range of the input index against
   * the number of elements in this postings list.
   *
   * @param index Index of an element in the original data
   * @return Block number.
   */
  public int getBlockNumber(int index) {
    Preconditions.checkArgument(index >= 0);
    return (int) Math.floor(((double) index) / ((double) blockSize));
  }

  /**
   * Returns the index (in the original data) of the
   * first element in the compressed block.
   *
   * @param blockNumber Block index.
   * @return Index of the first value in the specified block.
   */
  public int getBlockStartIndex(int blockNumber) {
    Preconditions.checkArgument(blockNumber >= 0 && blockNumber < compressedBlocks.length);
    return blockNumber * blockSize;
  }

  /**
   * @param index Index of an element in the original data
   * @return Whether or not this element is the first element of a
   * block. For more information on how to read the elements
   * please refer to {@link #decompressBlock}
   */
  public boolean isFirstElementInBlock(int index) {
    Preconditions.checkArgument(index >= 0);
    return (getPositionInBlock(index) == 0);
  }

  /**
   * @param index Index of an element in the original data
   * @return Index of the element in the block.
   */
  public int getPositionInBlock(int index) {
    Preconditions.checkArgument(index >= 0);
    return index % blockSize;
  }

  /**
   * Reads an instance of this class from a given input.
   *
   * @param input Data input
   * @return An instance of this class.
   */
  public static CompressedPostings readInstance(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    CompressedPostings postings = new CompressedPostings();
    postings.readFields(input);
    return postings;
  }

  @Override public void readFields(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    lastBlockSize = input.readInt();
    compressedBlocks = new int[input.readInt()][];
    for(int i = 0; i < compressedBlocks.length; i++) {
      compressedBlocks[i] = new int[input.readInt()];
      for(int j = 0; j < compressedBlocks[i].length; j++) {
        compressedBlocks[i][j] = input.readInt();
      }
    }
  }

  @Override public void write(DataOutput output) throws IOException {
    Preconditions.checkNotNull(output);

    output.writeInt(lastBlockSize);
    output.writeInt(compressedBlocks.length);
    for(int i = 0; i < compressedBlocks.length; i++) {
      output.writeInt(compressedBlocks[i].length);
      for(int j = 0; j < compressedBlocks[i].length; j++) {
        output.writeInt(compressedBlocks[i][j]);
      }
    }
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof CompressedPostings);

    CompressedPostings other = (CompressedPostings) o;
    if(this.lastBlockSize != other.lastBlockSize) {
      return false;
    }
    if(this.compressedBlocks.length != other.compressedBlocks.length) {
      return false;
    }
    for(int i = 0; i < this.compressedBlocks.length; i++) {
      if(this.compressedBlocks[i].length != other.compressedBlocks[i].length) {
        return false;
      }
      for(int j = 0; j < this.compressedBlocks[i].length; j++) {
        if(this.compressedBlocks[i][j] != other.compressedBlocks[i][j]) {
          return false;
        }
      }
    }
    return true;
  }
}
