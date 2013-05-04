package ivory.ffg.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.kamikaze.pfordelta.PForDelta;

import ivory.core.data.index.TermPositions;

/**
 * Implementation of an (mini-)indexed document vector refered to as IDV.
 * In this class, term ids are encoded using PForDelta whereas positions
 * are compressed using the gamma coding.
 *
 * @author Nima Asadi
 */

public class DocumentVectorMiniInvertedIndex implements DocumentVector {
  private static final int BLOCK_SIZE = 128;

  private int documentLength; // Length of the document vector
  private int termidsLastBlockSize;
  private int[][] termids; // Compressed term id vectors
  private int positionalSkipListLastBlockSize; //Length of the forward index vector (holds the index of position arrays)
  private int[][] positionalSkipListCompressedBlock; // Compressed forward index vector
  private int tfLastBlockSize;
  private int[][] tfCompressedBlock; // Compressed forward index vector
  private int positionalLastBlockSize;
  private int[][] positionalCompressedBlock; // Array of positions

  protected DocumentVectorMiniInvertedIndex() {
  }

  /**
   * Constructs an indexed document vector given the input term id vector and the term positions
   *
   * @param data Array of term ids
   * @param positions List of TermPosition objects, one for each term id
   * @return A compressed, indexed document vector.
   */
  public static DocumentVectorMiniInvertedIndex newInstance(int[] data, List<TermPositions> positions, int documentLength)
    throws IOException {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(positions);
    Preconditions.checkArgument(data.length == positions.size());
    Preconditions.checkArgument(documentLength >= data.length);

    DocumentVectorMiniInvertedIndex postings = new DocumentVectorMiniInvertedIndex();

    // Compress term ids using PForDelta
    postings.documentLength = documentLength;
    postings.termids = DocumentVectorUtility.compressData(data, BLOCK_SIZE, true);
    postings.termidsLastBlockSize =
      DocumentVectorUtility.lastBlockSize(data.length,
                                          postings.termids.length,
                                          BLOCK_SIZE);

    // Create a forward index to the position array and encode the positions
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
      DocumentVectorUtility.compressData(tempPositions, BLOCK_SIZE, false);
    postings.positionalLastBlockSize =
      DocumentVectorUtility.lastBlockSize(tempPositions.length,
                                          postings.positionalCompressedBlock.length,
                                          BLOCK_SIZE);

    postings.positionalSkipListCompressedBlock =
      DocumentVectorUtility.compressData(skipList, BLOCK_SIZE, true);
    postings.positionalSkipListLastBlockSize =
      DocumentVectorUtility.lastBlockSize(skipList.length,
                                          postings.positionalSkipListCompressedBlock.length,
                                          BLOCK_SIZE);

    postings.tfCompressedBlock =
      DocumentVectorUtility.compressData(tf, BLOCK_SIZE, false);
    postings.tfLastBlockSize =
      DocumentVectorUtility.lastBlockSize(tf.length,
                                          postings.tfCompressedBlock.length,
                                          BLOCK_SIZE);

    return postings;
  }


  /**
   * Decompresses the forward index to position array.
   *
   * @return Decompressed forward index used when retrieving positions
   */
  private int[] decompressSkipList(int block) {
    int[] outBlock = null;
    if(block == positionalSkipListCompressedBlock.length - 1) {
      outBlock = new int[positionalSkipListLastBlockSize];
    } else {
      outBlock = new int[BLOCK_SIZE];
    }

    PForDelta.decompressOneBlock(outBlock, positionalSkipListCompressedBlock[block], outBlock.length);
    for(int i = 1; i < outBlock.length; i++) {
      outBlock[i] += outBlock[i - 1];
    }
    return outBlock;
  }

  private int[] decompressTermFrequency(int block) {
    int[] outBlock = null;
    if(block == tfCompressedBlock.length - 1) {
      outBlock = new int[tfLastBlockSize];
    } else {
      outBlock = new int[BLOCK_SIZE];
    }

    PForDelta.decompressOneBlock(outBlock, tfCompressedBlock[block], outBlock.length);
    return outBlock;
  }

  /**
   * Decompresses the term id array
   *
   * @return Decompressed term id array
   */
  private int[] decompressTermids(int block) {
    int[] outBlock = null;
    if(block == termids.length - 1) {
      outBlock = new int[termidsLastBlockSize];
    } else {
      outBlock = new int[BLOCK_SIZE];
    }

    PForDelta.decompressOneBlock(outBlock, termids[block], outBlock.length);
    for(int i = 1; i < outBlock.length; i++) {
      outBlock[i] += outBlock[i - 1];
    }
    return outBlock;
  }

  @Override public int[][] decompressPositions(int[] terms) throws IOException {
    Preconditions.checkNotNull(terms);

    int[][] positions = new int[terms.length][];
    if(documentLength == 0) {
      return positions;
    }

    int skipListBlock = -1;
    int positionBlock = -1;
    int[] tempSkipList = null;
    int[] tempTermFrequency = null;
    int[] tempPositions = null;

    int found = 0;
    for(int i = 0; i < termids.length; i++) {
      if(found >= terms.length) {
        return positions;
      }

      int[] tids = decompressTermids(i);
      for(int j = 0; j < tids.length; j++) {
        if(found >= terms.length) {
          return positions;
        }

        for(int q = 0; q < terms.length; q++) {
          if(terms[q] == tids[j]) {
            found++;

            if(i != skipListBlock || tempSkipList == null) {
              tempSkipList = decompressSkipList(i);
              tempTermFrequency = decompressTermFrequency(i);
              skipListBlock = i;
            }

            positions[q] = new int[tempTermFrequency[j]];
            int beginOffset = tempSkipList[j];
            int endOffset = beginOffset + positions[q].length - 1;
            int block = beginOffset/BLOCK_SIZE;

            if(block != positionBlock || tempPositions == null) {
              if(block == positionalCompressedBlock.length - 1) {
                tempPositions = new int[positionalLastBlockSize];
              } else {
                tempPositions = new int[BLOCK_SIZE];
              }
              PForDelta.decompressOneBlock(tempPositions, positionalCompressedBlock[block], tempPositions.length);
              positionBlock = block;
            }

            beginOffset %= BLOCK_SIZE;
            int bufferIndex = 0;
            int endBlock = endOffset/BLOCK_SIZE;
            endOffset %= BLOCK_SIZE;

            if(endBlock != block) {
              positions[q][bufferIndex++] = tempPositions[beginOffset];
              for(int o = beginOffset + 1; o < tempPositions.length; o++) {
                positions[q][bufferIndex] = tempPositions[o] + positions[q][bufferIndex - 1];
                bufferIndex++;
              }

              for(int k = block + 1; k < endBlock; k++) {
                PForDelta.decompressOneBlock(tempPositions, positionalCompressedBlock[k], tempPositions.length);
                for(int l = 0; l < tempPositions.length; l++) {
                  positions[q][bufferIndex] = tempPositions[l] + positions[q][bufferIndex - 1];
                  bufferIndex++;
                }
              }

              if(endBlock == positionalCompressedBlock.length - 1) {
                tempPositions = new int[positionalLastBlockSize];
              } else {
                tempPositions = new int[BLOCK_SIZE];
              }
              PForDelta.decompressOneBlock(tempPositions, positionalCompressedBlock[endBlock], tempPositions.length);
              positionBlock = endBlock;

              for(int o = 0; o <= endOffset; o++) {
                positions[q][bufferIndex] = tempPositions[o] + positions[q][bufferIndex - 1];
                bufferIndex++;
              }
            } else {
              positions[q][bufferIndex++] = tempPositions[beginOffset];
              for(int o = beginOffset + 1; o <= endOffset; o++) {
                positions[q][bufferIndex] = tempPositions[o] + positions[q][bufferIndex - 1];
                bufferIndex++;
              }
            }
          }
        }
      }
    }

    return positions;
  }

  @Override public int[] decompressDocument() throws IOException {
    throw new UnsupportedOperationException("Implementation not available!");
  }

  @Override public int[] transformTerms(int[] terms) {
    return terms;
  }

  @Override public int getDocumentLength() {
    return documentLength;
  }

  @Override public void write(DataOutput output) throws IOException {
    Preconditions.checkNotNull(output);

    output.writeInt(documentLength);
    output.writeInt(termidsLastBlockSize);
    output.writeInt(termids.length);
    for(int i = 0; i < termids.length; i++) {
      output.writeInt(termids[i].length);
      for(int j = 0; j < termids[i].length; j++) {
        output.writeInt(termids[i][j]);
      }
    }

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

    documentLength = input.readInt();
    termidsLastBlockSize = input.readInt();
    termids = new int[input.readInt()][];
    for(int i = 0; i < termids.length; i++) {
      termids[i] = new int[input.readInt()];
      for(int j = 0; j < termids[i].length; j++) {
        termids[i][j] = input.readInt();
      }
    }

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
   * Reads and returns an instance of this class from input.
   *
   * @param input DataInput
   * @return An instance of this class.
   */
  public static DocumentVectorMiniInvertedIndex readInstance(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    DocumentVectorMiniInvertedIndex postings = new DocumentVectorMiniInvertedIndex();
    postings.readFields(input);
    return postings;
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof DocumentVectorMiniInvertedIndex);

    DocumentVectorMiniInvertedIndex other = (DocumentVectorMiniInvertedIndex) o;
    if(this.documentLength != other.documentLength ||
       this.termidsLastBlockSize != other.termidsLastBlockSize ||
       this.termids.length != other.termids.length ||
       this.positionalSkipListLastBlockSize != other.positionalSkipListLastBlockSize ||
       this.positionalSkipListCompressedBlock.length != other.positionalSkipListCompressedBlock.length ||
       this.tfLastBlockSize != other.tfLastBlockSize ||
       this.tfCompressedBlock.length != other.tfCompressedBlock.length ||
       this.positionalLastBlockSize != other.positionalLastBlockSize ||
       this.positionalCompressedBlock.length != other.positionalCompressedBlock.length) {
      return false;
    }
    for(int i = 0; i < termids.length; i++) {
      if(this.termids[i].length != other.termids[i].length) {
        return false;
      }
      for(int j = 0; j < termids[i].length; j++) {
        if(this.termids[i][j] != other.termids[i][j]) {
          return false;
        }
      }
    }
    for(int i = 0; i < positionalSkipListCompressedBlock.length; i++) {
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
    for(int i = 0; i < tfCompressedBlock.length; i++) {
      if(this.tfCompressedBlock[i].length != other.tfCompressedBlock[i].length) {
        return false;
      }
      for(int j = 0; j < tfCompressedBlock[i].length; j++) {
        if(this.tfCompressedBlock[i][j] !=
           other.tfCompressedBlock[i][j]) {
          return false;
        }
      }
    }
    for(int i = 0; i < positionalCompressedBlock.length; i++) {
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
