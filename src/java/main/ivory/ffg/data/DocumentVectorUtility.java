package ivory.ffg.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import com.kamikaze.pfordelta.PForDelta;

import ivory.core.compression.BitInputStream;
import ivory.core.compression.BitOutputStream;
import ivory.core.data.document.IntDocVector;
import ivory.core.data.index.TermPositions;

/**
 * Auxiliary functions
 *
 * @author Nima Asadi
 */
public class DocumentVectorUtility {
  public static final int BLOCK_SIZE = 128;
  public static final int MAX_POSITIONS = 100;
  private static final int[] TEMP_POSITIONS = new int[MAX_POSITIONS];

  /**
   * Given a document vector and an array of query terms, this function
   * constructs the positions.
   *
   * @param doc Document vector
   * @param terms Query terms
   * @return Position array for every query term
   */
  public static int[][] getPositions(int[] doc, int[] terms) {
    int[][] positions = new int[terms.length][];
    int pindex = 0;

    for(int i = 0; i < terms.length; i++) {
      pindex = 0;
      for(int j = 0; j < doc.length && pindex < TEMP_POSITIONS.length; j++) {
        if(doc[j] == terms[i]) {
          TEMP_POSITIONS[pindex++] = j + 1;
        }
      }
      positions[i] = new int[pindex];
      for(int j = 0; j < positions[i].length; j++) {
        positions[i][j] = TEMP_POSITIONS[j];
      }
    }

    return positions;
  }

  /**
   * Serializes the positions using gamma codes
   *
   * @param positions Array of positions for a term
   * @return Serialized positions (using gamma codes)
   */
  public static byte[] serializePositions(int[] positions) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    BitOutputStream t = new BitOutputStream(b);

    t.writeGamma(positions.length);

    for (int i = 0; i < positions.length; i++) {
      if (i == 0) {
        t.writeGamma(positions[0] + 1);
      } else {
        int pgap = positions[i] - positions[i - 1];
        t.writeGamma(pgap);
      }
    }

    t.padAndFlush();
    t.close();

    return b.toByteArray();
  }

  /**
   * Deserializes the gamma-encoded positions.
   *
   * @param bytes Serialized positions
   * @return A decoded integer array of positions
   */
  public static int[] deserializePositions(byte[] bytes) throws IOException {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    BitInputStream bitStream = new BitInputStream(byteStream);

    int[] positions = new int[bitStream.readGamma()];
    for(int i = 0; i < positions.length; i++) {
      if (i == 0) {
        positions[i] = bitStream.readGamma() - 1;
      } else {
        positions[i] = (positions[i - 1] + bitStream.readGamma());
      }
    }

    bitStream.close();

    return positions;
  }

  /**
   * Compresses positions using PForDelta compression
   *
   * @param positions Array of positions for a term
   * @return Serialized positions (using PForDelta)
   */
  public static int[][] compressData(int[] data, int blockSize, boolean computeGaps) {
    // Data is stored in blocks of equal size..
    int nbBlocks = (int) Math.ceil(((double) data.length) / ((double) blockSize));
    int[][] compressedBlocks = new int[nbBlocks][];

    int[] temp = new int[blockSize];

    // Compress all blocks except for the last block which might
    // contain fewer elements.
    for(int i = 0; i < nbBlocks - 1; i++) {
      if(!computeGaps) {
        for(int j = 0; j < temp.length; j++) {
          temp[j] = data[i * blockSize + j];
        }
      } else {
        temp[0] = data[i * blockSize];
        int pre = temp[0];
        for(int j = 1; j < temp.length; j++) {
          temp[j] = data[i * blockSize + j] - pre;
          pre = data[i * blockSize + j];
        }
      }
      compressedBlocks[i] = PForDelta.compressOneBlockOpt(temp, blockSize);
    }

    // Compress the last block
    int remaining = lastBlockSize(data.length, nbBlocks, blockSize);
    temp = new int[remaining];
    if(!computeGaps) {
      for(int j = 0; j < temp.length; j++) {
        temp[j] = data[(nbBlocks - 1) * blockSize + j];
      }
    } else {
      temp[0] = data[(nbBlocks - 1) * blockSize];
      int pre = temp[0];
      for(int j = 1; j < temp.length; j++) {
        temp[j] = data[(nbBlocks - 1) * blockSize + j] - pre;
        pre = data[(nbBlocks - 1) * blockSize + j];
      }
    }
    compressedBlocks[nbBlocks - 1] = PForDelta.compressOneBlockOpt(temp, remaining);

    return compressedBlocks;
  }

  public static int lastBlockSize(int dataLength, int nbBlocks, int blockSize) {
    return dataLength - ((nbBlocks - 1) * blockSize);
  }

  /**
   * Factory method
   *
   * @param documentVectorClass DocumentVector class
   * @param document IntDocVector (term positions start from 1)
   * @return New DocumentVector (term positions start from 1)
   */
  public static DocumentVector newInstance(String documentVectorClass, IntDocVector document)
    throws Exception {
    IntDocVector.Reader r = document.getReader();

    if(documentVectorClass.equals(DocumentVectorMiniInvertedIndex.class.getName())) {
      List<Integer> termids = Lists.newArrayList();
      List<TermPositions> positions = Lists.newArrayList();

      int cnt = 0;
      while(r.hasMoreTerms()) {
        termids.add(r.nextTerm());
        int[] p = r.getPositions();
        positions.add(new TermPositions(p, r.getTf()));

        for(int j = 0; j < p.length; j++) {
          if(p[j] > cnt) {
            cnt = p[j];
          }
        }
      }

      int[] data = new int[termids.size()];
      for(int i = 0; i < termids.size(); i++) {
        data[i] = termids.get(i);
      }

      return DocumentVectorMiniInvertedIndex.newInstance(data, positions, cnt);
    }

    int cnt = 0;
    while(r.hasMoreTerms()) {
      r.nextTerm();
      int[] p = r.getPositions();
      for(int j = 0; j < p.length; j++) {
        if(p[j] > cnt) {
          cnt = p[j];
        }
      }
    }

    r = document.getReader();
    int[] data = new int[cnt];
    while(r.hasMoreTerms()) {
      int id = r.nextTerm();
      int[] p = r.getPositions();
      for(int j = 0; j < p.length; j++) {
        data[p[j] - 1] = id;
      }
    }

    if(documentVectorClass.equals(DocumentVectorHashedArray.class.getName())) {
      return DocumentVectorHashedArray.newInstance(data);
    } else if(documentVectorClass.equals(DocumentVectorPForDeltaArray.class.getName())) {
      return DocumentVectorPForDeltaArray.newInstance(data);
    } else if(documentVectorClass.equals(DocumentVectorVIntArray.class.getName())) {
      return DocumentVectorVIntArray.newInstance(data);
    } else {
      throw new ClassNotFoundException("DocumentVector " + documentVectorClass + " class not found!");
    }
  }

  /**
   * Reads an instance of DocumentVector from input
   *
   * @param input DataInput
   * @param documentVectorClass DocumentVector class
   * @return DocumentVector object
   */
  public static DocumentVector readInstance(String documentVectorClass, DataInput input) throws Exception {
    if(documentVectorClass.equals(DocumentVectorMiniInvertedIndex.class.getName())) {
      return DocumentVectorMiniInvertedIndex.readInstance(input);
    } else if(documentVectorClass.equals(DocumentVectorHashedArray.class.getName())) {
      return DocumentVectorHashedArray.readInstance(input);
    } else if(documentVectorClass.equals(DocumentVectorPForDeltaArray.class.getName())) {
      return DocumentVectorPForDeltaArray.readInstance(input);
    } else if(documentVectorClass.equals(DocumentVectorVIntArray.class.getName())) {
      return DocumentVectorVIntArray.readInstance(input);
    } else {
      throw new ClassNotFoundException("DocumentVector " + documentVectorClass + " class not found!");
    }
  }
}
