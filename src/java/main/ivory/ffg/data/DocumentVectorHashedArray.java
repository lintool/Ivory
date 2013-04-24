package ivory.ffg.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.kamikaze.pfordelta.PForDelta;

/**
 * Document vector representation using the hashing technique described
 * in \cite{sigir2011 submission}.
 *
 * @author Nima Asadi
 */

public class DocumentVectorHashedArray implements DocumentVector {
  private static final int[] CAPACITY = new int[]{256, 512, 1024, 2048, 4096, 8192, 16384, 32768};
  private static final int[] SHIFT = new int[]{8, 9, 10, 11, 12, 13, 14, 15};
  private static final int[] MASK = new int[]{0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF, 0x3FFF, 0x7FFF};

  /**
   * \theta_{Collisions}
   */
  private static final int MAX_COLLISIONS = 20;

  private int[] exceptionBlock; //Block that contains the exceptions
  private int[] compBlock; //Compressed document vector
  private int documentLength; //Length of the original document
  private byte exceptionLength; //Length of the exception table
  private byte mask; //\omega_m
  private byte hashIndex; //MASK[hashIndex] is \omega_h

  @Override public void write(DataOutput output) throws IOException {
    Preconditions.checkNotNull(output);

    output.writeByte(exceptionLength);
    if(exceptionLength > 0) {
      output.writeInt(exceptionBlock.length);
      for(int i = 0; i < exceptionBlock.length; i++) {
        output.writeInt(exceptionBlock[i]);
      }
    }

    output.writeInt(documentLength);
    output.writeInt(compBlock.length);
    for(int i = 0; i < compBlock.length; i++) {
      output.writeInt(compBlock[i]);
    }

    output.writeByte(mask);
    output.writeByte(hashIndex);
  }

  @Override public void readFields(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    exceptionLength = input.readByte();
    if(exceptionLength > 0) {
      exceptionBlock = new int[input.readInt()];
      for(int i = 0; i < exceptionBlock.length; i++) {
        exceptionBlock[i] = input.readInt();
      }
    }

    documentLength = input.readInt();
    compBlock = new int[input.readInt()];
    for(int i = 0; i < compBlock.length; i++) {
      compBlock[i] = input.readInt();
    }

    mask = input.readByte();
    hashIndex = input.readByte();
  }

  /**
   * Reads and returns a document vector from input
   *
   * @param input DataInput
   * @return A document vector
   */
  public static DocumentVectorHashedArray readInstance(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    DocumentVectorHashedArray document = new DocumentVectorHashedArray();
    document.readFields(input);
    return document;
  }

  private DocumentVectorHashedArray() {
  }

  // This hash function can be replaced by any integer hash function.
  private static int hash(int key, int hashIndex) {
    //    Preconditions.checkArgument(hashIndex >= 0 && hashIndex < SHIFT.length);
    return ((key >>> SHIFT[hashIndex]) ^ (key & MASK[hashIndex])) & MASK[hashIndex];
  }

  //Applies \omega_m
  private int lowMask(int key) {
    return (key & (0xFFFFFFFF>>>mask));
  }

  @Override public int[] transformTerms(int[] terms) {
    Preconditions.checkNotNull(terms);

    int[] hashedTerms = new int[terms.length];

    if(exceptionLength == 0) {
      if(hashIndex < CAPACITY.length) {
        // Case 2(a)
        for(int i = 0; i < terms.length; i++) {
          hashedTerms[i] = hash(lowMask(terms[i]), hashIndex);
        }
        return hashedTerms;
      } else {
        // Case 1 and 3
        for(int i = 0; i < terms.length; i++) {
          hashedTerms[i] = lowMask(terms[i]);
        }
        return hashedTerms;
      }
    } else {
      // Case 2(b)
      int[] exception = new int[exceptionLength];
      PForDelta.decompressOneBlock(exception, exceptionBlock, exceptionLength);

      for(int i = 0; i < terms.length; i++) {
        hashedTerms[i] = lowMask(terms[i]);
      }

      boolean inserted;
      for(int t = 0; t < hashedTerms.length; t++) {
        inserted = false;
        for(int i = 0; i < exception.length; i += 2) {
          if(exception[i] == hashedTerms[t]) {
            hashedTerms[t] = exception[i + 1];
            inserted = true;
            break;
          }
        }

        if(!inserted) {
          hashedTerms[t] = hash(hashedTerms[t], hashIndex);
        }
      }

      return hashedTerms;
    }
  }

  @Override public int getDocumentLength() {
    return documentLength;
  }

  @Override public int[] decompressDocument() throws IOException {
    int[] decomp = new int[documentLength];
    PForDelta.decompressOneBlock(decomp, compBlock, documentLength);
    return decomp;
  }

  @Override public int[][] decompressPositions(int[] terms) throws IOException {
    Preconditions.checkNotNull(terms);
    return DocumentVectorUtility.getPositions(decompressDocument(), transformTerms(terms));
  }

  /**
   * Creates an instance of this class by transforming the given
   * document vector into the new (hashed) space.
   *
   * @param document Input document vector.
   * @return Transformed document vector and the hash function.
   */
  public static DocumentVectorHashedArray newInstance(int[] document) {
    Preconditions.checkNotNull(document);

    Set<Integer> termids = Sets.newHashSet();
    Set<Integer> origTermids = Sets.newHashSet();

    // Construct the set of unique terms within the input document.
    for(int i = 0; i < document.length; i++) {
      origTermids.add(document[i]);
    }

    // Search for \omega_m
    int msb = 31;
    for(int i = 31; i >= 0; i--) {
      int mask = ~(1<<i);
      int[] tempDocument = new int[document.length];
      termids.clear();
      for(int j = 0; j < document.length; j++) {
        int d = document[j];
        tempDocument[j] = (d & mask);
        termids.add(tempDocument[j]);
      }

      int collision = origTermids.size() - termids.size();
      if(collision == 0) {
        for(int j = 0; j < document.length; j++) {
          document[j] = tempDocument[j];
        }
        msb = i;
      } else {
        break;
      }
    }

    // Case 1: if \omega_m <= \theta_\omega
    if(msb <= SHIFT[0]) {
      DocumentVectorHashedArray compDocument = new DocumentVectorHashedArray();
      compDocument.compBlock = PForDelta.compressOneBlockOpt(document, document.length);
      compDocument.documentLength = document.length;
      compDocument.exceptionLength = (byte) 0;
      compDocument.hashIndex = (byte) CAPACITY.length;
      compDocument.mask = (byte) (32 - msb);
      return compDocument;
    }

    termids.clear();
    for(int i = 0; i < document.length; i++) {
      termids.add(document[i]);
    }

    int[] uniqueTerms = new int[termids.size()];
    int uniqueIndex = 0;
    for(int term: termids) {
      uniqueTerms[uniqueIndex++] = term;
    }
    Arrays.sort(uniqueTerms);
    int hashIndex = 0;

    for(int i = 0; i < CAPACITY.length; i++) {
      if(uniqueTerms.length < CAPACITY[i] - 1) { //excluding zero
        hashIndex = i;
        break;
      }
    }

    List<Integer> collisions = Lists.newArrayList();
    List<Integer> code = Lists.newArrayList();

    // Search for \omega_h such that |Collisions| < \theta_collisions
    do {
      termids.clear();
      collisions.clear();
      for(int i = uniqueTerms.length - 1; i >= 0; i--) {
        int h = hash(uniqueTerms[i], hashIndex);
        if(!termids.contains(h)) {
          termids.add(h);
        } else {
          collisions.add(uniqueTerms[i]);
        }
      }
      hashIndex++;
    } while(collisions.size() > MAX_COLLISIONS && hashIndex < CAPACITY.length);
    hashIndex--;

    if(collisions.size() == 0) {
      // Case 2(a): |Collisions| == 0
      for(int i = 0; i < document.length; i++) {
        document[i] = hash(document[i], hashIndex);
      }

      DocumentVectorHashedArray compDocument = new DocumentVectorHashedArray();
      compDocument.compBlock = PForDelta.compressOneBlockOpt(document, document.length);
      compDocument.documentLength = document.length;
      compDocument.exceptionLength = (byte) 0;
      compDocument.hashIndex = (byte) hashIndex;
      compDocument.mask = (byte) (32 - msb);
      return compDocument;
    } else if(collisions.size() > MAX_COLLISIONS) {
      // Case 3: |Collisions| > \theta_collisions for all possible \omega_h
      DocumentVectorHashedArray compDocument = new DocumentVectorHashedArray();
      compDocument.compBlock = PForDelta.compressOneBlockOpt(document, document.length);
      compDocument.documentLength = document.length;
      compDocument.exceptionLength = (byte) 0;
      compDocument.hashIndex = (byte) CAPACITY.length;
      compDocument.mask = (byte) (32 - msb);
      return compDocument;
    }

    // Begin Case 2(b): Find exceptions and manuallay construct a hash table for them
    int id = 1;
    for(int i = 0; i < collisions.size(); i++) {
      while(termids.contains(id)) {
        id++;
      }
      code.add(id);
      termids.add(id);
    }

    // Encode exceptions into an array of pairs <exception, hash>
    int[] exception = new int[collisions.size() * 2];
    int pos = 0;
    for(int i = 0; i < collisions.size(); i++) {
      exception[pos++] = collisions.get(i);
      exception[pos++] = code.get(i);
    }

    // Encode the document using the manual table as well as the integer hash function,
    // with parameters \omega_h and \omega_m
    for(int i = 0; i < document.length; i++) {
      boolean inserted = false;
      for(int j = 0; j < exception.length; j += 2) {
        if(exception[j] == document[i]) {
          document[i] = exception[j + 1];
          inserted = true;
          break;
        }
      }
      if(!inserted) {
        document[i] = hash(document[i], hashIndex);
      }
    }

    DocumentVectorHashedArray compDocument = new DocumentVectorHashedArray();
    compDocument.compBlock = PForDelta.compressOneBlockOpt(document, document.length);
    compDocument.exceptionBlock = PForDelta.compressOneBlockOpt(exception, exception.length);
    compDocument.documentLength = document.length;
    compDocument.exceptionLength = (byte) exception.length;
    compDocument.hashIndex = (byte) hashIndex;
    compDocument.mask = (byte) (32 - msb);
    return compDocument;
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof DocumentVectorHashedArray);

    DocumentVectorHashedArray other = (DocumentVectorHashedArray) o;
    if(this.hashIndex != other.hashIndex || this.mask != other.mask ||
       this.exceptionLength != other.exceptionLength || this.documentLength != other.documentLength ||
       this.compBlock.length !=other.compBlock.length ||
       this.exceptionBlock.length != other.exceptionBlock.length) {
      return false;
    }
    for(int i = 0; i < compBlock.length; i++) {
      if(this.compBlock[i] != other.compBlock[i]) {
        return false;
      }
    }
    for(int i = 0; i < exceptionBlock.length; i++) {
      if(this.exceptionBlock[i] != other.exceptionBlock[i]) {
        return false;
      }
    }
    return true;
  }
}
