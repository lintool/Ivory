package ivory.ffg.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;

import com.kamikaze.pfordelta.PForDelta;

/**
 * Document vector representation: Flat array compressed
 * with PForDelta.
 *
 * @author Nima Asadi
 */

public class DocumentVectorPForDeltaArray implements DocumentVector {
  private int documentLength; //Length of the document
  private int[] document; //Compressed document vector

  @Override public void write(DataOutput output) throws IOException {
    Preconditions.checkNotNull(output);

    output.writeInt(documentLength);
    output.writeInt(document.length);
    for(int i = 0; i < document.length; i++) {
      output.writeInt(document[i]);
    }
  }

  @Override public void readFields(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    documentLength = input.readInt();
    document = new int[input.readInt()];
    for(int i = 0; i < document.length; i++) {
      document[i] = input.readInt();
    }
  }

  /**
   * Reads and returns an instance of this class from input
   *
   * @param input DataInput
   * @return A document vector
   */
  public static DocumentVectorPForDeltaArray readInstance(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);
    DocumentVectorPForDeltaArray document = new DocumentVectorPForDeltaArray();
    document.readFields(input);
    return document;
  }

  private DocumentVectorPForDeltaArray() {
  }

  @Override public int getDocumentLength() {
    return documentLength;
  }

  @Override public int[] decompressDocument() throws IOException {
    int[] decomp = new int[documentLength];
    PForDelta.decompressOneBlock(decomp, document, decomp.length);
    return decomp;
  }

  @Override public int[][] decompressPositions(int[] terms) throws IOException {
    Preconditions.checkNotNull(terms);
    return DocumentVectorUtility.getPositions(decompressDocument(), terms);
  }

  @Override public int[] transformTerms(int[] terms) {
    return terms;
  }

  /**
   * Constructs a new document vector and compresses it using PForDelta
   *
   * @param document Flat array representation of a document
   * @return Compressed document vector
   */
  public static DocumentVectorPForDeltaArray newInstance(int[] document) {
    Preconditions.checkNotNull(document);

    DocumentVectorPForDeltaArray doc = new DocumentVectorPForDeltaArray();
    doc.documentLength = document.length;
    doc.document = PForDelta.compressOneBlockOpt(document, document.length);
    return doc;
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof DocumentVectorPForDeltaArray);

    DocumentVectorPForDeltaArray other = (DocumentVectorPForDeltaArray) o;
    if(this.documentLength != other.documentLength ||
       this.document.length != other.document.length) {
      return false;
    }
    for(int i = 0; i < document.length; i++) {
      if(this.document[i] != other.document[i]) {
        return false;
      }
    }
    return true;
  }
}
