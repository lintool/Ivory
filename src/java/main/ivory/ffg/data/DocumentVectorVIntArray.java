package ivory.ffg.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.WritableUtils;

/**
 * Document vector representation: Flat array compressed using
 * Variable-Length Integers.
 *
 * @author Nima Asadi
 */

public class DocumentVectorVIntArray implements DocumentVector {
  private byte[] document; // Compressed document vector
  private int documentLength; // Length of the original document vector

  @Override public void write(DataOutput output) throws IOException {
    Preconditions.checkNotNull(output);

    output.writeInt(documentLength);
    output.writeInt(document.length);
    for(int i = 0; i < document.length; i++) {
      output.writeByte(document[i]);
    }
  }

  @Override public void readFields(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);

    documentLength = input.readInt();
    document = new byte[input.readInt()];
    for(int i = 0; i < document.length; i++) {
      document[i] = input.readByte();
    }
  }

  /**
   * Reads and returns an instance of this class from input
   *
   * @param input DataInput
   * @return A compressed document vector
   */
  public static DocumentVectorVIntArray readInstance(DataInput input) throws IOException {
    Preconditions.checkNotNull(input);
    DocumentVectorVIntArray document = new DocumentVectorVIntArray();
    document.readFields(input);
    return document;
  }

  private DocumentVectorVIntArray() {
  }

  @Override public int getDocumentLength() {
    return documentLength;
  }

  @Override public int[] decompressDocument() throws IOException {
    int[] decomp = new int[documentLength];
    ByteArrayInputStream byteStream = new ByteArrayInputStream(document);
    DataInputStream dataStream = new DataInputStream(byteStream);

    for(int i = 0; i < decomp.length; i++) {
      decomp[i] = WritableUtils.readVInt(dataStream);
    }

    dataStream.close();
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
   * Constructs a document vector and compresses it using Variable-Length Integer coding.
   *
   * @param data Flat array representation of a document
   * @return A document vector.
   */
  public static DocumentVectorVIntArray newInstance(int[] data) throws IOException {
    Preconditions.checkNotNull(data);

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(byteStream);

    for(int i = 0; i < data.length; i++) {
      WritableUtils.writeVInt(dataStream, data[i]);
    }

    dataStream.close();
    byte[] document = byteStream.toByteArray();

    DocumentVectorVIntArray instance = new DocumentVectorVIntArray();
    instance.document = document;
    instance.documentLength = data.length;

    return instance;
  }

  @Override public boolean equals(Object o) {
    Preconditions.checkNotNull(o);
    Preconditions.checkArgument(o instanceof DocumentVectorVIntArray);

    DocumentVectorVIntArray other = (DocumentVectorVIntArray) o;
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
