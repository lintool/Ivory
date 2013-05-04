package ivory.ffg.data;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Abstract document vector.
 *
 * @author Nima Asadi
 */
public interface DocumentVector extends Writable {

  /**
   * Decompresses the document vector into a flat array representation
   *
   * @return Decompressed flat array representation of the document vector
   */
  public int[] decompressDocument() throws IOException;

  /**
   * Decompresses/constructs positions. This method applies the necessary
   * transformation functions tot he input query terms.
   *
   * @param terms Query terms
   * @return Position arrays for every term in the query
   */
  public int[][] decompressPositions(int[] terms) throws IOException;

  /**
   * Transforms query terms
   *
   * @param terms Query terms
   * @return Transformed query terms
   */
  public int[] transformTerms(int[] terms);

  /**
   * @return Length of this document
   */
  public int getDocumentLength();
}
