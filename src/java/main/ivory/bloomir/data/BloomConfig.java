package ivory.bloomir.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A configuration of parameters for an experiment.
 *
 * @author Nima Asadi
 */
public class BloomConfig implements Writable {
  /**
   * Filename where the configuration parameters are stored.
   */
  public static final String CONFIG_FILE = "conf";
  private int nbDocuments;
  private int nbTerms;
  private int nbHash;
  private int bitsPerElement;
  private int identityHashThreshold;

  private BloomConfig() {
  }

  /**
   * Creates a new configuration object.
   *
   * @param nbDocuments Number of documents in the  collection
   * @param nbTerms Number of unique terms in the collection
   * @param nbHash Number of Hash functions
   * @param bitsPerElement Number of bit positions per element
   */
  public BloomConfig(int nbDocuments, int nbTerms, int nbHash, int bitsPerElement) {
    this.nbDocuments = nbDocuments;
    this.nbTerms = nbTerms;
    this.nbHash = nbHash;
    this.bitsPerElement = bitsPerElement;
    this.identityHashThreshold = computeIdentityHashThreshold(nbDocuments, bitsPerElement);
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeInt(nbDocuments);
    out.writeInt(nbTerms);
    out.writeInt(nbHash);
    out.writeInt(bitsPerElement);
    out.writeInt(identityHashThreshold);
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.nbDocuments = in.readInt();
    this.nbTerms = in.readInt();
    this.nbHash = in.readInt();
    this.bitsPerElement = in.readInt();
    this.identityHashThreshold = in.readInt();
  }

  /**
   * Reads and returns an instance of the configuration object.
   *
   * @param in DataInput stream
   * @return A BloomConfig object containing all the necessary parameters to
   * run an experiment
   */
  public static BloomConfig readInstance(DataInput in) throws IOException {
    BloomConfig bloomConfig = new BloomConfig();
    bloomConfig.readFields(in);
    return bloomConfig;
  }

  /**
   * @return The number of documents in the collection.
   */
  public int getDocumentCount() {
    return nbDocuments;
  }

  /**
   * @return The number of hash functions.
   */
  public int getHashCount() {
    return nbHash;
  }

  /**
   * @return The number of unique terms in the collection.
   */
  public int getTermCount() {
    return nbTerms;
  }

  /**
   * @return The number of bit positions per element.
   */
  public int getBitsPerElement() {
    return bitsPerElement;
  }

  /**
   * @return The identity hash function threshold (\theta_I)
   */
  public int getIdentityHashThreshold() {
    return identityHashThreshold;
  }

  private int computeIdentityHashThreshold(int nbDocuments, int bitsPerElement) {
    return (nbDocuments / bitsPerElement);
  }
}
