package ivory.bloomir.data;

import org.apache.hadoop.io.Writable;

/**
 * An abstract signature.
 *
 * @author Nima Asadi
 */
public abstract class Signature implements  Writable {
  /**
   * Adds a key to this signature.
   *
   * @param key Key to be added.
   */
  public abstract void add(int key);

  /**
   * Queries the current signature and performs a membership test.
   *
   * @param key Key to be checked.
   * @return A Boolean value indicating whether the key exists in the
   * signature or not.
   */
  public abstract boolean membershipTest(int key);
}
