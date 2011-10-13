/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.core.data.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A posting, consisting of a docno and a term frequency.
 *
 * @author Jimmy Lin
 */
public class Posting implements Writable {
  private int docno;
  private short tf;

  /**
   * Creates a new empty posting.
   */
  public Posting() {
    super();
  }

  /**
   * Creates a new posting with a specific docno and term frequency.
   */
  public Posting(int docno, short tf) {
    super();
    this.docno = docno;
    this.tf = tf;
  }

  /**
   * Returns the docno of this posting.
   */
  public int getDocno() {
    return docno;
  }

  /**
   * Sets the docno of this posting.
   */
  public void setDocno(int docno) {
    this.docno = docno;
  }

  /**
   * Returns the term frequency of this posting.
   */
  public short getTf() {
    return tf;
  }

  /**
   * Sets the term frequency of this posting.
   */
  public void setTf(short score) {
    this.tf = score;
  }

  /**
   * Deserializes this posting.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    docno = in.readInt();
    tf = in.readShort();
  }

  /**
   * Serializes this posting.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(docno);
    out.writeShort(tf);
  }

  /**
   * Clones this posting.
   */
  @Override
  public Posting clone() {
    return new Posting(this.getDocno(), this.getTf());
  }

  /**
   * Generates a human-readable String representation of this object.
   */
  @Override
  public String toString() {
    return "(" + docno + ", " + tf + ")";
  }
}
