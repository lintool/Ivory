/**
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package ivory.ptc.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * Data structure that holds a target document along with
 * a list of (source) documents that point to the target
 * document. In addition, a weight field is associated with
 * the target document if necessary.
 *
 * The iterator method iterates over the source documents
 * that point to the target document.
 *
 * @author Nima Asadi
 */

public class AnchorTextTarget implements WritableComparable<AnchorTextTarget>, Iterable<Integer> {
  private ArrayListOfIntsWritable sources;
  private int target;
  private float weight;

  /**
   * Constructs a target with an empty source list.
   */
  public AnchorTextTarget() {
    sources = new ArrayListOfIntsWritable();
  }

  /**
   * Constructs a target by cloning an existing target.
   *
   * @param at Existing target object.
   */
  public AnchorTextTarget(AnchorTextTarget at) {
    this();
    target = at.target;
    weight = at.weight;
    addSources(at.sources);
  }

  /**
   * Constructs a target object from scratch.
   *
   * @param trgt Document id of the target document.
   * @param srcs Source documents that point to the target document.
   * @param wt Weight for the target document.
   */
  public AnchorTextTarget(int trgt, ArrayListOfIntsWritable srcs, float wt) {
    this();
    target = trgt;
    weight = wt;
    addSources(srcs);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sources.clear();
    sources.readFields(in);
    target = in.readInt();
    weight = in.readFloat();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    sources.write(out);
    out.writeInt(target);
    out.writeFloat(weight);
  }

  /**
   * Adds source documents to the list of source documents that
   * point to the current target document.
   *
   * @param sources Source documents to be added.
   */
  public void addSources(ArrayListOfIntsWritable sources) {
    for (int i : sources) {
      this.sources.add(i);
    }
  }

  /**
   * Resets the source list to the given list. Please note
   * that this method removes the existing list with the given
   * parameter; i.e., the source list will be reset and re-initialized.
   *
   * @param sources Source documents to be initialized with.
   */
  public void setSources(ArrayListOfIntsWritable sources) {
    this.sources = sources;
  }

  /**
   * Retrieves the list of source documents.
   *
   * @return list of source documents.
   */
  public ArrayListOfIntsWritable getSources() {
    return sources;
  }

  /**
   * Sets the target document.
   *
   * @param target New target document id.
   */
  public void setTarget(int target) {
    this.target = target;
  }

  /**
   * Retrieves the target document id.
   *
   * @return document id of the target document.
   */
  public int getTarget() {
    return target;
  }

  /**
   * Sets the weight for the current target document.
   *
   * @param wt Weight
   */
  public void setWeight(float wt) {
    this.weight = wt;
  }

  /**
   * @return weight of the target document.
   */
  public float getWeight() {
    return weight;
  }

  @Override
  public int compareTo(AnchorTextTarget at) {
    if (weight < at.weight) {
      return 1;
    } else if (weight > at.weight) {
      return -1;
    } else if (target < at.target) {
      return 1;
    } else if (target > at.target) {
      return -1;
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    AnchorTextTarget other = (AnchorTextTarget) o;
    if (other.target != this.target) {
      return false;
    }
    return this.weight == other.weight;
  }

  @Override
  public int hashCode() {
    return target;
  }

  @Override
  public String toString() {
    return "[ to=" + getTarget() + ", from=" + getSources() + ", weight="
        + getWeight() + " ]";
  }

  @Override
  public Iterator<Integer> iterator() {
    return sources.iterator();
  }
}
