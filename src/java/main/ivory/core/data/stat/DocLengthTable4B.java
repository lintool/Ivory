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

package ivory.core.data.stat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * <p>
 * Object that keeps track of the length of each document in the collection as a four-byte integers
 * (ints). Document lengths are measured in number of terms.
 * </p>
 *
 * <p>
 * Document length data is stored in a serialized data file, in the following format:
 * </p>
 *
 * <ul>
 * <li>An integer that specifies the docno offset <i>d</i>, where <i>d</i> + 1 is the first docno in
 * the collection.</li>
 * <li>An integer that specifies the number of documents in the collection <i>n</i>.</li>
 * <li>Exactly <i>n</i> ints, one for each document in the collection.</li>
 * </ul>
 *
 * <p>
 * Since the documents are numbered sequentially starting at <i>d</i> + 1, each short corresponds
 * unambiguously to a particular document.
 * </p>
 *
 * @author Jimmy Lin
 */
public class DocLengthTable4B implements DocLengthTable {
  static final Logger LOG = Logger.getLogger(DocLengthTable4B.class);

  private final int[] lengths;
  private final int docnoOffset;

  private int docCount;
  private float avgDocLength;

  /**
   * Creates a new {@code DocLengthTable4B}.
   *
   * @param file document length data file
   * @throws IOException
   */
  public DocLengthTable4B(Path file) throws IOException {
    this(file, FileSystem.get(new Configuration()));
  }

  /**
   * Creates a new {@code DocLengthTable4B}.
   *
   * @param file document length data file
   * @param fs FileSystem to read from
   * @throws IOException
   */
  public DocLengthTable4B(Path file, FileSystem fs) throws IOException {
    long docLengthSum = 0;
    docCount = 0;

    FSDataInputStream in = fs.open(file);

    // The docno offset.
    docnoOffset = in.readInt();

    // The size of the document collection.
    int sz = in.readInt() + 1;

    LOG.info("Docno offset: " + docnoOffset);
    LOG.info("Number of docs: " + (sz - 1));

    // Initialize an array to hold all the doc lengths.
    lengths = new int[sz];

    // Read each doc length.
    for (int i = 1; i < sz; i++) {
      int l = in.readInt();
      lengths[i] = l;
      docLengthSum += l;
      docCount++;

      if (i % 1000000 == 0) {
        LOG.info(i + " doclengths read");
      }
    }

    in.close();

    LOG.info("Total of " + docCount + " doclengths read");

    // Compute average doc length.
    avgDocLength = docLengthSum * 1.0f / docCount;
  }

  @Override
  public int getDocLength(int docno) {
    return lengths[docno - docnoOffset];
  }

  @Override
  public int getDocnoOffset() {
    return docnoOffset;
  }

  @Override
  public float getAvgDocLength() {
    return avgDocLength;
  }

  @Override
  public int getDocCount() {
    return docCount;
  }
}
