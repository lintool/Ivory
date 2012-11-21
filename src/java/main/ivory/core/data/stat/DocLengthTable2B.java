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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.debug.MemoryUsageUtils;

/**
 * <p>
 * Object that keeps track of the length of each document in the collection as a two-byte integer
 * (shorts). Document lengths are measured in number of terms.
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
 * <li>Exactly <i>n</i> shorts, one for each document in the collection.</li>
 * </ul>
 *
 * <p>
 * Since the documents are numbered sequentially starting at <i>d</i> + 1, each short corresponds
 * unambiguously to a particular document.
 * </p>
 *
 * @author Jimmy Lin
 */
public class DocLengthTable2B implements DocLengthTable {
  static final Logger LOG = Logger.getLogger(DocLengthTable2B.class);

  private final short[] lengths;
  private final int docnoOffset;

  private int docCount;
  private float avgDocLength;

  /**
   * Creates a new {@code DocLengthTable2B}.
   *
   * @param file document length data file
   * @throws IOException
   */
  public DocLengthTable2B(Path file) throws IOException {
    this(file, FileSystem.get(new Configuration()));
  }

  /**
   * Creates a new {@code DocLengthTable2B}.
   *
   * @param file document length data file
   * @param fs FileSystem to read from
   * @throws IOException
   */
  public DocLengthTable2B(Path file, FileSystem fs) throws IOException {
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
    lengths = new short[sz];

    // Read each doc length.
    for (int i = 1; i < sz; i++) {
      int l = in.readInt();
      docLengthSum += l;

      lengths[i] = l > (Short.MAX_VALUE - Short.MIN_VALUE) ? Short.MAX_VALUE
          : (short) (l + Short.MIN_VALUE);
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
    return lengths[docno - docnoOffset] - Short.MIN_VALUE;
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

  // Main program for interactively querying document lengths.
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [doc-length-data]");
      System.exit(-1);
    }

    long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

    DocLengthTable2B lengths = new DocLengthTable2B(new Path(args[0]),
        FileSystem.get(new Configuration()));
    long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

    System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");
    System.out.println("Average doc length: " + lengths.getAvgDocLength());
    System.out.println("Docno offset: " + lengths.getDocnoOffset());

    String docno = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Look up doclength for docno> ");
    while ((docno = stdin.readLine()) != null) {
      System.out.println(lengths.getDocLength(Integer.parseInt(docno)));
      System.out.print("Look up doclength for docno> ");
    }
  }
}
