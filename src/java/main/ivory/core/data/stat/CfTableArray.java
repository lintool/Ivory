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
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

/**
 * Array-based implementation of {@code CfTable}. Binary search is used for lookup.
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class CfTableArray implements CfTable {
  private final int numTerms;
  private final long[] cfs;

  private long collectionSize;

  private long maxCf = 0;
  private int maxCfTerm;

  private int numSingletonTerms = 0;

  /**
   * Creates a {@code CfTableArray} object.
   *
   * @param file collection frequency data file
   * @throws IOException
   */
  public CfTableArray(Path file) throws IOException {
    this(file, FileSystem.get(new Configuration()));
  }

  /**
   * Creates a {@code CfTableArray} object.
   *
   * @param file collection frequency data file
   * @param fs FileSystem to read from
   * @throws IOException
   */
  public CfTableArray(Path file, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(fs);

    FSDataInputStream in = fs.open(file);

    this.numTerms = in.readInt();

    cfs = new long[numTerms];

    for (int i = 0; i < numTerms; i++) {
      long cf = WritableUtils.readVLong(in);

      cfs[i] = cf;
      collectionSize += cf;

      if (cf > maxCf) {
        maxCf = cf;
        maxCfTerm = i + 1;
      }

      if (cf == 1) {
        numSingletonTerms++;
      }
    }

    in.close();
  }

  @Override
  public long getCf(int term) {
    return cfs[term - 1];
  }

  @Override
  public long getCollectionSize() {
    return collectionSize;
  }

  @Override
  public int getVocabularySize() {
    return numTerms;
  }

  @Override
  public long getMaxCf() {
    return maxCf;
  }

  @Override
  public int getMaxCfTerm() {
    return maxCfTerm;
  }

  @Override
  public int getNumOfSingletonTerms() {
    return numSingletonTerms;
  }
}
