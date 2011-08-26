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

import ivory.core.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

/**
 * Array-based implementation of {@link DfTable}. Binary search is used for lookup.
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class DfTableArray implements DfTable {
  private final int numTerms;
  private final int[] dfs;

  private int maxDf = 0;
  private int maxDfTerm;

  private int numSingletonTerms = 0;

  /**
   * Creates a {@code DfTableArray} object.
   *
   * @param file collection frequency data file
   * @throws IOException
   */
  public DfTableArray(Path file) throws IOException {
    this(file, FileSystem.get(new Configuration()));
  }

  /**
   * Creates a {@code DfTableArray} object.
   *
   * @param file collection frequency data file path
   * @param fs FileSystem to read from
   * @throws IOException
   */
  public DfTableArray(Path file, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(fs);

    FSDataInputStream in = fs.open(file);

    this.numTerms = in.readInt();

    dfs = new int[numTerms];

    for (int i = 0; i < numTerms; i++) {
      int df = WritableUtils.readVInt(in);

      dfs[i] = df;
      if (df > maxDf) {
        maxDf = df;
        maxDfTerm = i + 1;
      }

      if (df == 1) {
        numSingletonTerms++;
      }
    }

    in.close();
  }

  @Override
  public int getDf(int term) {
    return dfs[term - 1];
  }

  @Override
  public int getVocabularySize() {
    return numTerms;
  }

  @Override
  public int getMaxDf() {
    return maxDf;
  }

  @Override
  public int getMaxDfTerm() {
    return maxDfTerm;
  }

  @Override
  public int getNumOfSingletonTerms() {
    return numSingletonTerms;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [index-path]");
      System.exit(-1);
    }

    String indexPath = args[0];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

    DfTableArray dfs = new DfTableArray(new Path(env.getDfByIntData()), fs);

    String input = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("lookup > ");
    while ((input = stdin.readLine()) != null) {
      int termid = Integer.parseInt(input);
      System.out.println("termid=" + termid + ", df=" + dfs.getDf(termid));

      System.out.print("lookup > ");
    }
  }
}
