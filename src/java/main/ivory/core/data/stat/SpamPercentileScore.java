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

import com.google.common.base.Preconditions;

import edu.umd.cloud9.debug.MemoryUsageUtils;

public class SpamPercentileScore implements DocScoreTable {
  static final Logger LOG = Logger.getLogger(SpamPercentileScore.class);

  protected byte[] scores;
  protected int docs;
  protected int docnoOffset;

  public SpamPercentileScore() {}

  @Override
  public void initialize(String file, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(fs);

    FSDataInputStream in = fs.open(new Path(file));

    // Read the docno offset.
    docnoOffset = in.readInt();

    // Read the size of the document collection.
    int sz = in.readInt() + 1;

    LOG.info("Docno offset: " + docnoOffset);
    LOG.info("Number of docs: " + (sz - 1));

    // Initialize an array to hold all the doc scores.
    scores = new byte[sz];

    // Read each doc length.
    for (int i = 1; i < sz; i++) {
      scores[i] = in.readByte();
      docs++;

      if (i % 1000000 == 0) {
        LOG.info(i + " docscores read");
      }
    }

    in.close();

    LOG.info("Total of " + docs + " docscores read");
  }

  /**
   * Returns the length of a document. Note that this method does not do any
   * bounds checking: it is the responsibility of the caller to specify a
   * valid docno.
   */
  @Override
  public float getScore(int docno) {
    // Remember that docnos are numbered starting from one.
    // We add one to avoid computing log(0).
    return (float) Math.log(scores[docno - docnoOffset] + 1);
  }

  public byte getRawScore(int docno) {
    // Docnos are numbered starting from one.
    return scores[docno - docnoOffset];
  }

  @Override
  public int getDocnoOffset() {
    return docnoOffset;
  }

  @Override
  public int getDocCount() {
    return docs;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [index-path]");
      System.exit(-1);
    }

    long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

    DocScoreTable scores = new SpamPercentileScore();
    scores.initialize(args[0], FileSystem.get(new Configuration()));
    long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

    System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

    String docno = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Look up postings of term> ");
    while ((docno = stdin.readLine()) != null) {
      System.out.println(scores.getScore(Integer.parseInt(docno)));
      System.out.print("Look up postings of term> ");
    }
  }
}
