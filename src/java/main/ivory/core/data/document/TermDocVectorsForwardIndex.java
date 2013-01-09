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

package ivory.core.data.document;

import ivory.core.RetrievalEnvironment;
import ivory.core.preprocess.BuildTermDocVectorsForwardIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.debug.MemoryUsageUtils;

/**
 * Object providing an index into one or more {@code SequenceFile}s
 * containing {@link TermDocVector}s, providing random access to the document
 * vectors.
 *
 * @see BuildTermDocVectorsForwardIndex
 *
 * @author Jimmy Lin
 */
public class TermDocVectorsForwardIndex {
  private static final Logger LOG = Logger.getLogger(TermDocVectorsForwardIndex.class);
  private static final NumberFormat FORMAT = new DecimalFormat("00000");

  // This is 10^15 (i.e., an exabyte). We're assuming that each individual file is smaller than
  // this value, which seems safe, at least for a while... :)
  public static final long BigNumber = 1000000000000000L;

  private final Configuration conf;
  private final long[] positions;
  private final String path;
  private final int docnoOffset;
  private final int collectionDocumentCount;

  /**
   * Creates a {code TermDocVectorsIndex} object.
   *
   * @param indexPath location of the index file
   * @param fs handle to the FileSystem
   * @throws IOException
   */
  public TermDocVectorsForwardIndex(String indexPath, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(indexPath);
    Preconditions.checkNotNull(fs);

    conf = fs.getConf();

    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    path = env.getTermDocVectorsDirectory();

    FSDataInputStream posInput = fs.open(new Path(env.getTermDocVectorsForwardIndex()));

    docnoOffset = posInput.readInt();
    collectionDocumentCount = posInput.readInt();

    positions = new long[collectionDocumentCount];
    for (int i = 0; i < collectionDocumentCount; i++) {
      positions[i] = posInput.readLong();
    }
    posInput.close();
  }

  /**
   * Returns the document vector given a docno.
   */
  public TermDocVector getDocVector(int docno) throws IOException {
    // TODO: This method re-opens the SequenceFile on every access.
    // Would be more efficient to cache the file handles.
    if (docno > collectionDocumentCount || docno < 1) {
      return null;
    }

    long pos = positions[docno - docnoOffset - 1];

    int fileNo = (int) (pos / BigNumber);
    pos = pos % BigNumber;

    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(new Path(path + "/part-m-" + FORMAT.format(fileNo))));
    } catch (IOException e) {
      // Try alternative naming scheme for output of old API.
      reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(new Path(path + "/part-" + FORMAT.format(fileNo))));
    }

    IntWritable key = new IntWritable();
    TermDocVector value;

    try {
      value = (TermDocVector) reader.getValueClass().newInstance();
    } catch (Exception e) {
      reader.close();
      throw new RuntimeException("Unable to instantiate key/value pair!");
    }

    reader.seek(pos);
    reader.next(key, value);

    if (key.get() != docno) {
      LOG.error(String.format("Unable to doc vector for docno %d: found docno %d instead.",
          docno, key));
      reader.close();
      return null;
    }

    reader.close();
    return value;
  }

  /**
   * Simple test program.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [indexPath]");
      System.exit(-1);
    }

    long startingMemoryUse = MemoryUsageUtils.getUsedMemory();
    Configuration conf = new Configuration();
    TermDocVectorsForwardIndex index = new TermDocVectorsForwardIndex(args[0], FileSystem.get(conf));
    long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

    System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

    String term = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Look up postings of doc > ");
    while ((term = stdin.readLine()) != null) {
      int docno = Integer.parseInt(term);
      System.out.println(docno + ": " + index.getDocVector(docno));
      System.out.print("Look up postings of doc > ");
    }
  }
}
