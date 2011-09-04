/*
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

package ivory.core.data.index;

import ivory.core.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.debug.MemoryUsageUtils;

public class IntPostingsForwardIndex {
  private static final Logger LOG = Logger.getLogger(IntPostingsForwardIndex.class);
  public static final long BIG_LONG_NUMBER = 1000000000;

  static {
    LOG.setLevel(Level.INFO);
  }

  long[] positions;
  String postingsPath;
  FileSystem fs;
  Configuration conf;
  String postingsType;

  public IntPostingsForwardIndex(String indexPath, FileSystem fs) throws IOException {
    this.fs = fs;
    this.conf = fs.getConf();
    postingsType = ivory.core.data.index.PostingsListDocSortedPositional.class.getCanonicalName();
    RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);
    postingsPath = env.getPostingsDirectory();

    FSDataInputStream posInput = fs.open(new Path(env
        .getPostingsIndexData()));

    int l = posInput.readInt();
    positions = new long[l];
    for (int i = 0; i < l; i++) {
      positions[i] = posInput.readLong();
      // sLogger.info(positions[i]);
    }
  }

  public PostingsList getPostingsList(int termid) throws IOException {
    // TODO: This method re-opens the SequenceFile on every access. Would be more efficient to cache
    // the file handles.

    // sLogger.info("getPostingsList("+termid+")");
    long pos = positions[termid - 1];

    int fileNo = (int) (pos / BIG_LONG_NUMBER);

    pos = pos % BIG_LONG_NUMBER;

    String fileNoStr = fileNo + "";
    String padd = "";
    for (int i = 5; i > fileNoStr.length(); i--)
      padd += "0";
    fileNoStr = padd + fileNoStr;

    // Open up the SequenceFile.
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs,
          new Path(postingsPath + "/part-" + fileNoStr), conf);
    } catch (IOException e) {
      // Try alternative naming scheme for output of new API.
      reader = new SequenceFile.Reader(fs,
          new Path(postingsPath + "/part-r-" + fileNoStr), conf);
    }

    IntWritable key = new IntWritable();
    PostingsList value = null;
    try {
      value = (PostingsList) Class.forName(reader.getValueClassName()).newInstance();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    reader.seek(pos);
    reader.next(key, value);

    if (key.get() != termid) {
      LOG.error("unable to fetch postings for term \"" + termid + "\": found key \""
          + key + "\" instead");
      return null;
    }

    reader.close();
    return value;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [indexPath]");
      System.exit(-1);
    }

    long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

    Configuration conf = new Configuration();

    IntPostingsForwardIndex index = new IntPostingsForwardIndex(args[0], FileSystem.get(conf));

    long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

    System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

    String term = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Look up postings of termid > ");
    while ((term = stdin.readLine()) != null) {
      int termid = Integer.parseInt(term);
      System.out.println(termid + ": " + index.getPostingsList(termid));
      System.out.print("Look up postings of termid > ");
    }
  }

}
