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

package ivory.core.data.dictionary;

import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class FrontCodedDictionary implements Writable, LexicographicallySortedDictionary {
  private static final Logger LOG = Logger.getLogger(FrontCodedDictionary.class);

  private FrontCodedStringList stringList;
  private ShiftAddXorSignedStringMap dictionary;

  public FrontCodedDictionary() {}

  @Override
  public int getId(String term) {
    return (int) dictionary.getLong(term);
  }

  @Override
  public String getTerm(int id) {
    return stringList.get(id).toString();
  }

  @Override
  public int size() {
    return stringList.size();
  }

  @Override
  public Iterator<String> iterator() {
    return null;
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    byte[] bytes;
    ObjectInputStream obj;

    bytes = new byte[in.readInt()];
    LOG.info("Loading front-coded list of terms: " + bytes.length + " bytes.");
    in.readFully(bytes);
    obj = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      stringList = (FrontCodedStringList) obj.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    obj.close();

    bytes = new byte[in.readInt()];
    LOG.info("Loading dictionary hash: " + bytes.length + " bytes.");
    in.readFully(bytes);
    obj = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      dictionary = (ShiftAddXorSignedStringMap) obj.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    obj.close();
    LOG.info("Finished loading.");
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  /**
   * Simple demo program for looking up terms and term ids.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [index-path]");
      System.exit(-1);
    }

    String indexPath = args[0];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    FrontCodedDictionary dictionary = new FrontCodedDictionary();
    dictionary.readFields(fs.open(new Path(indexPath)));

    int nTerms = dictionary.size();
    System.out.println("nTerms: " + nTerms);

    System.out.println(" \"term word\" to lookup termid; \"termid 234\" to lookup term");
    String cmd = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("lookup > ");
    while ((cmd = stdin.readLine()) != null) {

      String[] tokens = cmd.split("\\s+");

      if (tokens.length != 2) {
        System.out.println("Error: unrecognized command!");
        System.out.print("lookup > ");

        continue;
      }

      if (tokens[0].equals("termid")) {
        int termid;
        try {
          termid = Integer.parseInt(tokens[1]);
        } catch (Exception e) {
          System.out.println("Error: invalid termid!");
          System.out.print("lookup > ");

          continue;
        }

        System.out.println("termid=" + termid + ", term=" + dictionary.getTerm(termid));
      } else if (tokens[0].equals("term")) {
        String term = tokens[1];

        System.out.println("term=" + term + ", termid=" + dictionary.getId(term));
      } else {
        System.out.println("Error: unrecognized command!");
        System.out.print("lookup > ");
        continue;
      }

      System.out.print("lookup > ");
    }
  }
}
