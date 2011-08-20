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

package ivory.core.data.dictionary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

/**
 * An implementation of {@link LexicographicallySortedDictionary} that encodes the dictionary
 * with prefix encoding.
 * 
 * @author Jimmy Lin
 */
public class PrefixEncodedLexicographicallySortedDictionary
    implements Writable, LexicographicallySortedDictionary {
  private static final Logger LOG =
      Logger.getLogger(PrefixEncodedLexicographicallySortedDictionary.class);

  public long totalOriginalBytes = 0;
  public long totalMemoryBytes = 0;

  private static final int DEFAULT_INITIAL_SIZE = 10000000;
  private String lastKey = "";

  private int curKeyIndex = 0;
  private byte[][] keys;

  private int window = 4;
  float resizeFactor = 1; // >0 and <=1

  public PrefixEncodedLexicographicallySortedDictionary() {
    keys = new byte[DEFAULT_INITIAL_SIZE][];
  }

  public PrefixEncodedLexicographicallySortedDictionary(int indexWindow) {
    keys = new byte[DEFAULT_INITIAL_SIZE][];
    window = indexWindow;
  }

  public PrefixEncodedLexicographicallySortedDictionary(int initialSize, int indexWindow) {
    keys = new byte[initialSize][];
    window = indexWindow;
  }

  public PrefixEncodedLexicographicallySortedDictionary(int initialSize, float resizeF,
      int indexWindow) {
    keys = new byte[initialSize][];
    window = indexWindow;
    resizeFactor = resizeF;
  }

  public void readFields(DataInput in) throws IOException {
    curKeyIndex = in.readInt();

    keys = new byte[curKeyIndex][];
    window = in.readInt();

    for (int i = 0; i < curKeyIndex; i++) {
      if (i % window != 0) { // not a root
        int suffix = in.readByte();
        if (suffix < 0) {
          suffix += 256;
        }
        keys[i] = new byte[suffix + 1];
        keys[i][0] = in.readByte();
        for (int j = 1; j < keys[i].length; j++) {
          keys[i][j] = in.readByte();
        }
      } else { // root
        int suffix = in.readByte();
        if (suffix < 0) {
          suffix += 256;
        }
        keys[i] = new byte[suffix];
        for (int j = 0; j < keys[i].length; j++) {
          keys[i][j] = in.readByte();
        }
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(curKeyIndex);
    out.writeInt(window);
    for (int i = 0; i < curKeyIndex; i++) {
      if (i % window != 0) { // not a root
        out.writeByte((byte) (keys[i].length - 1)); // suffix length
        out.writeByte(keys[i][0]); // prefix length
        for (int j = 1; j < keys[i].length; j++) {
          out.writeByte(keys[i][j]);
        }
      } else { // root
        out.writeByte((byte) (keys[i].length)); // suffix length
        for (int j = 0; j < keys[i].length; j++) {
          out.writeByte(keys[i][j]);
        }
      }
    }
  }

  public void store(String file, FileSystem fs) throws IOException {
    DataOutputStream out = new DataOutputStream(fs.create(new Path(file)));
    write(out);
    out.close();
  }

  public void add(String key) {
    totalOriginalBytes += key.getBytes().length;
    int prefix;

    if (curKeyIndex >= keys.length) {
      keys = resize2DimByteArray(keys, (int) (curKeyIndex * (1 + resizeFactor)));
    }

    byte[] byteArray = null;
    if (curKeyIndex % window == 0) {
      prefix = 0;
      byteArray = key.getBytes();
    } else {
      prefix = getPrefix(lastKey, key);
      byte[] suffix = key.substring(prefix).getBytes();
      byteArray = new byte[suffix.length + 1];
      byteArray[0] = (byte) prefix;
      for (int i = 0; i < suffix.length; i++) {
        byteArray[i + 1] = suffix[i];
      }
    }
    totalMemoryBytes += byteArray.length;
    keys[curKeyIndex] = byteArray;

    lastKey = key;
    curKeyIndex++;
  }

  public int getWindow() {
    return window;
  }

  @Override
  public int size() {
    return curKeyIndex;
  }

  @Override
  public String getTerm(int pos) {
    // Check bounds.
    if (pos >= curKeyIndex || pos < 0) {
      return null;
    }

    if (pos % window == 0) {
      return new String(keys[pos]);
    }
    int rootPos = (pos / window) * window;
    String s = getSuffixString(rootPos);
    for (int i = rootPos + 1; i <= pos; i++) {
      s = getString(i, s);
    }
    return s;
  }

  @Override
  public int getId(String key) {
    int l = 0, h = (curKeyIndex - 1) / window;
    String s = getTerm(h * window);
    int i = s.compareTo(key);
    if (i == 0) {
      return h * window;
    } else if (i < 0) {
      return getPosFromRoot(key, h * window);
    }

    while (l <= h) {
      int m = ((l + h) / 2);
      s = getTerm(m * window);
      i = s.compareTo(key);
      if (i == 0)
        return m * window;
      else if (i < 0) {
        int k = getPosFromRoot(key, m * window);
        if (k >= 0) {
          return k;
        }
        l = m + 1;
      } else {
        int k = getPosFromRoot(key, (m - 1) * window);
        if (k >= 0) {
          return k;
        }
        h = m - 1;
      }
    }
    return -1;
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      private int cur = 0;
      final private int end = curKeyIndex;

      @Override
      public boolean hasNext() {
        return cur < end;
      }

      @Override
      public String next() {
        return getTerm(cur++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public float getCompresssionRatio() {
    return (float) totalMemoryBytes / totalOriginalBytes;
  }

  private static byte[][] resize2DimByteArray(byte[][] input, int size) {
    if (size < 0) {
      return null;
    }

    if (size <= input.length) {
      return input;
    }

    byte[][] newArray = new byte[size][];
    System.arraycopy(input, 0, newArray, 0, input.length);
    return newArray;
  }

  private String getString(int pos, String previousKey) {
    if (pos % window == 0) {
      return new String(keys[pos]);
    } else {
      int prefix = getPrefixLength(pos);
      return previousKey.substring(0, prefix) + getSuffixString(pos);
    }
  }

  private String getSuffixString(int pos) {
    if (pos % window == 0) {
      return new String(keys[pos]);
    }
    byte[] b = new byte[keys[pos].length - 1];
    for (int i = 0; i < keys[pos].length - 1; i++) {
      b[i] = keys[pos][i + 1];
    }

    return new String(b);
  }

  private int getPosFromRoot(String key, int pos) {
    for (int i = pos + 1; i < pos + window && i < curKeyIndex; i++) {
      String s = getTerm(i);
      if (s.equals(key)) {
        return i;
      }
    }
    return -1;
  }

  private int getPrefixLength(int pos) {
    int prefix = keys[pos][0];
    if (prefix < 0) {
      prefix += 256;
    }
    return prefix;
  }

  public static int getPrefix(String s1, String s2) {
    int i = 0;
    for (i = 0; i < s1.length() && i < s2.length(); i++) {
      if (s1.charAt(i) != s2.charAt(i)) {
        return i;
      }
    }
    return i;
  }

  public static PrefixEncodedLexicographicallySortedDictionary load(String file, FileSystem fs)
      throws IOException {
    FSDataInputStream in;

    in = fs.open(new Path(file));
    PrefixEncodedLexicographicallySortedDictionary terms =
        new PrefixEncodedLexicographicallySortedDictionary();
    terms.readFields(in);

    return terms;
  }

  public static PrefixEncodedLexicographicallySortedDictionary loadFromPlainTextFile(String file,
      FileSystem fs, int window) throws IOException {
    LOG.info("Reading from " + file);
    LineReader reader = new LineReader(fs.open(new Path(file)));
    PrefixEncodedLexicographicallySortedDictionary terms =
        new PrefixEncodedLexicographicallySortedDictionary(window);

    int cnt = 0;
    Text t = new Text();
    while (reader.readLine(t) > 0) {
      String term = t.toString();

      // if it's a tab-separated file, we only want the first column
      if (term.contains("\t")) {
        term = term.split("\\t")[0];
      }

      terms.add(term);
      cnt++;

      if (cnt % 1000000 == 0) {
        LOG.info("read " + cnt + " lines");
      }
    }

    LOG.info("Finished reading from " + file);
    LOG.info("compression ratio: " + terms.getCompresssionRatio());

    return terms;
  }
}
