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

package ivory.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.FSLineReader;

public class PrefixEncodedTermSet implements Writable {

	/**
	 * logger
	 */
	private static final Logger LOGGER = Logger.getLogger(PrefixEncodedTermSet.class);
	static {
		LOGGER.setLevel(Level.WARN);
	}

	public long totalOriginalBytes = 0;
	public long totalMemoryBytes = 0;

	private static final int DEFAULT_INITIAL_SIZE = 10000000;
	private String lastKey = "";

	private int curKeyIndex = 0;
	private byte[][] keys;

	private int window = 4;
	float resizeFactor = 1; // >0 and <=1

	public int getWindow() {
		return window;
	}

	public int length() {
		return curKeyIndex;
	}

	public PrefixEncodedTermSet() {
		keys = new byte[DEFAULT_INITIAL_SIZE][];
	}

	public PrefixEncodedTermSet(int indexWindow) {
		keys = new byte[DEFAULT_INITIAL_SIZE][];
		window = indexWindow;
	}

	public PrefixEncodedTermSet(int initialSize, int indexWindow) {
		keys = new byte[initialSize][];
		window = indexWindow;
	}

	public PrefixEncodedTermSet(int initialSize, float resizeF, int indexWindow) {
		keys = new byte[initialSize][];
		window = indexWindow;
		resizeFactor = resizeF;
	}

	public void readFields(DataInput in) throws IOException {
		LOGGER.info("Loading prefix set ...");

		curKeyIndex = in.readInt();

		keys = new byte[curKeyIndex][];
		window = in.readInt();

		for (int i = 0; i < curKeyIndex; i++) {
			if (i % window != 0) { // not a root
				int suffix = in.readByte();
				if (suffix < 0)
					suffix += 256;
				keys[i] = new byte[suffix + 1];
				keys[i][0] = in.readByte();
				for (int j = 1; j < keys[i].length; j++)
					keys[i][j] = in.readByte();
			} else { // root
				int suffix = in.readByte();
				if (suffix < 0)
					suffix += 256;
				keys[i] = new byte[suffix];
				for (int j = 0; j < keys[i].length; j++)
					keys[i][j] = in.readByte();
			}
		}
		LOGGER.info("Loading done.");
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(curKeyIndex);
		out.writeInt(window);
		for (int i = 0; i < curKeyIndex; i++) {
			if (i % window != 0) { // not a root
				out.writeByte((byte) (keys[i].length - 1)); // suffix length
				out.writeByte(keys[i][0]); // prefix length
				for (int j = 1; j < keys[i].length; j++)
					out.writeByte(keys[i][j]);
			} else { // root
				out.writeByte((byte) (keys[i].length)); // suffix length
				for (int j = 0; j < keys[i].length; j++)
					out.writeByte(keys[i][j]);
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
			for (int i = 0; i < suffix.length; i++)
				byteArray[i + 1] = suffix[i];
		}
		totalMemoryBytes += byteArray.length;
		keys[curKeyIndex] = byteArray;

		lastKey = key;
		curKeyIndex++;
	}

	public String getKey(int pos) {
		if (pos % window == 0)
			return new String(keys[pos]);
		int rootPos = (pos / window) * window;
		String s = getSuffixString(rootPos);
		for (int i = rootPos + 1; i <= pos; i++) {
			s = getString(i, s);
		}
		return s;
	}

	public Set<String> getKeySet() {
		LinkedHashSet<String> s = new LinkedHashSet<String>();
		for (int i = 0; i < curKeyIndex; i++) {
			s.add(getKey(i));
		}
		return s;
	}

	public int getIndex(String key) {
		int l = 0, h = (curKeyIndex - 1) / window;
		String s = getKey(h * window);
		int i = s.compareTo(key);
		if (i == 0)
			return h * window;
		else if (i < 0)
			return getPosFromRoot(key, h * window);

		while (l <= h) {
			int m = ((l + h) / 2);
			s = getKey(m * window);
			i = s.compareTo(key);
			if (i == 0)
				return m * window;
			else if (i < 0) {
				int k = getPosFromRoot(key, m * window);
				if (k >= 0)
					return k;
				l = m + 1;
			} else {
				int k = getPosFromRoot(key, (m - 1) * window);
				if (k >= 0)
					return k;
				h = m - 1;
			}
		}
		return -1;
	}

	public float getCompresssionRatio() {
		return (float) totalMemoryBytes / totalOriginalBytes;
	}

	private static byte[][] resize2DimByteArray(byte[][] input, int size) {
		if (size < 0)
			return null;

		if (size <= input.length)
			return input;

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
		for (int i = 0; i < keys[pos].length - 1; i++)
			b[i] = keys[pos][i + 1];
		return new String(b);
	}

	private int getPosFromRoot(String key, int pos) {
		for (int i = pos + 1; i < pos + window && i < curKeyIndex; i++) {
			String s = getKey(i);
			if (s.equals(key))
				return i;
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
			if (s1.charAt(i) != s2.charAt(i))
				return i;
		}
		return i;
	}

	public static PrefixEncodedTermSet load(String file, FileSystem fs) throws IOException {
		FSDataInputStream in;

		in = fs.open(new Path(file));
		PrefixEncodedTermSet terms = new PrefixEncodedTermSet();
		terms.readFields(in);

		return terms;
	}

	public static PrefixEncodedTermSet loadFromPlainTextFile(String file, FileSystem fs, int window)
			throws IOException {
		FSLineReader reader = new FSLineReader(new Path(file), fs);
		PrefixEncodedTermSet terms = new PrefixEncodedTermSet(window);

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

			if (cnt % 1000000 == 0)
				LOGGER.info("read " + cnt + " lines");

		}

		LOGGER.info("compression ratio: " + terms.getCompresssionRatio());
		
		return terms;
	}

}
