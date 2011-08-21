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

package ivory.core.data.stat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

/**
 * <p>
 * Array-based implementation of <code>CfTable</code>. Binary search is used
 * for lookup.
 * </p>
 * 
 * @author Jimmy Lin
 * @author Tamer Elsayed
 * 
 */
public class CfTableArray implements CfTable {
	private final int numTerms;
	private final long[] cfs;

	private long collectionSize;

	private long maxCf = 0;
	private int maxCfTerm;

	private int numSingletonTerms = 0;

	/**
	 * Creates a <code>CfTableArray</code> object.
	 *
	 * @param file collection frequency data file
	 * @throws IOException
	 */
	public CfTableArray(Path file) throws IOException {
		this(file, FileSystem.get(new Configuration()));
	}

	/**
	 * Creates a <code>CfTableArray</code> object.
	 *
	 * @param file collection frequency data file
	 * @param fs   FileSystem to read from
	 * @throws IOException
	 */
	public CfTableArray(Path file, FileSystem fs) throws IOException {
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

	public long getCf(int term) {
		return cfs[term - 1];
	}

	public long getCollectionSize() {
		return collectionSize;
	}

	public int getVocabularySize() {
		return numTerms;
	}

	public long getMaxCf() {
		return maxCf;
	}

	public int getMaxCfTerm() {
		return maxCfTerm;
	}

	public int getNumOfSingletonTerms() {
		return numSingletonTerms;
	}
}
