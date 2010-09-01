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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

/**
 * <p>
 * Array-based implementation of <code>DfTable</code>. Binary search is used
 * for lookup.
 * </p>
 * 
 * @author Jimmy Lin
 * @author Tamer Elsayed
 * 
 */
public class DfTableArray implements DfTable {
	private int mNumTerms;

	private int mMaxDf = 0;
	private int mMaxDfTerm;

	private int mDfOne = 0;

	private int[] mDfs;

	/**
	 * Creates a <code>DfTableArray</code> object.
	 * 
	 * @param file
	 *            collection frequency data file
	 * @throws IOException
	 */
	public DfTableArray(String file) throws IOException {
		this(file, FileSystem.get(new Configuration()));
	}

	/**
	 * Creates a <code>DfTableArray</code> object.
	 * 
	 * @param file
	 *            collection frequency data file
	 * @param fs
	 *            FileSystem to read from
	 * @throws IOException
	 */
	public DfTableArray(String file, FileSystem fs) throws IOException {
		this(new Path(file), fs);
	}

	/**
	 * Creates a <code>DfTableArray</code> object.
	 * 
	 * @param file
	 *            collection frequency data file path
	 * @param fs
	 *            FileSystem to read from
	 * @throws IOException
	 */
	public DfTableArray(Path file, FileSystem fs) throws IOException {
		FSDataInputStream in = fs.open(file);

		this.mNumTerms = in.readInt();

		mDfs = new int[mNumTerms];

		for (int i = 0; i < mNumTerms; i++) {
			int df = WritableUtils.readVInt(in);

			mDfs[i] = df;

			if (df > mMaxDf) {
				mMaxDf = df;
				mMaxDfTerm = i + 1;
			}

			if (df == 1) {
				mDfOne++;
			}
		}

		in.close();
	}

	public int getDf(int term) {
		return mDfs[term - 1];
	}

	public int getVocabularySize() {
		return mNumTerms;
	}

	public int getMaxDf() {
		return mMaxDf;
	}

	public int getMaxDfTerm() {
		return mMaxDfTerm;
	}

	public int getCountOfTermWithDfOne() {
		return mDfOne;
	}
}
