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
 * Array-based implementation of <code>CfTable</code>. Binary search is used
 * for lookup.
 * </p>
 * 
 * @author Jimmy Lin
 * @author Tamer Elsayed
 * 
 */
public class CfTableArray implements CfTable {
	private int mNumTerms;
	private long mTotalTermCount;

	private long mMaxCf = 0;
	private int mMaxCfTerm;

	private int mCfOne = 0;

	private long[] mCfs;

	/**
	 * Creates a <code>CfTableArray</code> object.
	 * 
	 * @param file
	 *            collection frequency data file
	 * @throws IOException
	 */
	public CfTableArray(String file) throws IOException {
		this(file, FileSystem.get(new Configuration()));
	}

	public CfTableArray(String file, FileSystem fs) throws IOException {
		this(new Path(file), fs);
	}

	public CfTableArray(Path file, FileSystem fs) throws IOException {
		FSDataInputStream in = fs.open(file);

		this.mNumTerms = in.readInt();

		mCfs = new long[mNumTerms];

		for (int i = 0; i < mNumTerms; i++) {
			long cf = WritableUtils.readVLong(in);

			mCfs[i] = cf;
			mTotalTermCount += cf;

			if (cf > mMaxCf) {
				mMaxCf = cf;
				mMaxCfTerm = i + 1;
			}

			if (cf == 1) {
				mCfOne++;
			}
		}

		in.close();
	}

	public long getCf(int term) {
		return mCfs[term - 1];
	}

	public long getCollectionSize() {
		return mTotalTermCount;
	}

	public int getVocabularySize() {
		return mNumTerms;
	}

	public long getMaxCf() {
		return mMaxCf;
	}

	public int getMaxCfTerm() {
		return mMaxCfTerm;
	}

	public int getCountOfTermWithCfOne() {
		return mCfOne;
	}
}
