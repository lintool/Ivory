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

import ivory.index.ExtractCfFromPostings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.debug.MemoryUsageUtils;

/**
 * <p>
 * Array-based implementation of <code>CfTable</code>. Binary search is used
 * for lookup.
 * </p>
 * 
 * @see ExtractCfFromPostings
 * @author Jimmy Lin
 * 
 */
public class CfTableArray implements CfTable {
	private int mNumDocs;
	private int mNumTerms;
	private long mTotalTermCount;

	private long mMaxCf = 0;
	private String mMaxCfTerm;

	private int mCfOne = 0;

	private String[] mTerms;
	private long[] mCfs;

	/**
	 * Creates a <code>CfTableArray</code> object.
	 * 
	 * @param file
	 *            collection frequency data file
	 * @throws IOException
	 */
	public CfTableArray(String file) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);

		FSDataInputStream in = fs.open(new Path(file));

		this.mNumDocs = in.readInt();
		this.mNumTerms = in.readInt();

		mTerms = new String[mNumTerms];
		mCfs = new long[mNumTerms];

		for (int i = 0; i < mNumTerms; i++) {
			String term = in.readUTF();
			long cf = WritableUtils.readVLong(in);

			mTerms[i] = term;
			mCfs[i] = cf;
			mTotalTermCount += cf;

			if (cf > mMaxCf) {
				mMaxCf = cf;
				mMaxCfTerm = term;
			}

			if (cf == 1) {
				mCfOne++;
			}
		}

		in.close();
	}

	public long getCf(String term) {
		int index = Arrays.binarySearch(mTerms, term);

		if (index < 0)
			return -1;

		return mCfs[index];
	}

	public long getCollectionSize() {
		return mTotalTermCount;
	}

	public int getDocumentCount() {
		return mNumDocs;
	}

	public int getVocabularySize() {
		return mNumTerms;
	}

	public long getMaxCf() {
		return mMaxCf;
	}

	public String getMaxCfTerm() {
		return mMaxCfTerm;
	}

	public int getCountOfTermWithCfOne() {
		return mCfOne;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [cf-file]");
			System.exit(-1);
		}

		long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

		CfTableArray cfs = new CfTableArray(args[0]);

		System.out.println("Number of documents: " + cfs.getDocumentCount());
		System.out.println("Vocab size: " + cfs.getVocabularySize());
		System.out.println("Collection size: " + cfs.getCollectionSize());
		System.out.println("term with max cf is " + cfs.getMaxCfTerm() + ", cf=" + cfs.getMaxCf());
		System.out.println(cfs.getCountOfTermWithCfOne() + " terms have cf=1");

		long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

		System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

		String term = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up cf of stemmed term> ");
		while ((term = stdin.readLine()) != null) {
			System.out.println(term + ", cf=" + cfs.getCf(term));
			System.out.print("Look up cf of stemmed term > ");
		}
	}
}
