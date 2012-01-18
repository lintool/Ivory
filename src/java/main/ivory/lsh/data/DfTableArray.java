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

package ivory.lsh.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.debug.MemoryUsageUtils;

/**
 * <p>
 * Array-based implementation of <code>DfTable</code>. Binary search is used
 * for lookup.
 * </p>
 * 
 * @see ExtractDfFromPostings
 * @author Jimmy Lin
 * 
 */
public class DfTableArray implements DfTable {
	private int mNumDocs;
	private int mNumTerms;

	private int mMaxDf = 0;
	private String mMaxDfTerm;

	private int mDfOne = 0;

	private String[] mTerms;
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
//		for(int i=0;i<20;i++){
//			int j= 100*i+1000;
//			while(mDfs[j]==1){
//				j++;
//			}
//			System.out.println(mTerms[j]+" "+mDfs[j]);
//
//		}
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
		FSDataInputStream in = fs.open(new Path(file));

		this.mNumDocs = in.readInt();
		this.mNumTerms = in.readInt();

		mTerms = new String[mNumTerms];
		mDfs = new int[mNumTerms];

		for (int i = 0; i < mNumTerms; i++) {
			String term = in.readUTF();
			
			//changed by Ferhan Ture : df table isn't read properly with commented line
			//int df = WritableUtils.readVInt(in);
			int df = in.readInt();
			
			mTerms[i] = term;
			mDfs[i] = df;

			if (df > mMaxDf) {
				mMaxDf = df;
				mMaxDfTerm = term;
			}

			if (df == 1) {
				mDfOne++;
			}
		}

		in.close();
	}

	public int getDf(String term) {
		int index = Arrays.binarySearch(mTerms, term);

		if (index < 0)
			return -1;

		return mDfs[index];
	}

	public int getDocumentCount() {
		return mNumDocs;
	}

	public int getVocabularySize() {
		return mNumTerms;
	}

	public int getMaxDf() {
		return mMaxDf;
	}

	public String getMaxDfTerm() {
		return mMaxDfTerm;
	}

	public int getCountOfTermWithDfOne() {
		return mDfOne;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [df-file]");
			System.exit(-1);
		}

		long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

		DfTableArray dfs = new DfTableArray(args[0]);

		System.out.println("Number of documents: " + dfs.getDocumentCount());
		System.out.println("Vocab size: " + dfs.getVocabularySize());
		System.out.println("term with max df is " + dfs.getMaxDfTerm() + ", df=" + dfs.getMaxDf());
		System.out.println(dfs.getCountOfTermWithDfOne() + " terms have df=1");

		long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

		System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

		String term = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up df of stemmed term> ");
		while ((term = stdin.readLine()) != null) {
			System.out.println(term + ", df=" + dfs.getDf(term));
			System.out.print("Look up df of stemmed term > ");
		}
	}
}
