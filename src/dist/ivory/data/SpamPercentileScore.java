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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.debug.MemoryUsageUtils;

public class SpamPercentileScore implements DocScoreTable {
	static final Logger sLogger = Logger.getLogger(SpamPercentileScore.class);

	protected byte[] mScores;
	protected int nDocs;
	protected int mDocnoOffset;

	public SpamPercentileScore() {

	}

	public void initialize(String file, FileSystem fs) throws IOException {
		FSDataInputStream in = fs.open(new Path(file));

		// docno offset
		mDocnoOffset = in.readInt();

		// this is the size of the document collection
		int sz = in.readInt() + 1;

		sLogger.info("Docno offset: " + mDocnoOffset);
		sLogger.info("Number of docs: " + (sz - 1));

		// initialize an array to hold all the doc scores
		mScores = new byte[sz];

		// read each doc length
		for (int i = 1; i < sz; i++) {
			mScores[i] = in.readByte();
			nDocs++;

			if (i % 1000000 == 0)
				sLogger.info(i + " docscores read");
		}

		in.close();

		sLogger.info("Total of " + nDocs + " docscores read");

	}

	/**
	 * Returns the length of a document.
	 */
	public float getScore(int docno) {
		// docnos are numbered starting from one
		return (float) Math.log(mScores[docno - mDocnoOffset]);
	}

	public int getDocnoOffset() {
		return mDocnoOffset;
	}

	/**
	 * Returns number of documents in the collection.
	 */
	public int getDocCount() {
		return nDocs;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

		DocScoreTable scores = new SpamPercentileScore();
		scores.initialize(args[0], FileSystem.get(new Configuration()));
		long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

		System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

		String docno = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up postings of term> ");
		while ((docno = stdin.readLine()) != null) {
			System.out.println(scores.getScore(Integer.parseInt(docno)));
			System.out.print("Look up postings of term> ");
		}

	}
}
