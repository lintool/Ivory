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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * <p>
 * Object that keeps track of the length of each document in the collection.
 * Document lengths are measured in number of terms.
 * </p>
 * 
 * <p>
 * Document length data is stored in a serialized data file, in the following
 * format: the data file consists of one long stream of integers. The first
 * integer in the stream specifies the number of documents in the collection (<i>n</i>).
 * Thereafter, the input stream contains exactly <i>n</i> integers, one for
 * every document in the collection. Since the documents are numbered
 * sequentially, each one of these integers corresponds to the length of
 * documents 1 ... <i>n</i> in the collection. Note that documents are numbered
 * starting from one because it is impossible to express zero in many
 * compression schemes (e.g., Golomb encoding).
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class DocLengthTable4B implements DocLengthTable {
	static final Logger sLogger = Logger.getLogger(DocLengthTable4B.class);

	protected int[] mLengths;
	protected int nDocs;
	protected float avgDocLength;
	protected int mDocnoOffset;

	/**
	 * Creates a new <code>DocLengthTable</code>.
	 * 
	 * @param file
	 *            document length data file
	 * @throws IOException
	 */
	public DocLengthTable4B(String file, FileSystem fs) throws IOException {
		this(new Path(file), fs);
	}	
	
	/**
	 * Creates a new <code>DocLengthTable</code>.
	 * 
	 * @param file
	 *            document length data file
	 * @param fs
	 *            FileSystem to read from
	 * @throws IOException
	 */
	public DocLengthTable4B(Path file, FileSystem fs) throws IOException {
		long docLengthSum = 0;
		nDocs = 0;

		FSDataInputStream in = fs.open(file);

		// docno offset
		mDocnoOffset = in.readInt();

		// this is the size of the document collection
		int sz = in.readInt() + 1;

		sLogger.info("Docno offset: " + mDocnoOffset);
		sLogger.info("Number of docs: " + (sz - 1));

		// initialize an array to hold all the doc lengths
		mLengths = new int[sz];

		// read each doc length
		for (int i = 1; i < sz; i++) {
			int l = in.readInt();
			mLengths[i] = l;
			docLengthSum += l;
			nDocs++;

			if (i % 1000000 == 0)
				sLogger.info(i + " doclengths read");
		}

		in.close();

		sLogger.info("Total of " + nDocs + " doclengths read");

		// compute avg doc length
		avgDocLength = docLengthSum * 1.0f / nDocs;
	}

	/**
	 * Returns the length of a document.
	 */
	public int getDocLength(int docno) {
		// docnos are numbered starting from one
		return mLengths[docno - mDocnoOffset];
	}

	public int getDocnoOffset() {
		return mDocnoOffset;
	}

	/**
	 * Returns the average length of documents in the collection.
	 */
	public float getAvgDocLength() {
		return avgDocLength;
	}

	/**
	 * Returns number of documents in the collection.
	 */
	public int getDocCount() {
		return nDocs;
	}

}
