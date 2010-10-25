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
 * Object that keeps track of the length of each document in the collection as a
 * four-byte integers (ints). Document lengths are measured in number of terms.
 * </p>
 * 
 * <p>
 * Document length data is stored in a serialized data file, in the following
 * format:
 * </p>
 * 
 * <ul>
 * <li>An integer that specifies the docno offset <i>d</i>, where <i>d</i> + 1
 * is the first docno in the collection.</li>
 * <li>An integer that specifies the number of documents in the collection
 * <i>n</i>.</li>
 * <li>Exactly <i>n</i> ints, one for each document in the collection.</li>
 * </ul>
 * 
 * <p>
 * Since the documents are numbered sequentially starting at <i>d</i> + 1, each
 * short corresponds unambiguously to a particular document.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class DocLengthTable4B implements DocLengthTable {
	static final Logger sLogger = Logger.getLogger(DocLengthTable4B.class);

	private int[] mLengths;
	private int mDocCount;
	private float mAvgDocLength;
	private int mDocnoOffset;

	/**
	 * Creates a new <code>DocLengthTable4B</code>.
	 * 
	 * @param file
	 *            document length data file
	 * @throws IOException
	 */
	public DocLengthTable4B(String file, FileSystem fs) throws IOException {
		this(new Path(file), fs);
	}

	/**
	 * Creates a new <code>DocLengthTable4B</code>.
	 * 
	 * @param file
	 *            document length data file
	 * @param fs
	 *            FileSystem to read from
	 * @throws IOException
	 */
	public DocLengthTable4B(Path file, FileSystem fs) throws IOException {
		long docLengthSum = 0;
		mDocCount = 0;

		FSDataInputStream in = fs.open(file);

		// The docno offset.
		mDocnoOffset = in.readInt();

		// The size of the document collection.
		int sz = in.readInt() + 1;

		sLogger.info("Docno offset: " + mDocnoOffset);
		sLogger.info("Number of docs: " + (sz - 1));

		// Initialize an array to hold all the doc lengths.
		mLengths = new int[sz];

		// Read each doc length.
		for (int i = 1; i < sz; i++) {
			int l = in.readInt();
			mLengths[i] = l;
			docLengthSum += l;
			mDocCount++;

			if (i % 1000000 == 0)
				sLogger.info(i + " doclengths read");
		}

		in.close();

		sLogger.info("Total of " + mDocCount + " doclengths read");

		// Compute average doc length.
		mAvgDocLength = docLengthSum * 1.0f / mDocCount;
	}

	// Inherit interface documentation.
	public int getDocLength(int docno) {
		return mLengths[docno - mDocnoOffset];
	}

	// Inherit interface documentation.
	public int getDocnoOffset() {
		return mDocnoOffset;
	}

	// Inherit interface documentation.
	public float getAvgDocLength() {
		return mAvgDocLength;
	}

	// Inherit interface documentation.
	public int getDocCount() {
		return mDocCount;
	}
}
