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

import ivory.preprocess.BuildIntDocVectorsForwardIndex;
import ivory.util.RetrievalEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.debug.MemoryUsageUtils;

/**
 * Object providing an index into one or more <code>SequenceFile</code>s
 * containing {@link IntDocVector}s, providing random access to the document
 * vectors.
 * 
 * @see BuildIntDocVectorsForwardIndex
 * 
 * @author Jimmy Lin
 */
public class IntDocVectorsForwardIndex {

	private static final Logger sLogger = Logger.getLogger(IntDocVectorsForwardIndex.class);
	{
		sLogger.setLevel (Level.WARN);
	}

	private static final NumberFormat sFormatW5 = new DecimalFormat("00000");

	private FileSystem mFs;
	private Configuration mConf;

	private long[] mPositions;

	private String mPath;

	private int mDocnoOffset;
	private int mCollectionDocumentCount;

	/**
	 * Creates an <code>IntDocVectorsIndex</code> object.
	 * 
	 * @param indexPath
	 *            location of the index file
	 * @param fs
	 *            handle to the FileSystem
	 * @throws IOException
	 */
	public IntDocVectorsForwardIndex(String indexPath, FileSystem fs) throws IOException {
		this(indexPath, fs, false);
	}


	public IntDocVectorsForwardIndex(String indexPath, FileSystem fs, boolean weighted) throws IOException {
		mFs = fs;
		mConf = fs.getConf();

		RetrievalEnvironment env = new RetrievalEnvironment (indexPath, fs);
		mPath = (weighted ? env.getWeightedIntDocVectorsDirectory () : env.getIntDocVectorsDirectory ());
		sLogger.debug ("mPath: " + mPath);

		String forwardIndexPath = (weighted ? env.getWeightedIntDocVectorsForwardIndex () : env.getIntDocVectorsForwardIndex ());
		sLogger.debug ("forwardIndexPath: " + forwardIndexPath);
		FSDataInputStream posInput = fs.open (new Path (forwardIndexPath));

		mDocnoOffset = posInput.readInt();
		mCollectionDocumentCount = posInput.readInt();

		mPositions = new long[mCollectionDocumentCount];
		for (int i = 0; i < mCollectionDocumentCount; i++) {
			mPositions[i] = posInput.readLong();
		}
	}

	public int getDocCount () {
		return mCollectionDocumentCount;
	}


	/**
	 * Returns the document vector given a docno.
	 */
	public IntDocVector getDocVector(int docno) throws IOException {
		if (docno > mCollectionDocumentCount || docno < 1)
			return null;

		long pos = mPositions[docno - mDocnoOffset - 1];

		int fileNo = (int) (pos / BuildIntDocVectorsForwardIndex.BigNumber);
		pos = pos % BuildIntDocVectorsForwardIndex.BigNumber;

		SequenceFile.Reader reader = new SequenceFile.Reader(mFs, new Path(mPath + "/part-"
				+ sFormatW5.format(fileNo)), mConf);

		IntWritable key = new IntWritable();
		IntDocVector value;

		try {
			value = (IntDocVector) reader.getValueClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate key/value pair!");
		}

		reader.seek(pos);
		reader.next(key, value);

		if (key.get() != docno) {
			sLogger.error("unable to doc vector for docno " + docno + ": found docno " + key
					+ " instead");
			return null;
		}

		reader.close();
		return value;
	}

	/**
	 * Simple test program.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [indexPath]");
			System.exit(-1);
		}

		long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

		Configuration conf = new Configuration();

		IntDocVectorsForwardIndex index = new IntDocVectorsForwardIndex(args[0], FileSystem.get(conf));

		long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

		System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

		String term = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up postings of docno > ");
		while ((term = stdin.readLine()) != null) {
			int docno = Integer.parseInt(term);
			System.out.println(docno + ": " + index.getDocVector(docno));
			System.out.print("Look up postings of docno > ");
		}
	}
}
