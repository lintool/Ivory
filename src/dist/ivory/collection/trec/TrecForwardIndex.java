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

package ivory.collection.trec;

import ivory.data.DocumentForwardIndex;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocument;

/**
 * <p>
 * Object representing a document forward index for TREC collections.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class TrecForwardIndex implements DocumentForwardIndex<TrecDocument> {

	private static final Logger sLogger = Logger.getLogger(TrecForwardIndex.class);

	private long[] mOffsets;
	private int[] mLengths;
	private FSDataInputStream mCollectionStream;
	private DocnoMapping mDocnoMapping;

	public TrecDocument getDocid(String docid) {
		return getDocno(mDocnoMapping.getDocno(docid));
	}

	public String getContentType() {
		return "text/plain";
	}

	public TrecDocument getDocno(int docno) {
		TrecDocument doc = new TrecDocument();

		try {
			sLogger.info("docno " + docno + ": byte offset " + mOffsets[docno] + ", length "
					+ mLengths[docno]);

			mCollectionStream.seek(mOffsets[docno]);

			byte[] arr = new byte[mLengths[docno]];

			mCollectionStream.read(arr);

			TrecDocument.readDocument(doc, new String(arr));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return doc;
	}

	public void loadIndex(DocnoMapping mapping, String file, String path) throws IOException {
		Path p = new Path(file);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream in = fs.open(p);

		// docnos start at one, so we need an array that's one larger than
		// number of docs
		int sz = in.readInt() + 1;
		mOffsets = new long[sz];
		mLengths = new int[sz];

		for (int i = 1; i < sz; i++) {
			mOffsets[i] = in.readLong();
			mLengths[i] = in.readInt();
		}
		in.close();

		mCollectionStream = fs.open(new Path(path));
		mDocnoMapping = mapping;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("usage: [index-path]");
			System.exit(-1);
		}

		FileSystem fs = FileSystem.get(new Configuration());
		String indexPath = args[0];

		String collectionPath = RetrievalEnvironment.readCollectionPath(fs, indexPath);
		String indexFile = RetrievalEnvironment.getDocumentForwardIndex(indexPath);
		String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);

		sLogger.info("index path: " + indexPath);
		sLogger.info("output path: " + indexFile);
		sLogger.info("collection path: " + collectionPath);
		sLogger.info("mapping file: " + mappingFile);

		TrecDocnoMapping docnoMapping = new TrecDocnoMapping();
		docnoMapping.loadMapping(new Path(mappingFile), fs);

		TrecForwardIndex findex = new TrecForwardIndex();
		findex.loadIndex(docnoMapping, indexFile, collectionPath);

		findex.getDocno(230);

		findex.getDocno(5231);
	}

}
