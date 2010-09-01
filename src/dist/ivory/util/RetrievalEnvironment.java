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

package ivory.util;

import ivory.data.DocLengthTable;
import ivory.data.Posting;
import ivory.data.PostingsList;
import ivory.data.PostingsListDocSortedPositional;
import ivory.data.PostingsReader;
import ivory.data.PrefixEncodedForwardIndex;
import ivory.data.ProximityPostingsListOrderedWindow;
import ivory.data.ProximityPostingsListUnorderedWindow;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.FSProperty;

/**
 * @author Don Metzler
 * @author Jimmy Lin
 * 
 */
public class RetrievalEnvironment {

	/**
	 * logger
	 */
	private static final Logger LOGGER = Logger.getLogger(RetrievalEnvironment.class);

	/**
	 * postings decoder
	 */
	private Constructor<? extends PostingsReader> mPostingsReaderConstructor;

	/**
	 * default df value
	 */
	private int mDefaultDF;

	/**
	 * default cf value
	 */
	private long mDefaultCF;

	/**
	 * type of postings in the index
	 */
	private String mPostingsType;

	/**
	 * document length lookup
	 */
	private DocLengthTable mDocumentLengths;

	/**
	 * number of documents in collection
	 */
	private int mDocumentCount;

	/**
	 * average document length
	 */
	// private float mAvgDocumentLength;
	/**
	 * number of terms in the collection
	 */
	private long mTermCount;

	/**
	 * tokenizer
	 */
	private Tokenizer mTokenizer = null;

	/**
	 * postings list forward index
	 */
	private PrefixEncodedForwardIndex mTermPostingsIndex;

	public RetrievalEnvironment(String indexRoot) throws IOException {
		this(indexRoot, true);
	}

	/**
	 * @param indexRoot
	 */
	public RetrievalEnvironment(String indexRoot, boolean loadDoclengths) throws IOException {
		// hadoop variables
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// get number of postings
		mTermCount = FSProperty.readInt(fs, indexRoot + "/property.NumberOfPostings");

		// get number of documents
		mDocumentCount = FSProperty.readInt(fs, indexRoot + "/property.CollectionDocumentCount");

		// total length of the collection
		mTermCount = FSProperty.readLong(fs, indexRoot + "/property.CollectionTermCount");

		// get the posting type
		mPostingsType = FSProperty.readString(fs, indexRoot + "/property.PostingsType");

		// read the table of doc lengths
		if (loadDoclengths) {
			LOGGER.info("Loading doclengths table...");
			mDocumentLengths = new DocLengthTable(indexRoot + "/doclengths.dat", fs);
		}

		// read document frequencies
		mDefaultDF = mDocumentCount / 100; // heuristic

		// read collection frequencies
		mDefaultCF = mDefaultDF * 2; // heuristic

		LOGGER.info("IndexPath: " + indexRoot);
		LOGGER.info("PostingsType: " + mPostingsType);
		LOGGER.info("Collection document count: " + mDocumentCount);
		LOGGER.info("Collection term count: " + mTermCount);

		// initialize the tokenizer; this information is stored along with the
		// index since we need to use the same tokenizer to parse queries
		try {
			String tokenizer = FSProperty.readString(fs, indexRoot + "/property.Tokenizer");
			LOGGER.info("Tokenizer: " + tokenizer);
			mTokenizer = (Tokenizer) Class.forName(tokenizer).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		LOGGER.info("Loading postings index...");
		String termsFile = indexRoot + "/postings-index-terms.dat";
		String positionsFile = indexRoot + "/postings-index-positions.dat";
		String postingsPath = indexRoot + "/postings/";
		int termCnt = FSProperty.readInt(fs, indexRoot + "/property.PostingsForwardIndexTermCount");

		LOGGER.info("Number of terms: " + termCnt);
		mTermPostingsIndex = new PrefixEncodedForwardIndex(new Path(termsFile), new Path(
				positionsFile), postingsPath);
		LOGGER.info("Done!");

		try {
			// use reflection to get the constructor for the postings reader
			Class<? extends PostingsReader> c = (Class<? extends PostingsReader>) Class
					.forName(mPostingsType + "$PostingsReader");

			// this is potentially dangerous as we don't actually check for
			// parameter types...
			mPostingsReaderConstructor = (Constructor<? extends PostingsReader>) c
					.getConstructors()[0];
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}

	}

	public long documentCount() {
		return mDocumentCount;
	}

	public int documentLength(int docid) {
		return mDocumentLengths.getDocLength(docid);
	}

	public long termCount() {
		return mTermCount;
	}

	/**
	 * @param expression
	 */
	public PostingsReader getPostingsReader(String expression) {
		PostingsReader postings = null;

		// set up a reader for decoding the postings
		try {
			if (expression.startsWith("#od")) {
				int gapSize = Integer.parseInt(expression.substring(3, expression.indexOf('(')));
				expression = expression.substring(expression.indexOf('(') + 1,
						expression.indexOf(')')).trim();
				String[] terms = expression.split(" ");

				List<PostingsListDocSortedPositional.PostingsReader> readers = new ArrayList<PostingsListDocSortedPositional.PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsList list = _getPostingsList(terms[i]);
					readers
							.add((PostingsListDocSortedPositional.PostingsReader) mPostingsReaderConstructor
									.newInstance(list.getRawBytes(), list.getNumberOfPostings(),
											mDocumentLengths.getDocCount(), list));
				}

				postings = new ProximityPostingsListOrderedWindow(readers
						.toArray(new PostingsListDocSortedPositional.PostingsReader[0]), gapSize);
			} else if (expression.startsWith("#uw")) {
				int windowSize = Integer.parseInt(expression.substring(3, expression.indexOf('(')));
				expression = expression.substring(expression.indexOf('(') + 1,
						expression.indexOf(')')).trim();
				String[] terms = expression.split(" ");

				List<PostingsListDocSortedPositional.PostingsReader> readers = new ArrayList<PostingsListDocSortedPositional.PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsList list = _getPostingsList(terms[i]);
					readers
							.add((PostingsListDocSortedPositional.PostingsReader) mPostingsReaderConstructor
									.newInstance(list.getRawBytes(), list.getNumberOfPostings(),
											mDocumentLengths.getDocCount(), list));
				}

				postings = new ProximityPostingsListUnorderedWindow(readers
						.toArray(new PostingsListDocSortedPositional.PostingsReader[0]), windowSize);
			} else {
				PostingsList postingsList = _getPostingsList(expression);

				if (postingsList == null)
					throw new RuntimeException("Unable to fetch postings list for \"" + expression
							+ "\"");

				postings = postingsList.getPostingsReader();
			}
		} catch (Exception e) {
			LOGGER.error("Error: unable to initialize PostingsReader");
			e.printStackTrace();
		}

		return postings;
	}

	/**
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private PostingsList _getPostingsList(String term) throws IOException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		Text key = new Text();
		PostingsList value = new PostingsListDocSortedPositional();

		long start = System.currentTimeMillis();
		mTermPostingsIndex.getTermPosting(term, key, value);
		long duration = System.currentTimeMillis() - start;

		value.setCollectionDocumentCount(mDocumentCount);

		LOGGER.info("fetched postings for term \"" + key + "\" in " + duration + " ms");

		return value;
	}

	public long collectionFrequency(String expression) throws Exception {
		// TODO: fix this
		// we currently don't support cf for proximity expressions
		if (expression.startsWith("#od")) {
			return mDefaultCF;
		} else if (expression.startsWith("#uw")) {
			return mDefaultCF;
		}

		try {
			return _getPostingsList(expression).getCf();
		} catch (Exception e) {
			throw new Exception(e);
		}
	}

	public int documentFrequency(String expression) throws Exception {
		// TODO: fix this
		// we currently don't support df for proximity expressions
		if (expression.startsWith("#od")) {
			return mDefaultDF;
		} else if (expression.startsWith("#uw")) {
			return mDefaultDF;
		}

		try {
			return _getPostingsList(expression).getDf();
		} catch (Exception e) {
			throw new Exception(e);
		}
	}

	public String[] tokenize(String text) {
		return mTokenizer.processContent(text);
	}

	public int getDefaultDF() {
		return mDefaultDF;
	}

	public long getDefaultCF() {
		return mDefaultCF;
	}

	private static Random r = new Random();

	private static String appendPath(String base, String file) {
		return base + (base.endsWith("/") ? "" : "/") + file;
	}

	public static String getDocumentForwardIndex(String indexPath) {
		return appendPath(indexPath, "doc-forward-index.dat");
	}

	public static String getDocumentForwardIndexDirectory(String indexPath) {
		return appendPath(indexPath, "doc-forward-index/");
	}

	public static String getDocnoMappingFile(String indexPath) {
		return appendPath(indexPath, "docno-mapping.dat");
	}

	public static String getDocnoMappingDirectory(String indexPath) {
		return appendPath(indexPath, "docno-mapping/");
	}

	public static String getDoclengthsFile(String indexPath) {
		return appendPath(indexPath, "doclengths.dat");
	}

	public static String getDoclengthsDirectory(String indexPath) {
		return appendPath(indexPath, "doclengths/");
	}

	public static String getDfFile(String indexPath) {
		return appendPath(indexPath, "df.dat");
	}

	public static String getDfDirectory(String indexPath) {
		return appendPath(indexPath, "df/");
	}

	public static String getCfFile(String indexPath) {
		return appendPath(indexPath, "cf.dat");
	}

	public static String getCfDirectory(String indexPath) {
		return appendPath(indexPath, "cf/");
	}

	public static String getPostingsDirectory(String indexPath) {
		return appendPath(indexPath, "postings/");
	}

	public static String getPostingsIndexTerms(String indexPath) {
		return indexPath + (indexPath.endsWith("/") ? "" : "/") + "postings-index-terms.dat";
	}

	public static String getPostingsIndexPositions(String indexPath) {
		return indexPath + (indexPath.endsWith("/") ? "" : "/") + "postings-index-positions.dat";
	}

	public static String getTempDirectory(String indexPath) {
		return appendPath(indexPath, "tmp" + r.nextInt(10000));
	}

	public static String readCollectionName(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.CollectionName"));
	}

	public static void writeCollectionName(FileSystem fs, String indexPath, String s) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.CollectionName"), s);
	}

	public static String readCollectionPath(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.CollectionPath"));
	}

	public static void writeCollectionPath(FileSystem fs, String indexPath, String s) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.CollectionPath"), s);
	}

	public static int readCollectionDocumentCount(FileSystem fs, String indexPath) {
		return FSProperty.readInt(fs, appendPath(indexPath, "property.CollectionDocumentCount"));
	}

	public static void writeCollectionDocumentCount(FileSystem fs, String indexPath, int n) {
		FSProperty.writeInt(fs, appendPath(indexPath, "property.CollectionDocumentCount"), n);
	}

	public static long readCollectionTermCount(FileSystem fs, String indexPath) {
		return FSProperty.readLong(fs, appendPath(indexPath, "property.CollectionTermCount"));
	}

	public static void writeCollectionTermCount(FileSystem fs, String indexPath, long cnt) {
		FSProperty.writeLong(fs, appendPath(indexPath, "property.CollectionTermCount"), cnt);
	}

	public static String readDocumentForwardIndexClass(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.ForwardIndexClass"));
	}

	public static void writeDocumentForwardIndexClass(FileSystem fs, String indexPath, String s) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.ForwardIndexClass"), s);
	}

	public static String readInputFormat(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.InputFormat"));
	}

	public static void writeInputFormat(FileSystem fs, String indexPath, String s) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.InputFormat"), s);
	}

	public static String readTokenizerClass(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.Tokenizer"));
	}

	public static void writeTokenizerClass(FileSystem fs, String indexPath, String s) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.Tokenizer"), s);
	}

	public static String readDocnoMappingClass(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.DocnoMappingClass"));
	}

	public static void writeDocnoMappingClass(FileSystem fs, String indexPath, String s) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.DocnoMappingClass"), s);
	}

	public static int readDocnoOffset(FileSystem fs, String indexPath) {
		return FSProperty.readInt(fs, appendPath(indexPath, "property.DocnoOffset"));
	}

	public static void writeDocnoOffset(FileSystem fs, String indexPath, int n) {
		FSProperty.writeInt(fs, appendPath(indexPath, "property.DocnoOffset"), n);
	}

	public static String readPostingsType(FileSystem fs, String indexPath) {
		return FSProperty.readString(fs, appendPath(indexPath, "property.PostingsType"));
	}

	public static void writePostingsType(FileSystem fs, String indexPath, String type) {
		FSProperty.writeString(fs, appendPath(indexPath, "property.PostingsType"), type);
	}

	public static int readPostingsIndexTermCount(FileSystem fs, String indexPath) {
		return FSProperty.readInt(fs, appendPath(indexPath,
				"property.PostingsForwardIndexTermCount"));
	}

	public static void writePostingsIndexTermCount(FileSystem fs, String indexPath, int n) {
		FSProperty.writeInt(fs, appendPath(indexPath, "property.PostingsForwardIndexTermCount"), n);
	}

	public static int readNumberOfPostings(FileSystem fs, String indexPath) {
		return FSProperty.readInt(fs, appendPath(indexPath, "/property.NumberOfPostings"));
	}

	public static void writeNumberOfPostings(FileSystem fs, String indexPath, int numPostings) {
		FSProperty.writeInt(fs, appendPath(indexPath, "property.NumberOfPostings"), numPostings);
	}

	public static void main(String[] args) throws Exception {
		// String indexPath = "/umd/indexes/trec45noCRFR.positional.galago/";
		String indexPath = "/umd/indexes/clue.en.segment.01/";

		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, false);

		long startTime = System.currentTimeMillis();

		PostingsReader reader = null;
		Posting p = new Posting();
		int df = 0;
		String termOrig = "iraq";
		String termTokenized = env.tokenize(termOrig)[0];

		reader = env.getPostingsReader(termTokenized);
		df = reader.getNumberOfPostings();
		System.out.print(termOrig + ", tokenized=" + termTokenized + ", df=" + df
				+ "\n First ten postings: ");
		for (int i = 0; i < 10; i++) {
			reader.nextPosting(p);
			System.out.print(p);
		}

		System.out.println("\n");

		long endTime = System.currentTimeMillis();

		System.out.println("total time: " + (endTime - startTime));
	}
}
