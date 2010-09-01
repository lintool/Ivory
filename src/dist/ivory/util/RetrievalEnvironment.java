/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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
import ivory.data.DocLengthTable2B;
import ivory.data.DocScoreTable;
import ivory.data.IntDocVector;
import ivory.data.IntDocVectorsForwardIndex;
import ivory.data.IntPostingsForwardIndex;
import ivory.data.Posting;
import ivory.data.PostingsList;
import ivory.data.PostingsListDocSortedPositional;
import ivory.data.PostingsReader;
import ivory.data.PrefixEncodedTermIDMapWithIndex;
import ivory.data.ProximityPostingsListOrderedWindow;
import ivory.data.ProximityPostingsListUnorderedWindow;
import ivory.data.TermDocVector;
import ivory.data.TermDocVectorsForwardIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.debug.MemoryUsageUtils;
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
	 * postings reader cache
	 */
	Map<String, ivory.data.PostingsListDocSortedPositional.PostingsReader> mPostingsReaderCache = null;

	/**
	 * default df value
	 */
	private int mDefaultDf;

	/**
	 * default cf value
	 */
	private long mDefaultCf;

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

	private int mDocumentCountLocal = -1;

	/**
	 * number of terms in the collection
	 */
	private long mCollectionLength;

	/**
	 * tokenizer
	 */
	private Tokenizer mTokenizer = null;

	/**
	 * postings list forward index
	 */
	private IntPostingsForwardIndex mTermPostingsIndex;

	private PrefixEncodedTermIDMapWithIndex mTermIdMap;

	private FileSystem mFs;
	private String mIndexPath;

	private IntDocVectorsForwardIndex mIntDocVectorsIndex;
	// private TermDocVectorsIndex mTermDocVectorsIndex;

	private Map<String, DocScoreTable> mDocScores = new HashMap<String, DocScoreTable>();

	public RetrievalEnvironment(String indexPath, FileSystem fs) {
		mIndexPath = indexPath;
		mFs = fs;
	}

	public void initialize(boolean loadDoclengths) throws IOException {
		// get number of documents
		mDocumentCount = readCollectionDocumentCount();

		// If property.CollectionDocumentCount.local exists, it means that this
		// index is a partition of a large document collection. We want to use
		// the global doc count for score, but the Golomb compression is
		// determined using the local doc count.
		if (mFs.exists(new Path(mIndexPath + "/property.CollectionDocumentCount.local"))) {
			mDocumentCountLocal = FSProperty.readInt(mFs, mIndexPath
					+ "/property.CollectionDocumentCount.local");
		}

		mCollectionLength = readCollectionLength();
		mPostingsType = readPostingsType();

		// read the table of doc lengths
		if (loadDoclengths) {
			LOGGER.info("Loading doclengths table...");
			mDocumentLengths = new DocLengthTable2B(getDoclengthsData(), mFs);
		}

		// read document frequencies
		mDefaultDf = mDocumentCount / 100; // heuristic

		// read collection frequencies
		mDefaultCf = mDefaultDf * 2; // heuristic

		LOGGER.info("IndexPath: " + mIndexPath);
		LOGGER.info("PostingsType: " + mPostingsType);
		LOGGER.info("Collection document count: " + mDocumentCount);
		LOGGER.info("Collection length: " + mCollectionLength);

		// initialize the tokenizer; this information is stored along with the
		// index since we need to use the same tokenizer to parse queries
		try {
			String tokenizer = readTokenizerClass();
			LOGGER.info("Tokenizer: " + tokenizer);
			mTokenizer = (Tokenizer) Class.forName(tokenizer).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		LOGGER.info("Loading postings index...");

		long termCnt = readCollectionTermCount();

		LOGGER.info("Number of terms: " + termCnt);
		mTermPostingsIndex = new IntPostingsForwardIndex(mIndexPath, mFs);
		LOGGER.info("Done!");

		try {
			mTermIdMap = new PrefixEncodedTermIDMapWithIndex(new Path(getIndexTermsData()),
					new Path(getIndexTermIdsData()), new Path(getIndexTermIdMappingData()), 0.2f,
					true, mFs);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing Term to Id map!");
		}

		try {
			mIntDocVectorsIndex = new IntDocVectorsForwardIndex(mIndexPath, mFs);
		} catch (Exception e) {
			LOGGER
					.warn("Unable to load IntDocVectorsForwardIndex: relevance feedback will not be available.");
		}

		mPostingsReaderCache = new HashMap<String, ivory.data.PostingsListDocSortedPositional.PostingsReader>();
	}

	@SuppressWarnings("unchecked")
	public void loadDocScore(String type, String provider, String path) {
		try {
			Class<? extends DocScoreTable> clz = (Class<? extends DocScoreTable>) Class
					.forName(provider);
			DocScoreTable s = clz.newInstance();
			s.initialize(path, mFs);

			mDocScores.put(type, s);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
	}

	public float getDocScore(String type, int docno) {
		if (!mDocScores.containsKey(type))
			throw new RuntimeException("Error: docscore type \"" + type + "\" not found!");

		return mDocScores.get(type).getScore(docno);
	}

	public void loadIntDocVectorsIndex() {
		// mIntDocVectorsIndex
	}

	public void loadTermDocVectorsIndex() {

	}

	public long documentCount() {
		return mDocumentCount;
	}

	public int documentLength(int docid) {
		return mDocumentLengths.getDocLength(docid);
	}

	public long termCount() {
		return mCollectionLength;
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
					PostingsListDocSortedPositional.PostingsReader reader = constructPostingsReader(terms[i]);
					readers.add(reader);
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
					PostingsListDocSortedPositional.PostingsReader reader = constructPostingsReader(terms[i]);
					readers.add(reader);
				}

				postings = new ProximityPostingsListUnorderedWindow(readers
						.toArray(new PostingsListDocSortedPositional.PostingsReader[0]), windowSize);
			} else {
				postings = constructPostingsReader(expression);
			}
		} catch (Exception e) {
			LOGGER.error("Error: unable to initialize PostingsReader");
			e.printStackTrace();
		}

		return postings;
	}

	private ivory.data.PostingsListDocSortedPositional.PostingsReader constructPostingsReader(
			String expression) throws Exception {
		PostingsListDocSortedPositional.PostingsReader reader = null;
		if (mPostingsReaderCache != null) {
			reader = mPostingsReaderCache.get(expression);
		}
		if (reader == null) {
			PostingsList list = getPostingsList(expression);
			if (list == null) {
				return null;
			}
			reader = (PostingsListDocSortedPositional.PostingsReader) list.getPostingsReader();
			// reader = (PostingsListDocSortedPositional.PostingsReader)
			// mPostingsReaderConstructor.newInstance(list.getRawBytes(),
			// list.getNumberOfPostings(), mDocumentLengths.getDocCount(),
			// list);
			if (mPostingsReaderCache != null) {
				mPostingsReaderCache.put(expression, reader);
			}
		}

		return reader;
	}

	public void clearPostingsReaderCache() {
		mPostingsReaderCache.clear();
	}

	/**
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private PostingsList getPostingsList(String term) throws IOException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		// long start = System.currentTimeMillis();
		int termid = mTermIdMap.getID(term);

		if (termid == -1) {
			throw new RuntimeException("couldn't find term id for query term \"" + term + "\"");
		}

		PostingsList value = mTermPostingsIndex.getPostingsList(termid);
		// long duration = System.currentTimeMillis() - start;

		if (value == null)
			return null;

		if (mDocumentCountLocal != -1) {
			value.setCollectionDocumentCount(mDocumentCountLocal);
		} else {
			value.setCollectionDocumentCount(mDocumentCount);
		}

		// LOGGER.info("fetched \"" + term + "\" in " + duration + "ms (cf=" +
		// value.getCf() + ", df="
		// + value.getDf() + ")");

		return value;
	}

	public IntDocVector[] documentVectors(int[] docSet) throws IOException {
		IntDocVector[] dvs = new IntDocVector[docSet.length];

		for (int i = 0; i < docSet.length; i++) {
			dvs[i] = mIntDocVectorsIndex.getDocVector(docSet[i]);
		}

		return dvs;
	}

	/**
	 * Returns the collection frequency of a particular expression.
	 */
	public long collectionFrequency(String expression) throws Exception {
		// this is a heuristic: we currently don't support cf for proximity
		// expressions
		if (expression.startsWith("#od")) {
			return mDefaultCf;
		} else if (expression.startsWith("#uw")) {
			return mDefaultCf;
		}

		try {
			return getPostingsList(expression).getCf();
		} catch (Exception e) {
			throw new Exception(e);
		}
	}

	/**
	 * Returns the document frequency of a particular expression.
	 */
	public int documentFrequency(String expression) throws Exception {
		// this is a heuristic: we currently don't support df for proximity
		// expressions
		if (expression.startsWith("#od")) {
			return mDefaultDf;
		} else if (expression.startsWith("#uw")) {
			return mDefaultDf;
		}

		try {
			return getPostingsList(expression).getDf();
		} catch (Exception e) {
			throw new Exception(e);
		}
	}

	public String getTermFromId(int termid) {
		return mTermIdMap.getTerm(termid);
	}

	/**
	 * Tokenizes text according to the tokenizer used to process the document
	 * collection. This typically includes stopwords filtering, stemming, etc.
	 * 
	 * @param text
	 *            text to tokenize
	 * @return array of tokens
	 */
	public String[] tokenize(String text) {
		return mTokenizer.processContent(text);
	}

	/**
	 * Returns the default document frequency.
	 */
	public int getDefaultDf() {
		return mDefaultDf;
	}

	/**
	 * Returns the default collection frequency.
	 */
	public long getDefaultCf() {
		return mDefaultCf;
	}

	private static Random r = new Random();

	public static String appendPath(String base, String file) {
		return base + (base.endsWith("/") ? "" : "/") + file;
	}

	public String getDocnoMappingData() {
		return appendPath(mIndexPath, "docno-mapping.dat");
	}

	public String getDocnoMappingDirectory() {
		return appendPath(mIndexPath, "docno-mapping/");
	}

	/**
	 * Returns file that contains the document length data. This file serves as
	 * input to {@link DocLengthTable}, which provides random access to
	 * document lengths.
	 */
	public String getDoclengthsData() {
		return appendPath(mIndexPath, "doclengths.dat");
	}

	/**
	 * Returns directory that contains the document length data. Data in this
	 * directory is compiled into the denoted by {@link #getDoclengthsData()}.
	 */
	public String getDoclengthsDirectory() {
		return appendPath(mIndexPath, "doclengths/");
	}

	public String getPostingsDirectory() {
		return appendPath(mIndexPath, "postings/");
	}

	public String getApproxPostingsDirectory() {
		return appendPath(mIndexPath, "postings-approx/");
	}

	/**
	 * Returns directory that contains the {@link IntDocVector} representation
	 * of the collection.
	 */
	public String getIntDocVectorsDirectory() {
		return appendPath(mIndexPath, "int-doc-vectors/");
	}

	/**
	 * Returns file that contains an index into the {@link IntDocVector}
	 * representation of the collection. This file serves as input to
	 * {@link IntDocVectorsForwardIndex}, which provides random access to the document
	 * vectors.
	 */
	public String getIntDocVectorsForwardIndex() {
		return appendPath(mIndexPath, "int-doc-vectors-forward-index.dat");
	}

	/**
	 * Returns directory that contains the {@link TermDocVector} representation
	 * of the collection.
	 */
	public String getTermDocVectorsDirectory() {
		return appendPath(mIndexPath, "term-doc-vectors/");
	}

	/**
	 * Returns file that contains an index into the {@link TermDocVector}
	 * representation of the collection. This file serves as input to
	 * {@link TermDocVectorsForwardIndex}, which provides random access to the
	 * document vectors.
	 */
	public String getTermDocVectorsForwardIndex() {
		return appendPath(mIndexPath, "term-doc-vectors-forward-index.dat");
	}

	public String getWeightedTermDocVectorsDirectory() {
		return appendPath(mIndexPath, "wt-term-doc-vectors/");
	}

	public String getWeightedIntDocVectorsDirectory() {
		return appendPath(mIndexPath, "wt-int-doc-vectors/");
	}

	public String getTermDfCfDirectory() {
		return appendPath(mIndexPath, "term-df-cf/");
	}

	/**
	 * Returns file that contains the list of terms in the collection. The file
	 * consists of a stream of bytes, read into an array, representing the
	 * terms, which are alphabetically sorted and prefix-coded.
	 */
	public String getIndexTermsData() {
		return appendPath(mIndexPath, "index-terms.dat");
	}

	/**
	 * Returns file that contains term ids sorted by the alphabetical order of
	 * terms. The file consists of a stream of ints, read into an array. The
	 * array index position corresponds to alphabetical sort order of the term.
	 * This is used to retrieve the term id of a term given its position in the
	 * {@link #getIndexTermsData()} data.
	 */
	public String getIndexTermIdsData() {
		return appendPath(mIndexPath, "index-termids.dat");
	}

	/**
	 * Returns file that contains an index of term ids into the array of terms.
	 * The file consists of a stream of ints, read into an array. The array
	 * index position corresponds to the term ids. This is used to retrieve the
	 * index position into the {@link #getIndexTermsData()} file to map from
	 * term ids back to terms.
	 */
	public String getIndexTermIdMappingData() {
		return appendPath(mIndexPath, "index-termid-mapping.dat");
	}

	/**
	 * Returns file that contains a list of document frequencies sorted by the
	 * alphabetical order of terms. The file consists of a stream of ints, read
	 * into an array. The array index position corresponds to alphabetical sort
	 * order of the term. This is used to retrieve the df of a term given its
	 * position in the {@link #getIndexTermsData()} data.
	 */
	public String getDfByTermData() {
		return appendPath(mIndexPath, "df-by-term.dat");
	}

	/**
	 * Returns file that contains a list of document frequencies sorted by term
	 * id. The file consists of a stream of ints, read into an array. The array
	 * index position corresponds to the term id. Note that the term ids are
	 * assigned in descending order of document frequency. This is used to
	 * retrieve the df of a term given its term id.
	 */
	public String getDfByIntData() {
		return appendPath(mIndexPath, "df-by-int.dat");
	}

	/**
	 * Returns file that contains a list of collection frequencies sorted by the
	 * alphabetical order of terms. The file consists of a stream of ints, read
	 * into an array. The array index position corresponds to alphabetical sort
	 * order of the term. This is used to retrieve the cf of a term given its
	 * position in the {@link #getIndexTermsData()} data.
	 */
	public String getCfByTermData() {
		return appendPath(mIndexPath, "cf-by-term.dat");
	}

	/**
	 * Returns file that contains a list of collection frequencies sorted by
	 * term id. The file consists of a stream of ints, read into an array. The
	 * array index position corresponds to the term id. This is used to retrieve
	 * the cf of a term given its term id.
	 */
	public String getCfByIntData() {
		return appendPath(mIndexPath, "cf-by-int.dat");
	}

	/**
	 * Returns file that contains an index into the postings. This file serves
	 * as input to {@link IntPostingsForwardIndex}, which provides random access to
	 * postings lists.
	 */
	public String getPostingsIndexData() {
		return appendPath(mIndexPath, "postings-index.dat");
	}

	public String getTempDirectory() {
		return appendPath(mIndexPath, "tmp" + r.nextInt(10000));
	}

	/**
	 * Returns the name of the collection. This value is read from a property
	 * file stored in the index.
	 */
	public String readCollectionName() {
		return FSProperty.readString(mFs, appendPath(mIndexPath, "property.CollectionName"));
	}

	/**
	 * Sets the name of the collection. This value is persisted to a property
	 * file stored in the index.
	 */
	public void writeCollectionName(String s) {
		FSProperty.writeString(mFs, appendPath(mIndexPath, "property.CollectionName"), s);
	}

	public String readCollectionPath() {
		return FSProperty.readString(mFs, appendPath(mIndexPath, "property.CollectionPath"));
	}

	public void writeCollectionPath(String s) {
		FSProperty.writeString(mFs, appendPath(mIndexPath, "property.CollectionPath"), s);
	}

	/**
	 * Returns the number of documents in the collection. This value is read
	 * from a property file stored in the index.
	 */
	public int readCollectionDocumentCount() {
		return FSProperty.readInt(mFs, appendPath(mIndexPath, "property.CollectionDocumentCount"));
	}

	/**
	 * Sets the number of documents in the collection. This value is persisted
	 * to a property file stored in the index.
	 */
	public void writeCollectionDocumentCount(int n) {
		FSProperty.writeInt(mFs, appendPath(mIndexPath, "property.CollectionDocumentCount"), n);
	}

	/**
	 * Returns the average document length of the collection. This value is read
	 * from a property file stored in the index.
	 */
	public float readCollectionAverageDocumentLength() {
		return FSProperty.readFloat(mFs, appendPath(mIndexPath,
				"property.CollectionAverageDocumentLength"));
	}

	/**
	 * Sets the average document length of the collection. This value is
	 * persisted to a property file stored in the index.
	 */
	public void writeCollectionAverageDocumentLength(float n) {
		FSProperty.writeFloat(mFs, appendPath(mIndexPath,
				"property.CollectionAverageDocumentLength"), n);
	}

	public int readCollectionTermCount() {
		return FSProperty.readInt(mFs, appendPath(mIndexPath, "property.CollectionTermCount"));
	}

	public void writeCollectionTermCount(int cnt) {
		FSProperty.writeInt(mFs, appendPath(mIndexPath, "property.CollectionTermCount"), cnt);
	}

	public long readCollectionLength() {
		return FSProperty.readLong(mFs, appendPath(mIndexPath, "property.CollectionLength"));
	}

	public void writeCollectionLength(long cnt) {
		FSProperty.writeLong(mFs, appendPath(mIndexPath, "property.CollectionLength"), cnt);
	}

	public String readInputFormat() {
		return FSProperty.readString(mFs, appendPath(mIndexPath, "property.InputFormat"));
	}

	public void writeInputFormat(String s) {
		FSProperty.writeString(mFs, appendPath(mIndexPath, "property.InputFormat"), s);
	}

	public String readTokenizerClass() {
		return FSProperty.readString(mFs, appendPath(mIndexPath, "property.Tokenizer"));
	}

	public void writeTokenizerClass(String s) {
		FSProperty.writeString(mFs, appendPath(mIndexPath, "property.Tokenizer"), s);
	}

	public String readDocnoMappingClass() {
		return FSProperty.readString(mFs, appendPath(mIndexPath, "property.DocnoMappingClass"));
	}

	public void writeDocnoMappingClass(String s) {
		FSProperty.writeString(mFs, appendPath(mIndexPath, "property.DocnoMappingClass"), s);
	}

	public int readDocnoOffset() {
		return FSProperty.readInt(mFs, appendPath(mIndexPath, "property.DocnoOffset"));
	}

	public void writeDocnoOffset(int n) {
		FSProperty.writeInt(mFs, appendPath(mIndexPath, "property.DocnoOffset"), n);
	}

	public String readPostingsType() {
		return FSProperty.readString(mFs, appendPath(mIndexPath, "property.PostingsType"));
	}

	public void writePostingsType(String type) {
		FSProperty.writeString(mFs, appendPath(mIndexPath, "property.PostingsType"), type);
	}

	private static void testTerm(RetrievalEnvironment env, String term) {
		long startTime = System.currentTimeMillis();

		PostingsReader reader = null;
		Posting p = new Posting();
		int df = 0;
		String termOrig = term;
		String termTokenized = env.tokenize(termOrig)[0];

		LOGGER.info("term=" + termOrig + ", tokenized=" + termTokenized);
		reader = env.getPostingsReader(termTokenized);
		df = reader.getNumberOfPostings();
		LOGGER.info("First ten postings: ");
		for (int i = 0; i < (df < 10 ? df : 10); i++) {
			reader.nextPosting(p);
			System.out.print(p);
		}

		System.out.println("\n");

		long endTime = System.currentTimeMillis();

		System.out.println("total time: " + (endTime - startTime) + "ms");
	}

	public static DocnoMapping loadDocnoMapping(String indexPath, FileSystem fs) {
		DocnoMapping mDocMapping = null;
		// load the docid to docno mappings
		try {
			LOGGER.info("Loading DocnoMapping file ...");
			RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

			String className = env.readDocnoMappingClass();
			LOGGER.info(" Class name: " + className);
			mDocMapping = (DocnoMapping) Class.forName(className).newInstance();

			String mappingFile = env.getDocnoMappingData();
			LOGGER.info(" File name: " + mappingFile);
			mDocMapping.loadMapping(new Path(mappingFile), fs);
			LOGGER.info("Loading Done.");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing DocnoMapping!");
		}
		return mDocMapping;
	}

	public static DocnoMapping loadDocnoMapping(String indexPath) {
		try {
			return loadDocnoMapping(indexPath, FileSystem.get(new Configuration()));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing DocnoMapping!");
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: [index-path]");
			System.exit(-1);
		}

		long startingMemoryUse = MemoryUsageUtils.getUsedMemory();

		String indexPath = args[0];
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, FileSystem
				.get(new Configuration()));
		env.initialize(false);

		long endingMemoryUse = MemoryUsageUtils.getUsedMemory();

		System.out.println("Memory usage: " + (endingMemoryUse - startingMemoryUse) + " bytes\n");

		String term = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up postings of term> ");
		while ((term = stdin.readLine()) != null) {
			testTerm(env, term);
			System.out.print("Look up postings of term> ");
		}

	}
}
