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
import ivory.data.PostingsReader;
import ivory.data.ProximityPostingsReaderOrderedWindow;
import ivory.data.ProximityPostingsReaderUnorderedWindow;
import ivory.data.TermDocVector;
import ivory.data.TermDocVectorsForwardIndex;
import ivory.data.TermIdMapWithCache;
import ivory.smrf.model.builder.Expression;
import ivory.smrf.model.importance.ConceptImportanceModel;
import ivory.tokenize.Tokenizer;

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

	private static final Logger LOG = Logger.getLogger(RetrievalEnvironment.class);

	// postings reader cache
	Map<String, PostingsReader> mPostingsReaderCache = null;

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

	private TermIdMapWithCache mTermIdMap;

	private FileSystem mFs;
	private String mIndexPath;

	private IntDocVectorsForwardIndex mIntDocVectorsIndex;

	/**
	 * (globally-defined) query-independent document priors
	 */
	private final Map<String, DocScoreTable> mDocScores = new HashMap<String, DocScoreTable>();

	/**
	 * (globally-defined) concept importance models
	 */
	private final Map<String, ConceptImportanceModel> mImportanceModels = new HashMap<String, ConceptImportanceModel>();
	
	public RetrievalEnvironment(String indexPath, FileSystem fs) throws IOException {
		if (!fs.exists(new Path(indexPath))) {
			throw new IOException("Index path " + indexPath + " doesn't exist!");
		}

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
			LOG.info("Loading doclengths table...");
			mDocumentLengths = new DocLengthTable2B(getDoclengthsData(), mFs);
		}

		// read document frequencies
		mDefaultDf = mDocumentCount / 100; // heuristic

		// read collection frequencies
		mDefaultCf = mDefaultDf * 2; // heuristic

		LOG.info("IndexPath: " + mIndexPath);
		LOG.info("PostingsType: " + mPostingsType);
		LOG.info("Collection document count: " + mDocumentCount);
		LOG.info("Collection length: " + mCollectionLength);

		// initialize the tokenizer; this information is stored along with the
		// index since we need to use the same tokenizer to parse queries
		try {
			String tokenizer = readTokenizerClass();

			if (tokenizer.startsWith("ivory.util.GalagoTokenizer")) {
				LOG
						.warn("Warning: GalagoTokenizer has been refactored to ivory.tokenize.GalagoTokenizer!");
				tokenizer = "ivory.tokenize.GalagoTokenizer";
			}

			LOG.info("Tokenizer: " + tokenizer);
			mTokenizer = (Tokenizer) Class.forName(tokenizer).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		LOG.info("Loading postings index...");

		long termCnt = readCollectionTermCount();

		LOG.info("Number of terms: " + termCnt);
		mTermPostingsIndex = new IntPostingsForwardIndex(mIndexPath, mFs);
		LOG.info("Done!");

		try {
			mTermIdMap = new TermIdMapWithCache(new Path(getIndexTermsData()),
					new Path(getIndexTermIdsData()), new Path(getIndexTermIdMappingData()), 0.2f,
					mFs);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing Term to Id map!");
		}

		try {
			mIntDocVectorsIndex = new IntDocVectorsForwardIndex(mIndexPath, mFs);
		} catch (Exception e) {
			LOG
					.warn("Unable to load IntDocVectorsForwardIndex: relevance feedback will not be available.");
		}

		mPostingsReaderCache = new HashMap<String, PostingsReader>();
	}

	@SuppressWarnings("unchecked")
	public void loadDocScore(String type, String provider, String path) {
		// LOGGER.setLevel(Level.ALL);
		LOG.info("Loading doc scores of type: " + type + ", from: " + path + ", prvider: "
				+ provider);
		try {
			Class<? extends DocScoreTable> clz = (Class<? extends DocScoreTable>) Class
					.forName(provider);
			DocScoreTable s = clz.newInstance();
			s.initialize(path, mFs);
			mDocScores.put(type, s);
			LOG.info(s.getDocCount() + ", " + s.getDocnoOffset());
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
		LOG.info("Loading done.");
	}

	public float getDocScore(String type, int docno) {
		if (!mDocScores.containsKey(type))
			throw new RuntimeException("Error: docscore type \"" + type + "\" not found!");

		return mDocScores.get(type).getScore(docno);
	}

	public void addImportanceModel(String key, ConceptImportanceModel m) {
		mImportanceModels.put(key, m);
	}

	public ConceptImportanceModel getImportanceModel(String id) {
		return mImportanceModels.get(id);
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

	public PostingsReader getPostingsReader(Expression exp) {
		PostingsReader postings = null;
		try {
			if (exp.getType().equals(Expression.Type.OD)) {
				int gapSize = exp.getWindow();
				String[] terms = exp.getTerms();

				List<PostingsReader> readers = new ArrayList<PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsReader reader = constructPostingsReader(terms[i]);
					readers.add(reader);
				}

				postings = new ProximityPostingsReaderOrderedWindow(readers
						.toArray(new PostingsReader[0]), gapSize);
			} else if (exp.getType().equals(Expression.Type.UW)) {
				int windowSize = exp.getWindow();
				String[] terms = exp.getTerms();

				List<PostingsReader> readers = new ArrayList<PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsReader reader = constructPostingsReader(terms[i]);
					readers.add(reader);
				}

				postings = new ProximityPostingsReaderUnorderedWindow(readers
						.toArray(new PostingsReader[0]), windowSize);
			} else {
				postings = constructPostingsReader(exp.getTerms()[0]);
			}
		} catch (Exception e) {
			LOG.error("Unable to initialize PostingsReader!");

			e.printStackTrace();
		}

		return postings;
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

				List<PostingsReader> readers = new ArrayList<PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsReader reader = constructPostingsReader(terms[i]);
					readers.add(reader);
				}

				postings = new ProximityPostingsReaderOrderedWindow(readers
						.toArray(new PostingsReader[0]), gapSize);
			} else if (expression.startsWith("#uw")) {
				int windowSize = Integer.parseInt(expression.substring(3, expression.indexOf('(')));
				expression = expression.substring(expression.indexOf('(') + 1,
						expression.indexOf(')')).trim();
				String[] terms = expression.split(" ");

				List<PostingsReader> readers = new ArrayList<PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsReader reader = constructPostingsReader(terms[i]);
					readers.add(reader);
				}

				postings = new ProximityPostingsReaderUnorderedWindow(readers
						.toArray(new PostingsReader[0]), windowSize);
			} else {
				postings = constructPostingsReader(expression);
			}
		} catch (Exception e) {
			LOG.error("Unable to initialize PostingsReader!");

			e.printStackTrace();
		}

		return postings;
	}

	private PostingsReader constructPostingsReader(String expression) throws Exception {
		PostingsReader reader = null;

		if (mPostingsReaderCache != null) {
			reader = mPostingsReaderCache.get(expression);
		}

		if (reader == null) {
			PostingsList list = getPostingsList(expression);
			if (list == null) {
				return null;
			}

			reader = (PostingsReader) list.getPostingsReader();

			if (mPostingsReaderCache != null) {
				mPostingsReaderCache.put(expression, reader);
			}
		}

		return reader;
	}

	public void clearPostingsReaderCache() {
		mPostingsReaderCache.clear();
	}

	private PostingsList getPostingsList(String term) {

		int termid = mTermIdMap.getID(term);

		if (termid == -1) {
			LOG.error("couldn't find term id for term \"" + term + "\"");
			return null;
		}

		PostingsList value;
		try {
			value = mTermPostingsIndex.getPostingsList(termid);

			if (value == null) {
				LOG.error("couldn't find PostingsList for term \"" + term + "\"");
				return null;
			}
		} catch (IOException e) {
			LOG.error("couldn't find PostingsList for term \"" + term + "\"");
			return null;
		}

		if (mDocumentCountLocal != -1) {
			value.setCollectionDocumentCount(mDocumentCountLocal);
		} else {
			value.setCollectionDocumentCount(mDocumentCount);
		}

		return value;
	}

	public IntDocVector[] documentVectors(int[] docSet) {
		IntDocVector[] dvs = new IntDocVector[docSet.length];

		try {
			for (int i = 0; i < docSet.length; i++) {
				dvs[i] = mIntDocVectorsIndex.getDocVector(docSet[i]);
			}
		} catch (IOException e) {
			LOG.error("Unable to retrieve document vectors!");
			return null;
		}

		return dvs;
	}

	/**
	 * Returns the collection frequency of a particular expression.
	 */
	public long collectionFrequency(String expression) {
		// Heuristic: we currently don't support cf for proximity expressions.
		if (expression.startsWith("#od")) {
			return mDefaultCf;
		} else if (expression.startsWith("#uw")) {
			return mDefaultCf;
		}

		try {
			return getPostingsList(expression).getCf();
		} catch (Exception e) {
			LOG.error("Unable to get cf for " + expression);
			return 0;
		}
	}

	/**
	 * Returns the document frequency of a particular expression.
	 */
	public int documentFrequency(String expression) {
		// Heuristic: we currently don't support df for proximity expressions.
		if (expression.startsWith("#od")) {
			return mDefaultDf;
		} else if (expression.startsWith("#uw")) {
			return mDefaultDf;
		}

		try {
			return getPostingsList(expression).getDf();
		} catch (Exception e) {
			LOG.error("Unable to get cf for " + expression);
			return 0;
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
	 * {@link IntDocVectorsForwardIndex}, which provides random access to the
	 * document
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

	/**
	 * Returns file that contains an index into the {@link WeightedIntDocVector}
	 * representation of the collection. This file serves as input to
	 * {@link WeightedIntDocVectorsForwardIndex}, which provides random access to the
	 * document
	 * vectors.
	 */
	public String getWeightedIntDocVectorsForwardIndex() {
		return appendPath(mIndexPath, "wt-int-doc-vectors-forward-index.dat");
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
	 * as input to {@link IntPostingsForwardIndex}, which provides random access
	 * to
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

		LOG.info("term=" + termOrig + ", tokenized=" + termTokenized);
		reader = env.getPostingsReader(termTokenized);
		df = reader.getNumberOfPostings();
		LOG.info("First ten postings: ");
		for (int i = 0; i < (df < 10 ? df : 10); i++) {
			reader.nextPosting(p);
			System.out.print(p);
		}

		System.out.println("\n");

		long endTime = System.currentTimeMillis();

		System.out.println("total time: " + (endTime - startTime) + "ms");
	}

	public DocnoMapping getDocnoMapping() throws IOException {
		return loadDocnoMapping(mIndexPath, mFs);
	}

	public static DocnoMapping loadDocnoMapping(String indexPath, FileSystem fs) throws IOException {
		DocnoMapping mDocMapping = null;
		// load the docid to docno mappings
		try {
			LOG.info("Loading DocnoMapping file ...");
			RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

			String className = env.readDocnoMappingClass();
			LOG.info(" Class name: " + className);
			mDocMapping = (DocnoMapping) Class.forName(className).newInstance();

			String mappingFile = env.getDocnoMappingData();
			LOG.info(" File name: " + mappingFile);
			mDocMapping.loadMapping(new Path(mappingFile), fs);
			LOG.info("Loading Done.");
		} catch (Exception e) {
			throw new IOException("Error initializing DocnoMapping!");
		}
		return mDocMapping;
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
