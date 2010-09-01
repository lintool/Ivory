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
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
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
	 * @param indexPath
	 */
	public RetrievalEnvironment(String indexPath, boolean loadDoclengths) throws IOException {
		// hadoop variables
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// get number of documents
		mDocumentCount = readCollectionDocumentCount(fs, indexPath);

		// If property.CollectionDocumentCount.local exists, it means that this
		// index is a partition of a large document collection. We want to use
		// the global doc count for score, but the Golomb compression is
		// determined using the local doc count.
		if (fs.exists(new Path(indexPath + "/property.CollectionDocumentCount.local"))) {
			mDocumentCountLocal = FSProperty.readInt(fs, indexPath
					+ "/property.CollectionDocumentCount.local");
		}

		// get vocabulary size
		mTermCount = readCollectionTermCount(fs, indexPath);

		// get the posting type
		mPostingsType = readPostingsType(fs, indexPath);

		// read the table of doc lengths
		if (loadDoclengths) {
			LOGGER.info("Loading doclengths table...");
			mDocumentLengths = new DocLengthTable(getDoclengthsFile(indexPath), fs);
		}

		// read document frequencies
		mDefaultDf = mDocumentCount / 100; // heuristic

		// read collection frequencies
		mDefaultCf = mDefaultDf * 2; // heuristic

		LOGGER.info("IndexPath: " + indexPath);
		LOGGER.info("PostingsType: " + mPostingsType);
		LOGGER.info("Collection document count: " + mDocumentCount);
		LOGGER.info("Collection term count: " + mTermCount);

		// initialize the tokenizer; this information is stored along with the
		// index since we need to use the same tokenizer to parse queries
		try {
			String tokenizer = readTokenizerClass(fs, indexPath);
			LOGGER.info("Tokenizer: " + tokenizer);
			mTokenizer = (Tokenizer) Class.forName(tokenizer).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		LOGGER.info("Loading postings index...");
		String termsFile = RetrievalEnvironment.getPostingsIndexTerms(indexPath);
		String positionsFile = RetrievalEnvironment.getPostingsIndexPositions(indexPath);
		String postingsPath = RetrievalEnvironment.getPostingsDirectory(indexPath);
		int termCnt = RetrievalEnvironment.readPostingsIndexTermCount(fs, indexPath);

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
					PostingsList list = getPostingsList(terms[i]);
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
					PostingsList list = getPostingsList(terms[i]);
					readers
							.add((PostingsListDocSortedPositional.PostingsReader) mPostingsReaderConstructor
									.newInstance(list.getRawBytes(), list.getNumberOfPostings(),
											mDocumentLengths.getDocCount(), list));
				}

				postings = new ProximityPostingsListUnorderedWindow(readers
						.toArray(new PostingsListDocSortedPositional.PostingsReader[0]), windowSize);
			} else {
				PostingsList postingsList = getPostingsList(expression);

				// if we couldn't get the postings list, then just return null
				if (postingsList == null) {
					return null;
				}

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
	private PostingsList getPostingsList(String term) throws IOException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		

		long start = System.currentTimeMillis();
		PostingsList value = mTermPostingsIndex.getTermPosting(term);
		long duration = System.currentTimeMillis() - start;

		if(value == null) return null;
		
		if (mDocumentCountLocal != -1) {
			value.setCollectionDocumentCount(mDocumentCountLocal);
		} else {
			value.setCollectionDocumentCount(mDocumentCount);
		}

		LOGGER.info("fetched postings for term \"" + term + "\" in " + duration + " ms");
		LOGGER.info("CF: "+value.getCf()+"\tDF: "+value.getDf()+"\tnDocsInColl: "+value.getCollectionDocumentCount()+"\t nPostings: "+value.getNumberOfPostings());
		
		/*if (!key.toString().equals(term)) {
			LOGGER.error("unable to fetch postings for term \"" + term + "\": found key \"" + key
					+ "\" instead");
			return null;
			// LOGGER.info("Getting posting list of: "+key.toString());
			// mTermPostingsIndex.getTermPosting(key.toString(), key, value);
		}*/

		return value;
	}

	/**
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	/*private PostingsList getPostingsList(String term) throws IOException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		Text key = new Text();
		PostingsList value = new PostingsListDocSortedPositional();

		long start = System.currentTimeMillis();
		mTermPostingsIndex.getTermPosting(term, key, value);
		long duration = System.currentTimeMillis() - start;

		if (mDocumentCountLocal != -1) {
			value.setCollectionDocumentCount(mDocumentCountLocal);
		} else {
			value.setCollectionDocumentCount(mDocumentCount);
		}

		LOGGER.info("fetched postings for term \"" + key + "\" in " + duration + " ms");
		if (!key.toString().equals(term)) {
			LOGGER.error("unable to fetch postings for term \"" + term + "\": found key \"" + key
					+ "\" instead");
			return null;
			// LOGGER.info("Getting posting list of: "+key.toString());
			// mTermPostingsIndex.getTermPosting(key.toString(), key, value);
		}

		return value;
	}*/

	public long collectionFrequency(String expression) throws Exception {
		// TODO: fix this
		// we currently don't support cf for proximity expressions
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

	public int documentFrequency(String expression) throws Exception {
		// TODO: fix this
		// we currently don't support df for proximity expressions
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

	public String[] tokenize(String text) {
		return mTokenizer.processContent(text);
	}

	public int getDefaultDF() {
		return mDefaultDf;
	}

	public long getDefaultCF() {
		return mDefaultCf;
	}

	private static Random r = new Random();

	private static String appendPath(String base, String file) {
		return base + (base.endsWith("/") ? "" : "/") + file;
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

	public static void testTerm(RetrievalEnvironment env, String term) {
		long startTime = System.currentTimeMillis();

		PostingsReader reader = null;
		Posting p = new Posting();
		int df = 0;
		String termOrig = term;
		String termTokenized = env.tokenize(termOrig)[0];

		reader = env.getPostingsReader(termTokenized);
		df = reader.getNumberOfPostings();
		System.out.print(termOrig + ", tokenized=" + termTokenized + ", df=" + df
				+ "\n First ten postings: ");
		for (int i = 0; i < (df < 10 ? df : 10); i++) {
			reader.nextPosting(p);
			System.out.print(p);
		}

		System.out.println("\n");

		long endTime = System.currentTimeMillis();

		System.out.println("total time: " + (endTime - startTime));
	}
	
	public static DocnoMapping loadDocnoMapping(String indexPath){
		DocnoMapping mDocMapping = null;
		// load the docid to docno mappings
		try {
			LOGGER.info("Loading DocnoMapping file ...");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			String className = RetrievalEnvironment.readDocnoMappingClass(fs, indexPath);
			LOGGER.info(" Class name: "+className);
			mDocMapping = (DocnoMapping) Class.forName(className).newInstance();
	
			String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);
			LOGGER.info(" File name: "+mappingFile);
			mDocMapping.loadMapping(new Path(mappingFile), fs);
			LOGGER.info("Loading Done.");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error initializing DocnoMapping!");
		}
		return mDocMapping;
	}


	public static void main(String[] args) throws Exception {
		// String indexPath = "/umd/indexes/trec45noCRFR.positional.galago/";
		// String indexPath = "/umd/indexes/clue.en.segment.01/";
		// String indexPath = "/fs/clip-qa/clue.en.segment.01.stable/";
		String indexPath = "/umd/indexes/medline04.positional/";
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, false);
		testTerm(env, "jim");
		// testTerm(env, "test");
		// testTerm(env, "usa");
		// testTerm(env, "general");
		// testTerm(env, "ice");
		// testTerm(env, "back");
		testTerm(env, "egypt");
		// testTerm(env, "iraq");
		// testTerm(env, "turkey");
		// testTerm(env, "best");

	}
}
