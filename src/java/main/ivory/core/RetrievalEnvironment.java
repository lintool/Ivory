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

package ivory.core;

import ivory.core.data.dictionary.DefaultFrequencySortedDictionary;
import ivory.core.data.document.IntDocVector;
import ivory.core.data.document.IntDocVectorsForwardIndex;
import ivory.core.data.document.TermDocVector;
import ivory.core.data.document.TermDocVectorsForwardIndex;
import ivory.core.data.index.IntPostingsForwardIndex;
import ivory.core.data.index.Posting;
import ivory.core.data.index.PostingsList;
import ivory.core.data.index.PostingsReader;
import ivory.core.data.index.ProximityPostingsReaderOrderedWindow;
import ivory.core.data.index.ProximityPostingsReaderUnorderedWindow;
import ivory.core.data.stat.DocLengthTable;
import ivory.core.data.stat.DocLengthTable2B;
import ivory.core.data.stat.DocScoreTable;
import ivory.core.tokenize.Tokenizer;
import ivory.smrf.model.builder.Expression;
import ivory.smrf.model.importance.ConceptImportanceModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.debug.MemoryUsageUtils;
import edu.umd.cloud9.io.FSProperty;

/**
 * @author Don Metzler
 * @author Jimmy Lin
 * 
 */
public class RetrievalEnvironment {
	private static final Logger LOG = Logger.getLogger(RetrievalEnvironment.class);

	// Postings reader cache.
	private final Map<String, PostingsReader> mPostingsReaderCache = new HashMap<String, PostingsReader>(); 	

	protected int numDocs;                   // Number of documents in collection.
	protected int numDocsLocal = -1;
	protected long collectionSize;           // Number of terms in the collection.

	public static int defaultDf;                 // Default df value.
	public static long defaultCf;                // Default cf value.

	protected String postingsType;           // Type of postings in the index.
	protected DocLengthTable doclengths;     // Document length lookup.
	protected Tokenizer tokenizer;           // Tokenizer for parsing queries.
	protected DefaultFrequencySortedDictionary termidMap;  // Mapping from terms to term ids.

	protected IntPostingsForwardIndex postingsIndex;     // Forward index into postings.
	protected IntDocVectorsForwardIndex docvectorsIndex; // Forward index into int doc vectors.

	protected final FileSystem fs;
	protected final String indexPath;

	// Globally-defined query-independent document priors.
	private final Map<String, DocScoreTable> docScores = new HashMap<String, DocScoreTable>();

	// Globally-defined concept importance models.
	private final Map<String, ConceptImportanceModel> importanceModels = new HashMap<String, ConceptImportanceModel>();

	// These are for the cascade.
	public static int topK;
	public static boolean mIsNewModel;
	public static String dataCollection;
	public static int documentCount;

	public RetrievalEnvironment(String indexPath, FileSystem fs) throws IOException {
		if (!fs.exists(new Path(indexPath))) {
			throw new IOException("Index path " + indexPath + " doesn't exist!");
		}

		this.indexPath = indexPath;
		this.fs = fs;
	}

	
	public void initialize(boolean loadDoclengths) throws IOException, ConfigurationException {
		LOG.info("Initializing index at " + indexPath);

		// Suppress verbose output.
		Logger.getLogger(DocLengthTable2B.class).setLevel(Level.WARN);

		// get number of documents
		numDocs = readCollectionDocumentCount();
		collectionSize = readCollectionLength();
		postingsType = readPostingsType();
		//postingsType = "ivory.data.PostingsListDocSortedPositional";

		LOG.info("PostingsType: " + postingsType);
		LOG.info("Collection document count: " + numDocs);
		LOG.info("Collection length: " + collectionSize);

		// If property.CollectionDocumentCount.local exists, it means that this
		// index is a partition of a large document collection. We want to use
		// the global doc count for score, but the Golomb compression is
		// determined using the local doc count.
		if (fs.exists(new Path(indexPath + "/property.CollectionDocumentCount.local"))) {
			numDocsLocal = FSProperty.readInt(fs, indexPath + "/property.CollectionDocumentCount.local");
		}

		defaultDf = numDocs / 100; // Heuristic!
		defaultCf = defaultDf * 2; // Heuristic!

		// Initialize the tokenizer; this information is stored along with the
		// index since we need to use the same tokenizer to parse queries.
		try {
			String tokenizerClassName = readTokenizerClass();
			if (tokenizerClassName.startsWith("ivory.util.GalagoTokenizer")) {
				LOG.warn("Warning: GalagoTokenizer has been refactored to ivory.core.tokenize.GalagoTokenizer!");
				tokenizerClassName = "ivory.core.tokenize.GalagoTokenizer";
			} else if (tokenizerClassName.startsWith("ivory.tokenize.GalagoTokenizer")) {
        LOG.warn("Warning: GalagoTokenizer has been refactored to ivory.core.tokenize.GalagoTokenizer!");
        tokenizerClassName = "ivory.core.tokenize.GalagoTokenizer";
      }

			LOG.info("Tokenizer: " + tokenizerClassName);
			tokenizer = (Tokenizer) Class.forName(tokenizerClassName).newInstance();
		} catch (Exception e) {
			throw new ConfigurationException("Error initializing tokenizer!");
		}

		LOG.info("Loading postings index...");
		postingsIndex = new IntPostingsForwardIndex(indexPath, fs);
		LOG.info(" - Number of terms: " + readCollectionTermCount());
		LOG.info("Done!");

		try {
			termidMap = new DefaultFrequencySortedDictionary(new Path(getIndexTermsData()),
			    new Path(getIndexTermIdsData()), new Path(getIndexTermIdMappingData()), fs);
		} catch (Exception e) {
			throw new ConfigurationException("Error initializing dictionary!");
		}

		try {
			docvectorsIndex = new IntDocVectorsForwardIndex(indexPath, fs);
		} catch (Exception e) {
			LOG.warn("Unable to load IntDocVectorsForwardIndex: relevance feedback will not be available.");
		}

		// Read the table of doc lengths.
		if (loadDoclengths) {
			LOG.info("Loading doclengths table...");
			doclengths = new DocLengthTable2B(getDoclengthsData(), fs);
			LOG.info(" - Number of docs: " + doclengths.getDocCount());
			LOG.info(" - Avg. doc length: " + doclengths.getAvgDocLength());
			LOG.info("Done!");
		}
	}

	@SuppressWarnings("unchecked")
	public void loadDocScore(String type, String provider, String path) {
		LOG.info("Loading doc scores of type: " + type + ", from: " + path + ", provider: " + provider);
		try {
			Class<? extends DocScoreTable> clz = (Class<? extends DocScoreTable>) Class.forName(provider);
			DocScoreTable s = clz.newInstance();
			s.initialize(path, fs);
			docScores.put(type, s);
			LOG.info(s.getDocCount() + ", " + s.getDocnoOffset());
		} catch (Exception e) {
			throw new RuntimeException("Erorr reading doc scores!", e);
		}
		LOG.info("Loading done.");
	}

	public float getDocScore(String type, int docno) {
		if (!docScores.containsKey(type)) {
			throw new RuntimeException("Error: docscore type \"" + type + "\" not found!");
		}
		return docScores.get(type).getScore(docno);
	}

	public static void setIsNew(boolean isNewModel){
	  mIsNewModel = isNewModel;
	}

	public void addImportanceModel(String key, ConceptImportanceModel m) {
		importanceModels.put(key, m);
	}

	public ConceptImportanceModel getImportanceModel(String id) {
		return importanceModels.get(id);
	}

	public Collection<ConceptImportanceModel> getImportanceModels() {
		return importanceModels.values();
	}

	public long getDocumentCount() {
		return numDocs;
	}

	public int getDocumentLength(int docid) {
		return doclengths.getDocLength(docid);
	}

	public long getCollectionSize() {
		return collectionSize;
	}

	public PostingsReader getPostingsReader(Expression exp) {
		//LOG.info("**getPostingsReader("+exp+")");;
		PostingsReader postingsReader = null;
		try {
			if (exp.getType().equals(Expression.Type.OD)) {
				int gapSize = exp.getWindow();
				String[] terms = exp.getTerms();

				List<PostingsReader> readers = new ArrayList<PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsReader reader = constructPostingsReader(terms[i]);
					if(reader != null)
						readers.add(reader);
				}

				postingsReader = new ProximityPostingsReaderOrderedWindow(readers.toArray(new PostingsReader[0]), gapSize);
			} else if (exp.getType().equals(Expression.Type.UW)) {
				int windowSize = exp.getWindow();
				String[] terms = exp.getTerms();

				List<PostingsReader> readers = new ArrayList<PostingsReader>();
				for (int i = 0; i < terms.length; i++) {
					PostingsReader reader = constructPostingsReader(terms[i]);
					if(reader != null)
						readers.add(reader);
				}

				postingsReader = new ProximityPostingsReaderUnorderedWindow(readers.toArray(new PostingsReader[0]), windowSize);
			} else {
				postingsReader = constructPostingsReader(exp.getTerms()[0]);
			}
		} catch (Exception e) {
			throw new RuntimeException("Unable to initialize PostingsReader!", e);
		}

		return postingsReader;
	}

	protected PostingsReader constructPostingsReader(String expression) throws Exception {
		//LOG.info("**constructPostingsReader("+expression+")");
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

	public PostingsList getPostingsList(String term) {
		//LOG.info("**getPostingsList("+term+")");
		int termid = termidMap.getId(term);

		if (termid == -1) {
			LOG.error("couldn't find term id (-1) for term \"" + term + "\"");
			return null;
		}
		if (termid == 0) {
			LOG.error("couldn't find term id (0) for term \"" + term + "\"");
			return null;
		}
		//LOG.info("termid: "+termid);

		PostingsList value;
		try {
			value = postingsIndex.getPostingsList(termid);

			if (value == null) {
				LOG.error("[1] couldn't find PostingsList for term \"" + term + "\"");
				return null;
			}
		} catch (IOException e) {
			LOG.error("[2] couldn't find PostingsList for term \"" + term + "\"");
			return null;
		}

		if (numDocsLocal != -1) {
			value.setCollectionDocumentCount(numDocsLocal);
		} else {
			value.setCollectionDocumentCount(numDocs);
		}

		return value;
	}

	public IntDocVector[] documentVectors(int[] docSet) {
		IntDocVector[] dvs = new IntDocVector[docSet.length];

		try {
			for (int i = 0; i < docSet.length; i++) {
				dvs[i] = docvectorsIndex.getDocVector(docSet[i]);
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
			return defaultCf;
		} else if (expression.startsWith("#uw")) {
			return defaultCf;
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
			return defaultDf;
		} else if (expression.startsWith("#uw")) {
			return defaultDf;
		}

		try {
			return getPostingsList(expression).getDf();
		} catch (Exception e) {
			LOG.error("Unable to get cf for " + expression);
			return 0;
		}
	}

	public String getTermFromId(int termid) {
		return termidMap.getTerm(termid);
	}

  public int getIdFromTerm(String term) {
    return termidMap.getId(term);
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
		return tokenizer.processContent(text);
	}

	/**
	 * Returns the default document frequency.
	 */
	public int getDefaultDf() {
		return defaultDf;
	}

	/**
	 * Returns the default collection frequency.
	 */
	public long getDefaultCf() {
		return defaultCf;
	}

	private static Random r = new Random();

	// TODO: Change this method to return Path object instead.
	public static String appendPath(String base, String file) {
		return base + (base.endsWith("/") ? "" : "/") + file;
	}

	public static Path createPath(String base, String file) {
		return new Path(base + (base.endsWith("/") ? "" : "/") + file);
	}

	public Path getDocnoMappingData() {
		return createPath(indexPath, "docno-mapping.dat");
	}

	
	public Path getDocnoMappingDirectory() {
		return createPath(indexPath, "docno-mapping/");
	}

	/**
	 * Returns file that contains the document length data. This file serves as
	 * input to {@link DocLengthTable}, which provides random access to
	 * document lengths.
	 */
	public Path getDoclengthsData() {
		return createPath(indexPath, "doclengths.dat");
	}
	
	/**
	 * Returns directory that contains the document length data. Data in this
	 * directory is compiled into the denoted by {@link #getDoclengthsData()}.
	 */
	public Path getDoclengthsDirectory() {
		return createPath(indexPath, "doclengths/");
	}

	public String getPostingsDirectory() {
		return appendPath(indexPath, "postings/");
	}

	public String getNonPositionalPostingsDirectory() {
		return appendPath(indexPath, "postings-non-pos/");
	}

	/**
	 * Returns directory that contains the {@link IntDocVector} representation
	 * of the collection.
	 */
	public String getIntDocVectorsDirectory() {
		return appendPath(indexPath, "int-doc-vectors/");
	}

	/**
	 * Returns file that contains an index into the {@link IntDocVector}
	 * representation of the collection. This file serves as input to
	 * {@link IntDocVectorsForwardIndex}, which provides random access to the
	 * document
	 * vectors.
	 */
	public String getIntDocVectorsForwardIndex() {
		return appendPath(indexPath, "int-doc-vectors-forward-index.dat");
	}

	/**
	 * Returns directory that contains the {@link TermDocVector} representation
	 * of the collection.
	 */
	public String getTermDocVectorsDirectory() {
		return appendPath(indexPath, "term-doc-vectors/");
	}

	/**
	 * Returns file that contains an index into the {@link TermDocVector}
	 * representation of the collection. This file serves as input to
	 * {@link TermDocVectorsForwardIndex}, which provides random access to the
	 * document vectors.
	 */
	public String getTermDocVectorsForwardIndex() {
		return appendPath(indexPath, "term-doc-vectors-forward-index.dat");
	}

	public String getWeightedTermDocVectorsDirectory() {
		return appendPath(indexPath, "wt-term-doc-vectors/");
	}

	/**
	 * Returns file that contains an index into the {@link WeightedIntDocVector}
	 * representation of the collection. This file serves as input to
	 * {@link WeightedIntDocVectorsForwardIndex}, which provides random access to the
	 * document
	 * vectors.
	 */
	public String getWeightedIntDocVectorsForwardIndex() {
		return appendPath(indexPath, "wt-int-doc-vectors-forward-index.dat");
	}

	public String getWeightedIntDocVectorsDirectory() {
		return appendPath(indexPath, "wt-int-doc-vectors/");
	}

	public String getTermDfCfDirectory() {
		return appendPath(indexPath, "term-df-cf/");
	}

	/**
	 * Returns file that contains the list of terms in the collection. The file
	 * consists of a stream of bytes, read into an array, representing the
	 * terms, which are alphabetically sorted and prefix-coded.
	 */
	public String getIndexTermsData() {
		return appendPath(indexPath, "index-terms.dat");
	}

	/**
	 * Returns file that contains term ids sorted by the alphabetical order of
	 * terms. The file consists of a stream of ints, read into an array. The
	 * array index position corresponds to alphabetical sort order of the term.
	 * This is used to retrieve the term id of a term given its position in the
	 * {@link #getIndexTermsData()} data.
	 */
	public String getIndexTermIdsData() {
		return appendPath(indexPath, "index-termids.dat");
	}

	/**
	 * Returns file that contains an index of term ids into the array of terms.
	 * The file consists of a stream of ints, read into an array. The array
	 * index position corresponds to the term ids. This is used to retrieve the
	 * index position into the {@link #getIndexTermsData()} file to map from
	 * term ids back to terms.
	 */
	public String getIndexTermIdMappingData() {
		return appendPath(indexPath, "index-termid-mapping.dat");
	}

	/**
	 * Returns file that contains a list of document frequencies sorted by the
	 * alphabetical order of terms. The file consists of a stream of ints, read
	 * into an array. The array index position corresponds to alphabetical sort
	 * order of the term. This is used to retrieve the df of a term given its
	 * position in the {@link #getIndexTermsData()} data.
	 */
	public String getDfByTermData() {
		return appendPath(indexPath, "df-by-term.dat");
	}

	/**
	 * Returns file that contains a list of document frequencies sorted by term
	 * id. The file consists of a stream of ints, read into an array. The array
	 * index position corresponds to the term id. Note that the term ids are
	 * assigned in descending order of document frequency. This is used to
	 * retrieve the df of a term given its term id.
	 */
	public String getDfByIntData() {
		return appendPath(indexPath, "df-by-int.dat");
	}

	/**
	 * Returns file that contains a list of collection frequencies sorted by the
	 * alphabetical order of terms. The file consists of a stream of ints, read
	 * into an array. The array index position corresponds to alphabetical sort
	 * order of the term. This is used to retrieve the cf of a term given its
	 * position in the {@link #getIndexTermsData()} data.
	 */
	public String getCfByTermData() {
		return appendPath(indexPath, "cf-by-term.dat");
	}

	/**
	 * Returns file that contains a list of collection frequencies sorted by
	 * term id. The file consists of a stream of ints, read into an array. The
	 * array index position corresponds to the term id. This is used to retrieve
	 * the cf of a term given its term id.
	 */
	public String getCfByIntData() {
		return appendPath(indexPath, "cf-by-int.dat");
	}

	/**
	 * Returns file that contains an index into the postings. This file serves
	 * as input to {@link IntPostingsForwardIndex}, which provides random access
	 * to postings lists.
	 */
	public String getPostingsIndexData() {
		return appendPath(indexPath, "postings-index.dat");
	}

	public String getTempDirectory() {
		return appendPath(indexPath, "tmp" + r.nextInt(10000));
	}

	public String readCollectionName()    { return FSProperty.readString(fs, appendPath(indexPath, "property.CollectionName")); }
	public String readCollectionPath()    { return FSProperty.readString(fs, appendPath(indexPath, "property.CollectionPath")); }
	public String readInputFormat()       { return FSProperty.readString(fs, appendPath(indexPath, "property.InputFormat")); }
	public String readTokenizerClass()    { return FSProperty.readString(fs, appendPath(indexPath, "property.Tokenizer"));	}
	public String readDocnoMappingClass() {	return FSProperty.readString(fs, appendPath(indexPath, "property.DocnoMappingClass")); }
	public String readPostingsType()      { return FSProperty.readString(fs, appendPath(indexPath, "property.PostingsType")); }

	public int   readCollectionDocumentCount() { return FSProperty.readInt(fs, appendPath(indexPath, "property.CollectionDocumentCount")); }
	public int   readCollectionTermCount()     { return FSProperty.readInt(fs, appendPath(indexPath, "property.CollectionTermCount")); }
	public int   readDocnoOffset()             { return FSProperty.readInt(fs, appendPath(indexPath, "property.DocnoOffset")); }
	public long  readCollectionLength()        { return FSProperty.readLong(fs, appendPath(indexPath, "property.CollectionLength")); }
	public float readCollectionAverageDocumentLength() { return FSProperty.readFloat(fs, appendPath(indexPath, "property.CollectionAverageDocumentLength")); }

	public void writeCollectionName(String s)    { FSProperty.writeString(fs, appendPath(indexPath, "property.CollectionName"), s); }
	public void writeCollectionPath(String s)    { FSProperty.writeString(fs, appendPath(indexPath, "property.CollectionPath"), s); }
	public void writeInputFormat(String s)       { FSProperty.writeString(fs, appendPath(indexPath, "property.InputFormat"), s); }
	public void writeTokenizerClass(String s)    { FSProperty.writeString(fs, appendPath(indexPath, "property.Tokenizer"), s); }
	public void writeDocnoMappingClass(String s) { FSProperty.writeString(fs, appendPath(indexPath, "property.DocnoMappingClass"), s); }
	public void writePostingsType(String type)   { FSProperty.writeString(fs, appendPath(indexPath, "property.PostingsType"), type); }

	public void writeCollectionDocumentCount(int n) { FSProperty.writeInt(fs, appendPath(indexPath, "property.CollectionDocumentCount"), n); }
	public void writeCollectionTermCount(int cnt)   { FSProperty.writeInt(fs, appendPath(indexPath, "property.CollectionTermCount"), cnt); }
	public void writeDocnoOffset(int n)             { FSProperty.writeInt(fs, appendPath(indexPath, "property.DocnoOffset"), n); }
	public void writeCollectionLength(long cnt)     { FSProperty.writeLong(fs, appendPath(indexPath, "property.CollectionLength"), cnt); }
	public void writeCollectionAverageDocumentLength(float n) { FSProperty.writeFloat(fs, appendPath(indexPath, "property.CollectionAverageDocumentLength"), n); }

	private static void testTerm(RetrievalEnvironment env, String term) {
		long startTime = System.currentTimeMillis();

		PostingsReader reader = null;
		Posting p = new Posting();
		int df = 0;
		String termOrig = term;
		String termTokenized = env.tokenize(termOrig)[0];

		LOG.info("term=" + termOrig + ", tokenized=" + termTokenized);
		reader = env.getPostingsReader(new Expression(termTokenized));
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
		return loadDocnoMapping(indexPath, fs);
	}

	public static DocnoMapping loadDocnoMapping(String indexPath, FileSystem fs) throws IOException {
		DocnoMapping mDocMapping = null;
		// load the docid to docno mappings
		try {
			LOG.info("Loading DocnoMapping file...");
			RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

			String className = env.readDocnoMappingClass();
			LOG.info(" - Class name: " + className);
			mDocMapping = (DocnoMapping) Class.forName(className).newInstance();

			Path mappingFile = env.getDocnoMappingData();
			LOG.info(" - File name: " + mappingFile);
			mDocMapping.loadMapping(mappingFile, fs);
			LOG.info("Done!");
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
