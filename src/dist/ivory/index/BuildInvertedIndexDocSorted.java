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

package ivory.index;

import ivory.data.PostingsList;
import ivory.data.PostingsListDocSortedNonPositional;
import ivory.data.PostingsListDocSortedPositional;
import ivory.util.RetrievalEnvironment;
import ivory.util.Tokenizer;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.util.ArrayListOfInts;
import edu.umd.cloud9.util.FSLineReader;
import edu.umd.cloud9.util.MapII;
import edu.umd.cloud9.util.MapKI;
import edu.umd.cloud9.util.OHMapII;
import edu.umd.cloud9.util.OHMapKI;
import edu.umd.cloud9.util.PowerTool;

/**
 * <p>
 * Indexer for building document-sorted inverted indexes (both positional and
 * non-positional). Description of the input parameters:
 * </p>
 * 
 * <ul>
 * 
 * <li><b>Ivory.NumMapTasks</b> (int): number of map tasks</li>
 * 
 * <li><b>Ivory.NumReduceTasks</b> (int): number of reduce tasks</li>
 * 
 * <li><b>Ivory.CollectionName</b> (String): name of the collection</li>
 * 
 * <li><b>Ivory.CollectionPath</b> (String): path to the collection</li>
 * 
 * <li><b>Ivory.IndexPath</b> (String): path to the index location</li>
 * 
 * <li><b>Ivory.InputFormat</b> (String): InputFormat class</li>
 * 
 * <li><b>Ivory.Tokenizer</b> (String): tokenizer class</li>
 * 
 * <li><b>Ivory.DocnoMappingClass</b> (String): DocnoMapping class</li>
 * 
 * <li><b>Ivory.Positional</b> (boolean): whether or not to build a positional
 * index.</li>
 * 
 * <li><b>Ivory.CollectionDocumentCount</b> (int): number of documents in the
 * collection</li>
 * 
 * </ul>
 * 
 * <p>
 * Description of the various counters:
 * </p>
 * 
 * <ul>
 * 
 * <li><b>Docs.Indexed</b>: number of documents indexed</li>
 * 
 * <li><b>Docs.Skipped</b>: number of documents skipped</li>
 * 
 * <li><b>Docs.Total</b>: total number of documents processed</li>
 * 
 * <li><b>Size.IndexedTerms</b>: total number of terms added to the index</li>
 * 
 * <li><b>Size.IndexedBytes</b>: byte total of all those terms</li>
 * 
 * <li><b>MapTime.Total</b>: cumulative time spent inside the mappers (in
 * milliseconds)</li>
 * 
 * <li><b>MapTime.Parsing</b>: cumulative time spent tokenizing/stemming the
 * documents (in milliseconds)</li>
 * 
 * <li><b>DocLengths</b>: number of documents for which document length was
 * computed (should be the same as Docs.Indexed; useful as a sanity check)</li>
 * 
 * <li><b>DocLengths.SumOfDocLengths</b>: sum of document lengths of all
 * documents (should be the same as Size.IndexedTermsl useful as a sanity check)</li>
 * 
 * </ul>
 * 
 * @author Jimmy Lin
 * 
 */
public class BuildInvertedIndexDocSorted extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(BuildInvertedIndexDocSorted.class);

	private static final int sPostingsMinDf = 0;

	protected static enum Docs {
		Indexed, Skipped, Total
	};

	protected static enum Size {
		IndexedBytes, IndexedTerms
	};

	protected static enum Terms {
		Total
	};

	protected static enum MapTime {
		Total, Parsing
	}

	protected static enum ReduceTime {
		Total
	}

	protected static enum DocLengths {
		Count, SumOfDocLengths
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<Writable, Indexable, TermDocno, TermPositions> {

		// key and value
		private static TermPositions sTermPositions = new TermPositions();
		private static TermDocno sTermDocno = new TermDocno();

		// the tokenizer
		private Tokenizer mTokenizer;

		// for mapping docids to docnos
		private DocnoMapping mDocMapping;

		// keep reference to OutputCollector and Reporter to use in close()
		private OutputCollector<TermDocno, TermPositions> mOutput;
		private Reporter mReporter;

		// need configuration to get FileSystem handle in close()
		private JobConf mJobConf;

		// holds the dfs for terms encountered by this mapper
		private OHMapKI<String> mLocalDfs;

		// holds document lengths
		private OHMapII mDocLengths;

		// keep track of index path to know where to write doclengths
		private String indexPath;

		// current docno
		private int mDocno;

		private String mTaskId;

		public void map(Writable key, Indexable doc,
				OutputCollector<TermDocno, TermPositions> output, Reporter reporter)
				throws IOException {
			mOutput = output;
			mReporter = reporter;
			mDocno = mDocMapping.getDocno(doc.getDocid());

			reporter.incrCounter(Docs.Total, 1);

			// if invalid docno, skip doc; remember docnos start at one
			if (mDocno <= 0) {
				reporter.incrCounter(Docs.Skipped, 1);
				return;
			}

			reporter.incrCounter(Docs.Indexed, 1);

			// start the timer
			long startTime = System.currentTimeMillis();

			// for storing the positions
			Map<String, ArrayListOfInts> positions = new HashMap<String, ArrayListOfInts>();

			String text = doc.getContent();
			String[] terms = mTokenizer.processContent(text);

			reporter.incrCounter(Size.IndexedBytes, text.length());

			// It may be tempting to compute doc length and and contribution to
			// collection term count here, but this may potentially result
			// in inaccurate numbers for a few reasons: the tokenizer may return
			// terms with zero length (empty terms), and the tf may exceed the
			// capacity of a short (in which case we need to handle separately).
			// The doc length and contribution to term count is computed as the
			// sum of all tfs of indexed terms a bit later.

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// guard against bad tokenization
				if (term.length() == 0)
					continue;

				// remember, token position is numbered started from one...
				if (positions.containsKey(term)) {
					positions.get(term).add(i + 1);
				} else {
					ArrayListOfInts l = new ArrayListOfInts();
					l.add(i + 1);
					positions.put(term, l);
				}
			}

			// end of parsing phase
			reporter.incrCounter(MapTime.Parsing, System.currentTimeMillis() - startTime);

			int dl = 0;
			// we're going to emit postings now...
			for (Map.Entry<String, ArrayListOfInts> e : positions.entrySet()) {
				ArrayListOfInts positionsList = e.getValue(); // positions.get(e.getKey());

				// we're storing tfs as shorts, so check for overflow...
				if (positionsList.size() > Short.MAX_VALUE) {
					// There are a few ways to handle this... If we're getting
					// such a high tf, then it most likely means that this is a
					// junk doc. The current implementation simply skips this
					// posting...
					sLogger.warn("Error: tf of " + e.getValue()
							+ " will overflow max short value. docno=" + mDocno + ", term="
							+ e.getKey());
					continue;
				}

				// set up the key and value, and emit
				sTermPositions.set(positionsList.getArray(), (short) positionsList.size());
				sTermDocno.set(e.getKey(), mDocno);
				output.collect(sTermDocno, sTermPositions);

				// document length of the current doc
				dl += positionsList.size();

				// df of the term in the partition handled by this mapper
				mLocalDfs.increment(e.getKey());
			}

			// record the document length
			mDocLengths.put(mDocno, dl);
			reporter.incrCounter(Size.IndexedTerms, dl);

			// end of mapper
			reporter.incrCounter(MapTime.Total, System.currentTimeMillis() - startTime);
		}

		public void configure(JobConf job) {
			sLogger.setLevel(Level.WARN);
		
			mLocalDfs = new OHMapKI<String>();
			mDocLengths = new OHMapII();
		
			mTaskId = job.get("mapred.task.id");
			indexPath = job.get("Ivory.IndexPath");
			mJobConf = job;
		
			// initialize the tokenizer
			try {
				mTokenizer = (Tokenizer) Class.forName(job.get("Ivory.Tokenizer")).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing Ivory.Tokenizer!");
			}
		
			// load the docid to docno mappings
			try {
				mDocMapping = (DocnoMapping) Class.forName(job.get("Ivory.DocnoMappingClass"))
						.newInstance();
		
				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (job.get("mapred.job.tracker").equals("local")) {
					FileSystem fs = FileSystem.get(job);
					String indexPath = job.get("Ivory.IndexPath");
					String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);
					mDocMapping.loadMapping(new Path(mappingFile), fs);
				} else {
					Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
					mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
		}

		public void close() throws IOException {
			int[] arr = new int[1];

			// emit dfs for terms encountered in this partition of the
			// collection
			for (MapKI.Entry<String> e : mLocalDfs.entrySet()) {
				// dummy value
				arr[0] = e.getValue();
				sTermPositions.set(arr, (short) 1);
				// special docno of "-1" to make sure this key-value pair
				// comes before all other postings in reducer
				sTermDocno.set(e.getKey(), -1);
				mOutput.collect(sTermDocno, sTermPositions);
			}

			// Now we want to write out the doclengths as "side data" onto HDFS.
			// Since speculative execution is on, we'll append the task id to
			// the filename to guarantee uniqueness. However, this means that
			// the may be multiple files with the same doclength information,
			// which we have the handle when we go to write out the binary
			// encoding of the data.

			long bytesCnt = 0;
			FileSystem fs = FileSystem.get(mJobConf);
			// use the last processed docno as the file name + task id
			Path path = new Path(indexPath + "/doclengths/" + mDocno + "." + mTaskId);
			FSDataOutputStream out = fs.create(path, false);

			// iterate through the docs and write out doclengths
			long dlSum = 0;
			int cnt = 0;
			for (MapII.Entry e : mDocLengths.entrySet()) {
				String s = e.getKey() + "\t" + e.getValue() + "\n";
				out.write(s.getBytes());
				bytesCnt += s.getBytes().length;
				cnt++;
				dlSum += e.getValue();
			}
			out.close();

			// We want to check if the file has actually been written
			// successfully...
			sLogger.info("Expected length of doclengths file: " + bytesCnt);

			long bytesActual = fs.listStatus(path)[0].getLen();
			sLogger.info("Actual length of doclengths file: " + bytesActual);

			if (bytesCnt == 0 || bytesActual == 0 || bytesCnt != bytesActual)
				throw new RuntimeException("Error writing Doclengths file: " + path);

			mReporter.incrCounter(DocLengths.Count, cnt);
			// sum of the document lengths, should match sum of tfs
			mReporter.incrCounter(DocLengths.SumOfDocLengths, dlSum);
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<TermDocno, TermPositions, Text, PostingsList> {
		private static final Text sTerm = new Text();
		private String mPrevTerm = null;
		private int mNumPostings = 0;

		private OutputCollector<Text, PostingsList> mOutput;
		private Reporter mReporter;
		private PostingsList mPostings;

		public void configure(JobConf job) {
			sLogger.setLevel(Level.WARN);

			if (job.getBoolean("Ivory.Positional", false)) {
				mPostings = new PostingsListDocSortedPositional();
			} else {
				mPostings = new PostingsListDocSortedNonPositional();
			}
			mPostings.setCollectionDocumentCount(job.getInt("Ivory.CollectionDocumentCount", 0));
		}

		public void reduce(TermDocno tp, Iterator<TermPositions> values,
				OutputCollector<Text, PostingsList> output, Reporter reporter) throws IOException {
			long start = System.currentTimeMillis();
			String curTerm = tp.getTerm();

			if (tp.getDocno() == -1) {
				if (mPrevTerm == null) {
					mOutput = output;
					mReporter = reporter;
				} else if (!curTerm.equals(mPrevTerm)) {
					if (mPostings.size() > sPostingsMinDf) {
						sTerm.set(mPrevTerm);
						output.collect(sTerm, mPostings);
						reporter.incrCounter(Terms.Total, 1);

						sLogger.info("Finished processing postings for term \"" + mPrevTerm
								+ "\" (expected=" + mNumPostings + ", actual=" + mPostings.size()
								+ ")");

						if (mNumPostings != mPostings.size()) {
							throw new RuntimeException(
									"Error, number of postings added doesn't match number of expected postings.");
						}
					} else {
						sLogger.info("Skipping \"" + mPrevTerm + "\" because df=1");
					}

					mPostings.clear();
				}

				mNumPostings = 0;
				while (values.hasNext()) {
					TermPositions positions = values.next();
					mNumPostings += positions.getPositions()[0];
				}

				sLogger.info("Processing postings for term \"" + curTerm + "\", " + mNumPostings
						+ " postings expected.");

				mPostings.setNumberOfPostings(mNumPostings);

				return;
			}

			TermPositions positions = values.next();
			mPostings.add(tp.getDocno(), positions.getTf(), positions);

			if (values.hasNext()) {
				throw new RuntimeException(
						"Error: multiple TermDocno with the same term and docno -- docno: " + tp.getDocno() + ", term:" + curTerm);
			}

			mPrevTerm = curTerm;

			long duration = System.currentTimeMillis() - start;
			reporter.incrCounter(ReduceTime.Total, duration);
		}

		public void close() throws IOException {
			long start = System.currentTimeMillis();
			if (mPostings.size() > sPostingsMinDf) {
				sTerm.set(mPrevTerm);
				mOutput.collect(sTerm, mPostings);
				mReporter.incrCounter(Terms.Total, 1);

				sLogger.info("Finished processing postings for term \"" + mPrevTerm + "\" (df="
						+ mNumPostings + ", number of postings=" + mPostings.size() + ")");

				if (mNumPostings != mPostings.size()) {
					throw new RuntimeException(
							"Error: actual number of postings processed is different from expected df.");
				}
			} else {
				sLogger.info("Skipping \"" + mPrevTerm + "\" because df=1.");
			}

			sLogger.info("Finished processing all postings!");
			long duration = System.currentTimeMillis() - start;
			mReporter.incrCounter(ReduceTime.Total, duration);

		}
	}

	private static class MyPartitioner implements Partitioner<TermDocno, TermPositions> {
		public void configure(JobConf job) {
		}

		// keys with the same terms should go to the same reducer
		public int getPartition(TermDocno key, TermPositions value, int numReduceTasks) {
			return (key.getTerm().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private static class DocLengthDataWriterMapper extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			String outputPath = conf.get("OutputPath");
			String dataFile = conf.get("DocLengthDataFile");

			int docnoOffset = conf.getInt("Ivory.DocnoOffset", 0);

			Path p = new Path(outputPath);

			sLogger.info("OutputPath: " + outputPath);
			sLogger.info("DocLengthDataFile: " + dataFile);
			sLogger.info("DocnoOffset: " + docnoOffset);

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fileStats = fs.listStatus(p);

			// initial array to hold the doc lengths
			int[] doclengths = new int[1000];

			// largest docno
			int maxDocno = 0;

			// smallest docno
			int minDocno = Integer.MAX_VALUE;

			for (int i = 0; i < fileStats.length; i++) {
				// skip log files
				if (fileStats[i].getPath().getName().startsWith("_"))
					continue;

				sLogger.info("reading " + fileStats[i].getPath().toString());

				FSLineReader reader = new FSLineReader(fileStats[i].getPath(), fs);

				Text line = new Text();
				while (reader.readLine(line) > 0) {
					String[] arr = line.toString().split("\\t+", 2);

					int docno = Integer.parseInt(arr[0]);
					int len = Integer.parseInt(arr[1]);

					// resize array if it's too small
					if (doclengths.length < (docno - docnoOffset) + 1) {
						doclengths = resize(doclengths, (docno - docnoOffset) * 2);
					}

					// Note that because of speculative execution there may be
					// multiple copies of doclength data. Therefore, we can't
					// just count number of doclengths read. Instead, keep track
					// of largest docno encountered.

					if (docno < docnoOffset) {
						throw new RuntimeException("Error: docno " + docno + " < docnoOffset "
								+ docnoOffset + "!");
					}

					doclengths[docno - docnoOffset] = len;

					if (docno > maxDocno)
						maxDocno = docno;

					if (docno < minDocno)
						minDocno = docno;

					reporter.incrCounter(DocLengths.Count, 1);
					reporter.incrCounter(DocLengths.SumOfDocLengths, len);

				}
				reader.close();
			}

			sLogger.info("min docno: " + minDocno);
			sLogger.info("max docno: " + maxDocno);

			// write out the doc length data into a single file
			FSDataOutputStream out = fs.create(new Path(dataFile), true);

			out.writeInt(docnoOffset);

			// first, write out the collection size
			out.writeInt(maxDocno - docnoOffset);

			// write out length of each document (docnos are sequentially
			// ordered starting from one, so no need to explicitly keep track)
			int n = 0;
			for (int i = 1; i <= maxDocno - docnoOffset; i++) {
				out.writeInt(doclengths[i]);
				n++;
			}
			sLogger.info(n + " doc lengths written");

			out.close();
		}

		private int[] resize(int[] input, int size) {
			if (size < 0)
				return null;

			if (size <= input.length)
				return input;

			int[] newArray = new int[size];

			System.arraycopy(input, 0, newArray, 0, size < input.length ? size : input.length);

			return newArray;
		}

	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
			"Ivory.NumReduceTasks", "Ivory.CollectionName", "Ivory.CollectionPath",
			"Ivory.IndexPath", "Ivory.InputFormat", "Ivory.Tokenizer", "Ivory.DocnoMappingClass",
			"Ivory.Positional", "Ivory.CollectionDocumentCount" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildInvertedIndexDocSorted(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unchecked")
	public int runTool() throws Exception {
		sLogger.info("PowerTool: BuildInvertedIndexDocSorted");

		// create a new JobConf, inheriting from the configuration of this
		// PowerTool
		JobConf conf = new JobConf(getConf(), BuildInvertedIndexDocSorted.class);
		FileSystem fs = FileSystem.get(conf);
		
		String indexPath = conf.get("Ivory.IndexPath");
		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 0);
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);

		String collectionName = conf.get("Ivory.CollectionName");
		String collectionPath = conf.get("Ivory.CollectionPath");
		String inputFormat = conf.get("Ivory.InputFormat");
		String tokenizer = conf.get("Ivory.Tokenizer");
		String mappingClass = conf.get("Ivory.DocnoMappingClass");

		boolean positional = conf.getBoolean("Ivory.Positional", false);
		int collectionDocCount = conf.getInt("Ivory.CollectionDocumentCount", 0);

		sLogger.info("Characteristics of the collection:");
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - CollectionPath: " + collectionPath);
		sLogger.info(" - CollectionDocumentCount: " + collectionDocCount);
		sLogger.info(" - InputputFormat: " + inputFormat);
		sLogger.info(" - Tokenizer: " + tokenizer);
		sLogger.info(" - DocnoMappingClass: " + mappingClass);
		sLogger.info(" - Positional? " + positional);
		sLogger.info("Characteristics of the job:");
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);
		sLogger.info(" - MinSplitSize: " + minSplitSize);

		String mappingFile = RetrievalEnvironment.getDocnoMappingFile(indexPath);

		if (!fs.exists(new Path(mappingFile))) {
			throw new RuntimeException("Error, docno mapping data file " + mappingFile + "doesn't exist!");
		}

		DistributedCache.addCacheFile(new URI(mappingFile), conf);

		if (!fs.exists(new Path(indexPath))) {
			fs.mkdirs(new Path(indexPath));
		}

		Path postingsPath = new Path(RetrievalEnvironment.getPostingsDirectory(indexPath));
		Path dlPath = new Path(RetrievalEnvironment.getDoclengthsDirectory(indexPath));

		fs.delete(postingsPath, true);
		fs.delete(dlPath, true);

		conf.setJobName("BuildInvertedIndexDocSorted:" + collectionName);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.setInt("mapred.min.split.size", minSplitSize);

		if (collectionPath.indexOf(",") == -1) {
			FileInputFormat.setInputPaths(conf, new Path(collectionPath));
			sLogger.info("Adding input path " + collectionPath);
		} else {
			String[] paths = collectionPath.split(",");
			for (String p : paths) {
				FileInputFormat.addInputPath(conf, new Path(p));
				sLogger.info("Adding input path " + p);
			}
		}

		FileOutputFormat.setOutputPath(conf, postingsPath);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		//conf.setInt("mapred.map.max.attempts", 20);
		//conf.setInt("mapred.reduce.max.attempts", 10);
		//conf.setSpeculativeExecution(false);
		//conf.setCompressMapOutput(true);

		conf.setInputFormat((Class<? extends InputFormat>) Class.forName(inputFormat));
		conf.setMapOutputKeyClass(TermDocno.class);
		conf.setMapOutputValueClass(TermPositions.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(Text.class);

		if (positional) {
			conf.setOutputValueClass(PostingsListDocSortedPositional.class);
		} else {
			conf.setOutputValueClass(PostingsListDocSortedNonPositional.class);
		}

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		RetrievalEnvironment.writeCollectionName(fs, indexPath, collectionName);
		RetrievalEnvironment.writeCollectionPath(fs, indexPath, collectionPath);
		RetrievalEnvironment.writeCollectionDocumentCount(fs, indexPath, collectionDocCount);
		RetrievalEnvironment.writeInputFormat(fs, indexPath, inputFormat);
		RetrievalEnvironment.writeDocnoMappingClass(fs, indexPath, mappingClass);
		RetrievalEnvironment.writeTokenizerClass(fs, indexPath, tokenizer);

		// write out type of postings
		RetrievalEnvironment.writePostingsType(fs, indexPath,
				positional ? "ivory.data.PostingsListDocSortedPositional"
						: "ivory.data.PostingsListDocSortedNonPositional");

		Counters counters = job.getCounters();

		// write out number of postings
		Counter counter = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", 8,
				"REDUCE_OUTPUT_RECORDS");
		int numPostings = (int) counter.getCounter();
		RetrievalEnvironment.writeNumberOfPostings(fs, indexPath, numPostings);

		long collectionTermCount = counters.findCounter(Size.IndexedTerms).getCounter();
		RetrievalEnvironment.writeCollectionTermCount(fs, indexPath, collectionTermCount);

		sLogger.info(" - NumberOfPostings: " + numPostings);
		sLogger.info(" - CollectionTermCount: " + collectionTermCount);

		writeDoclengthsData();

		return 0;
	}

	private void writeDoclengthsData() throws IOException {
		JobConf conf = new JobConf(getConf(), BuildInvertedIndexDocSorted.class);

		String indexPath = conf.get("Ivory.IndexPath");
		String collectionName = conf.get("Ivory.CollectionName");
		int docnoOffset = conf.getInt("Ivory.DocnoOffset", 0);

		String dlFile = RetrievalEnvironment.getDoclengthsFile(indexPath);
		String outputPath = RetrievalEnvironment.getDoclengthsDirectory(indexPath);

		sLogger.info("Writing doc length data to " + dlFile + "...");
		sLogger.info(" - Docno offset: " + docnoOffset);

		conf.setJobName("DocLengthTable:" + collectionName);
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setSpeculativeExecution(false);

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(DocLengthDataWriterMapper.class);

		conf.set("OutputPath", outputPath);
		conf.set("DocLengthDataFile", dlFile);
		JobClient.runJob(conf);

		// remove temp output
		// FileSystem.get(conf).delete(new Path(outputPath), true);

		RetrievalEnvironment.writeDocnoOffset(FileSystem.get(conf), indexPath, docnoOffset);

		sLogger.info("Done!");
	}
}
