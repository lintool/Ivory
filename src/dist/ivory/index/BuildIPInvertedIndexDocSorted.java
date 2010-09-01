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

import ivory.data.IntDocVector;
import ivory.data.IntDocVectorReader;
import ivory.data.PostingsList;
import ivory.data.PostingsListDocSortedPositional;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.HMapII;
import edu.umd.cloud9.util.MapII;
import edu.umd.cloud9.util.PowerTool;

/**
 * <p>
 * Indexer for building document-sorted inverted indexes.
 * </p>
 * 
 * @author Jimmy Lin
 * @author Tamer Elsayed
 * 
 */
public class BuildIPInvertedIndexDocSorted extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(BuildIPInvertedIndexDocSorted.class);

	protected static enum Docs {
		Total
	}

	protected static enum IndexedTerms {
		Unique, Total
	}

	protected static enum MapTime {
		Total
	}

	protected static enum ReduceTime {
		Total
	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<IntWritable, IntDocVector, IntTermDocno, TermPositions> {

		// key and value
		private static TermPositions sTermPositions = new TermPositions();
		private static IntTermDocno sTermDocno = new IntTermDocno();

		// keep reference to OutputCollector and Reporter to use in close()
		private OutputCollector<IntTermDocno, TermPositions> mOutput;

		// holds the dfs for terms encountered by this mapper
		private HMapII mLocalDfs;

		// current docno
		private int mDocno;

		public void configure(JobConf job) {
			sLogger.setLevel(Level.WARN);
			mLocalDfs = new HMapII();
		}

		public void map(IntWritable key, IntDocVector doc,
				OutputCollector<IntTermDocno, TermPositions> output, Reporter reporter)
				throws IOException {
			mOutput = output;
			mDocno = key.get();

			// start the timer
			long startTime = System.currentTimeMillis();

			IntDocVectorReader r = doc.getDocVectorReader();

			int dl = 0;
			while (r.hasMoreTerms()) {
				int term = r.nextTerm();
				r.getPositions(sTermPositions);
				// set up the key and value, and emit
				sTermDocno.set(term, mDocno);
				output.collect(sTermDocno, sTermPositions);

				// document length of the current doc
				dl += sTermPositions.getTf();

				// df of the term in the partition handled by this mapper
				mLocalDfs.increment(term);
			}

			// update number of indexed terms
			reporter.incrCounter(IndexedTerms.Total, dl);

			// end of mapper
			reporter.incrCounter(MapTime.Total, System.currentTimeMillis() - startTime);

			reporter.incrCounter(Docs.Total, 1);
		}

		public void close() throws IOException {
			int[] arr = new int[1];

			// emit dfs for terms encountered in this partition of the
			// collection
			for (MapII.Entry e : mLocalDfs.entrySet()) {
				arr[0] = e.getValue();
				// dummy value
				sTermPositions.set(arr, (short) 1);
				// special docno of "-1" to make sure this key-value pair
				// comes before all other postings in reducer
				sTermDocno.set(e.getKey(), -1);
				mOutput.collect(sTermDocno, sTermPositions);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<IntTermDocno, TermPositions, IntWritable, PostingsList> {
		private static final IntWritable sTerm = new IntWritable();
		private int mPrevTerm = -1;
		private int mNumPostings = 0;

		private OutputCollector<IntWritable, PostingsList> mOutput;
		private Reporter mReporter;
		private PostingsList mPostings;

		public void configure(JobConf job) {
			sLogger.setLevel(Level.WARN);

			mPostings = new PostingsListDocSortedPositional();
			mPostings.setCollectionDocumentCount(job.getInt("Ivory.CollectionDocumentCount", 0));
		}

		public void reduce(IntTermDocno termDocno, Iterator<TermPositions> values,
				OutputCollector<IntWritable, PostingsList> output, Reporter reporter)
				throws IOException {
			long start = System.currentTimeMillis();
			int curTerm = termDocno.getTerm();

			if (termDocno.getDocno() == -1) {
				if (mPrevTerm == -1) {
					mOutput = output;
					mReporter = reporter;
				} else if (curTerm != mPrevTerm) {
					sTerm.set(mPrevTerm);
					output.collect(sTerm, mPostings);
					reporter.incrCounter(IndexedTerms.Unique, 1);

					sLogger
							.info("Finished processing postings for term \"" + mPrevTerm
									+ "\" (expected=" + mNumPostings + ", actual="
									+ mPostings.size() + ")");

					if (mNumPostings != mPostings.size()) {
						throw new RuntimeException(
								"Error, number of postings added doesn't match number of expected postings.");
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
			mPostings.add(termDocno.getDocno(), positions.getTf(), positions);

			if (values.hasNext()) {
				throw new RuntimeException(
						"Error: multiple TermDocno with the same term and docno -- docno: "
								+ termDocno.getDocno() + ", term:" + curTerm);
			}

			mPrevTerm = curTerm;

			long duration = System.currentTimeMillis() - start;
			reporter.incrCounter(ReduceTime.Total, duration);
		}

		public void close() throws IOException {
			long start = System.currentTimeMillis();

			sTerm.set(mPrevTerm);
			mOutput.collect(sTerm, mPostings);
			mReporter.incrCounter(IndexedTerms.Unique, 1);

			sLogger.info("Finished processing postings for term \"" + mPrevTerm + "\" (df="
					+ mNumPostings + ", number of postings=" + mPostings.size() + ")");

			if (mNumPostings != mPostings.size()) {
				throw new RuntimeException(
						"Error: actual number of postings processed is different from expected df.");
			}

			sLogger.info("Finished processing all postings!");
			long duration = System.currentTimeMillis() - start;
			mReporter.incrCounter(ReduceTime.Total, duration);

		}
	}

	private static class MyPartitioner implements Partitioner<IntTermDocno, TermPositions> {
		public void configure(JobConf job) {
		}

		// keys with the same terms should go to the same reducer
		public int getPartition(IntTermDocno key, TermPositions value, int numReduceTasks) {
			return (key.getTerm() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks",
			"Ivory.NumReduceTasks", "Ivory.IndexPath" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildIPInvertedIndexDocSorted(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unchecked")
	public int runTool() throws Exception {
		// create a new JobConf, inheriting from the configuration of this
		// PowerTool
		JobConf conf = new JobConf(getConf(), BuildIPInvertedIndexDocSorted.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		String collectionName = env.readCollectionName();

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 0);
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);
		int collectionDocCnt = env.readCollectionDocumentCount();

		sLogger.info("PowerTool: BuildIPInvertedIndexDocSorted");
		sLogger.info(" - IndexPath: " + indexPath);
		sLogger.info(" - CollectionName: " + collectionName);
		sLogger.info(" - CollectionDocumentCount: " + collectionDocCnt);
		sLogger.info(" - NumMapTasks: " + mapTasks);
		sLogger.info(" - NumReduceTasks: " + reduceTasks);
		sLogger.info(" - MinSplitSize: " + minSplitSize);

		if (!fs.exists(new Path(indexPath))) {
			fs.mkdirs(new Path(indexPath));
		}

		Path inputPath = new Path(env.getIntDocVectorsDirectory());
		Path postingsPath = new Path(env.getPostingsDirectory());

		if (fs.exists(postingsPath)) {
			sLogger.info("Postings already exist: no indexing will be performed.");
			return 0;
		}

		conf.setJobName("BuildIPInvertedIndex:" + collectionName);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInt("Ivory.CollectionDocumentCount", collectionDocCnt);

		conf.setInt("mapred.min.split.size", minSplitSize);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, postingsPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(IntTermDocno.class);
		conf.setMapOutputValueClass(TermPositions.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PostingsListDocSortedPositional.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		env.writePostingsType("ivory.data.PostingsListDocSortedPositional");

		return 0;
	}
}
