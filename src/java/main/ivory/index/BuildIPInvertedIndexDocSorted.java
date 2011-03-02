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
import ivory.data.PostingsList;
import ivory.data.PostingsListDocSortedPositional;
import ivory.data.IntDocVector.Reader;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

/**
 * Indexer for building document-sorted inverted indexes.
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 * 
 */
@SuppressWarnings("deprecation")
public class BuildIPInvertedIndexDocSorted extends PowerTool {
	private static final Logger LOG = Logger.getLogger(BuildIPInvertedIndexDocSorted.class);

	protected static enum Docs { Total }
	protected static enum IndexedTerms { Unique, Total }
	protected static enum MapTime { Total }
	protected static enum ReduceTime { Total }

	private static class MyMapper extends MapReduceBase implements Mapper<IntWritable, IntDocVector, PairOfInts, TermPositions> {
		private static final TermPositions termPositions = new TermPositions();
		private static final PairOfInts pair = new PairOfInts();
		private static final HMapII dfs = new HMapII();  // Holds dfs of terms processed by this mapper.

		private int docno;

		// Need to keep reference around for use in close().
		private OutputCollector<PairOfInts, TermPositions> output;

		public void map(IntWritable key, IntDocVector doc, OutputCollector<PairOfInts, TermPositions> output, Reporter reporter) throws IOException {
			this.output = output;
			docno = key.get();

			long startTime = System.currentTimeMillis();
			Reader r = doc.getReader();

			int dl = 0;
			while (r.hasMoreTerms()) {
				int term = r.nextTerm();
				r.getPositions(termPositions);

				// Set up the key and value, and emit.
				pair.set(term, docno);
				output.collect(pair, termPositions);

				// Document length of the current doc.
				dl += termPositions.getTf();

				// Df of the term in the partition handled by this mapper.
				dfs.increment(term);
			}

			reporter.incrCounter(IndexedTerms.Total, dl);   // Update number of indexed terms.
			reporter.incrCounter(Docs.Total, 1);            // Update number of docs.
			reporter.incrCounter(MapTime.Total, System.currentTimeMillis() - startTime);
		}

		public void close() throws IOException {
			int[] arr = new int[1];

			// Emit dfs for terms encountered in this partition of the collection.
			for (MapII.Entry e : dfs.entrySet()) {
				arr[0] = e.getValue();
				termPositions.set(arr, (short) 1);          // Dummy value.
				// Special docno of "-1" to make sure this key-value pair
				// comes before all other postings in reducer.
				pair.set(e.getKey(), -1);
				output.collect(pair, termPositions);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements	Reducer<PairOfInts, TermPositions, IntWritable, PostingsList> {
		private static final IntWritable term = new IntWritable();
		private static final PostingsList postings = new PostingsListDocSortedPositional();

		private int prevTerm = -1;
		private int numPostings = 0;

		private OutputCollector<IntWritable, PostingsList> output;
		private Reporter reporter;

		@Override
		public void configure(JobConf job) {
			LOG.setLevel(Level.WARN);
			postings.setCollectionDocumentCount(job.getInt("Ivory.CollectionDocumentCount", 0));
		}

		public void reduce(PairOfInts pair, Iterator<TermPositions> values, OutputCollector<IntWritable, PostingsList> output, Reporter reporter) throws IOException {
			long start = System.currentTimeMillis();
			int curTerm = pair.getLeftElement();

			if (pair.getRightElement() == -1) {
				if (prevTerm == -1) {
					// First term, so save references.
					this.output = output;
					this.reporter = reporter;
				} else if (curTerm != prevTerm) {
					// Encountered next term, so emit postings corresponding to previous term.
					if (numPostings != postings.size()) {
						throw new RuntimeException("Error: actual number of postings processed is different from expected!");
					}

					term.set(prevTerm);
					output.collect(term, postings);
					reporter.incrCounter(IndexedTerms.Unique, 1);

					LOG.info(String.format("Finished processing postings for term \"%s\" (num postings=%d)", prevTerm, postings.size()));
					postings.clear();
				}

				numPostings = 0;
				while (values.hasNext()) {
					TermPositions positions = values.next();
					numPostings += positions.getPositions()[0];
				}

				postings.setNumberOfPostings(numPostings);
				return;
			}

			TermPositions positions = values.next();
			postings.add(pair.getRightElement(), positions.getTf(), positions);

			if (values.hasNext()) {
				throw new RuntimeException("Error: values with the same (term, docno): docno=" + pair.getRightElement() + ", term=" + curTerm);
			}

			prevTerm = curTerm;

			long duration = System.currentTimeMillis() - start;
			reporter.incrCounter(ReduceTime.Total, duration);
		}

		public void close() throws IOException {
			long start = System.currentTimeMillis();

			// We need to flush out the final postings list.
			if (numPostings != postings.size()) {
				throw new RuntimeException("Error: actual number of postings processed is different from expected!");
			}

			term.set(prevTerm);
			output.collect(term, postings);
			reporter.incrCounter(IndexedTerms.Unique, 1);

			LOG.info(String.format("Finished processing postings for term \"%s\" (num postings=%d)", prevTerm, postings.size()));
			reporter.incrCounter(ReduceTime.Total, System.currentTimeMillis() - start);
		}
	}

	private static class MyPartitioner implements Partitioner<PairOfInts, TermPositions> {
		public void configure(JobConf job) {}

		// Keys with the same terms should go to the same reducer.
		public int getPartition(PairOfInts key, TermPositions value, int numReduceTasks) {
			return (key.getLeftElement() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	public static final String[] RequiredParameters = { "Ivory.NumMapTasks", "Ivory.NumReduceTasks", "Ivory.IndexPath" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildIPInvertedIndexDocSorted(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("unused")
	public int runTool() throws Exception {
		JobConf conf = new JobConf(getConf(), BuildIPInvertedIndexDocSorted.class);
		FileSystem fs = FileSystem.get(conf);

		String indexPath = conf.get("Ivory.IndexPath");
		RetrievalEnvironment env = new RetrievalEnvironment(indexPath, fs);

		String collectionName = env.readCollectionName();

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 0);
		int minSplitSize = conf.getInt("Ivory.MinSplitSize", 0);
		int collectionDocCnt = env.readCollectionDocumentCount();

		LOG.info("PowerTool: BuildIPInvertedIndexDocSorted");
		LOG.info(" - IndexPath: " + indexPath);
		LOG.info(" - CollectionName: " + collectionName);
		LOG.info(" - CollectionDocumentCount: " + collectionDocCnt);
		LOG.info(" - NumMapTasks: " + mapTasks);
		LOG.info(" - NumReduceTasks: " + reduceTasks);
		LOG.info(" - MinSplitSize: " + minSplitSize);

		if (!fs.exists(new Path(indexPath))) {
			fs.mkdirs(new Path(indexPath));
		}

		Path inputPath = new Path(env.getIntDocVectorsDirectory());
		Path postingsPath = new Path(env.getPostingsDirectory());

		if (fs.exists(postingsPath)) {
			LOG.info("Postings already exist: no indexing will be performed.");
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

		conf.setMapOutputKeyClass(PairOfInts.class);
		conf.setMapOutputValueClass(TermPositions.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PostingsListDocSortedPositional.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		long startTime = System.currentTimeMillis();
		RunningJob job = JobClient.runJob(conf);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0	+ " seconds");

		env.writePostingsType("ivory.data.PostingsListDocSortedPositional");

		return 0;
	}
}
