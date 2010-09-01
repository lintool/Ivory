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

package ivory.smrf.retrieval;

import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.TermNode;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

/**
 * @author Don Metzler
 * 
 */
public class ThreadedQueryRunner implements QueryRunner {
	private static final Logger sLogger = Logger.getLogger(ThreadedQueryRunner.class);

	private MRFBuilder mBuilder;
	private MRFExpander mExpander;
	private ExecutorService mThreadPool;
	private Map<String, Future<Accumulator[]>> mQueryResults;
	private int mNumHits;

	public ThreadedQueryRunner(MRFBuilder builder, MRFExpander expander, int numThreads, int numHits) {
		mBuilder = builder;
		mExpander = expander;
		mThreadPool = Executors.newFixedThreadPool(numThreads);
		mQueryResults = new LinkedHashMap<String, Future<Accumulator[]>>();
		mNumHits = numHits;
	}

	public void runQuery(String qid, String[] query) {
		Future<Accumulator[]> future = mThreadPool.submit(new ThreadTask(query, mBuilder,
				mExpander, mNumHits));
		mQueryResults.put(qid, future);
	}

	public Accumulator[] runQuery(String[] query) {
		Future<Accumulator[]> future = mThreadPool.submit(new ThreadTask(query, mBuilder,
				mExpander, mNumHits));
		Accumulator[] results = null;
		try {
			results = future.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	/**
	 * @param qid
	 */
	public Accumulator[] getResults(String qid) {
		try {
			return mQueryResults.get(qid).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		} catch (ExecutionException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void clearResults() {
		mQueryResults.clear();
	}

	public Map<String, Accumulator[]> getResults() {
		Map<String, Accumulator[]> results = new LinkedHashMap<String, Accumulator[]>();
		for (Map.Entry<String, Future<Accumulator[]>> e : mQueryResults.entrySet()) {
			try {
				results.put(e.getKey(), e.getValue().get());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return results;
	}

	public class ThreadTask implements Callable<Accumulator[]> {
		private String[] mQuery;
		private MRFBuilder mBuilder;
		private MRFExpander mExpander;
		private int mNumHits;

		/**
		 * @param query
		 * @param builder
		 * @param expander
		 */
		public ThreadTask(String[] query, MRFBuilder builder, MRFExpander expander, int numHits) {
			mQuery = query;
			mBuilder = builder;
			mExpander = expander;
			mNumHits = numHits;
		}

		public Accumulator[] call() throws Exception {
			long startTime;
			long endTime;

			try {
				startTime = System.currentTimeMillis();

				// build the MRF for this query
				MarkovRandomField mrf = mBuilder.buildMRF(mQuery);
				endTime = System.currentTimeMillis();

				sLogger.info("MRF initialization time (ms): " + (endTime - startTime));

				// retrieve documents using this MRF
				startTime = System.currentTimeMillis();
				MRFDocumentRanker ranker = new MRFDocumentRanker(mrf, mNumHits);

				// run initial query, if necessary
				Accumulator[] results = null;
				if (mExpander != null) {
					results = ranker.rank();
				}

				// perform pseudo-relevance feedback, if requested
				if (mExpander != null) {
					// get expanded MRF
					MarkovRandomField expandedMRF = mExpander.getExpandedMRF(mrf, results);

					// re-rank documents according to expanded MRF
					ranker = new MRFDocumentRanker(expandedMRF, mNumHits);
				}


				endTime = System.currentTimeMillis();
				sLogger.info("MRF document ranker initialization time (ms): "
						+ (endTime - startTime));

				// rank the documents
				startTime = System.currentTimeMillis();
				results = ranker.rank();
				endTime = System.currentTimeMillis();
				sLogger.info("MRF document ranking time (ms): " + (endTime - startTime));

				return results;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

	}

}
