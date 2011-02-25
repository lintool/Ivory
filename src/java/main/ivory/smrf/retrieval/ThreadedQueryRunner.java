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

import ivory.exception.ConfigurationException;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Multi-threaded implementation of class to run queries.
 *
 * @author Don Metzler
 * @author Jimmy Lin
 */
public class ThreadedQueryRunner implements QueryRunner {
	private static final Logger LOG = Logger.getLogger(ThreadedQueryRunner.class);

	private final MRFBuilder builder;
	private final MRFExpander expander;
	private final ExecutorService threadPool;
	private final Map<String, Future<Accumulator[]>> queryResults = Maps.newLinkedHashMap();
	private final int numHits;

	public ThreadedQueryRunner(MRFBuilder builder, MRFExpander expander, int numThreads, int numHits) {
	  Preconditions.checkArgument(numThreads > 0);
	  Preconditions.checkArgument(numHits > 0);
	  this.builder = Preconditions.checkNotNull(builder);
	  this.expander = expander; // Expander can be null.
		this.threadPool = Executors.newFixedThreadPool(numThreads);
		this.numHits = numHits;
	}

	/**
	 * Runs a query asynchronously. Results can be fetched using
	 * {@link getResults}.
	 */
	public void runQuery(String qid, String[] query) {
		Preconditions.checkNotNull(qid);
		Preconditions.checkNotNull(query);

		Future<Accumulator[]> future = threadPool.submit(new ThreadTask(query, builder,
				expander, qid, numHits));
		queryResults.put(qid, future);
	}

	/**
	 * Runs a query synchronously, waiting until completion.
	 */
	public Accumulator[] runQuery(String[] query) {
		Preconditions.checkNotNull(query);

		Future<Accumulator[]> future = threadPool.submit(new ThreadTask(query, builder,
				expander, "query", numHits));
		Accumulator[] results = null;
		try {
			results = future.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	/**
	 * Fetches the results of a query. If necessary, waits until completion of
	 * the query.
	 * 
	 * @param qid
	 *            query id
	 */
	public Accumulator[] getResults(String qid) {
		try {
			return queryResults.get(qid).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		} catch (ExecutionException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Clears all stored results.
	 */
	public void clearResults() {
		queryResults.clear();
	}

	/**
	 * Returns results of all queries executed.
	 */
	public Map<String, Accumulator[]> getResults() {
		Map<String, Accumulator[]> results = new LinkedHashMap<String, Accumulator[]>();
		for (Map.Entry<String, Future<Accumulator[]>> e : queryResults.entrySet()) {
			try {
				Accumulator[] a = e.getValue().get();
				
				if ( a != null) {
					results.put(e.getKey(), e.getValue().get());
				}
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return results;
	}

	// Thread for running a query.  No need to expose implementation.
	private class ThreadTask implements Callable<Accumulator[]> {
		private final String[] query;
		private final MRFBuilder builder;
		private final MRFExpander expander;
		private final String qid;
		private final int numHits;

		public ThreadTask(String[] query, MRFBuilder builder, MRFExpander expander, String qid, int numHits) {
			this.query = query;
			this.builder = builder;
			this.expander = expander;
			this.qid = qid;
			this.numHits = numHits;
		}

		public Accumulator[] call() {
			try {
				long startTime;
				long endTime;

				startTime = System.currentTimeMillis();

				// Build the MRF for this query.
				MarkovRandomField mrf = builder.buildMRF(query);

				// Retrieve documents using this MRF.
				MRFDocumentRanker ranker = new MRFDocumentRanker(mrf, numHits);

				// Run initial query, if necessary.
				Accumulator[] results = null;
				if (expander != null) {
					results = ranker.rank();
				}

				// Perform pseudo-relevance feedback, if requested.
				if (expander != null) {
					// Get expanded MRF.
					MarkovRandomField expandedMRF = expander.getExpandedMRF(mrf, results);

					// Re-rank documents according to expanded MRF.
					ranker = new MRFDocumentRanker(expandedMRF, numHits);
				}

				// Rank the documents.
				results = ranker.rank();
				
				endTime = System.currentTimeMillis();
				LOG.info("Processed query " + qid + " in " + (endTime - startTime) + " ms.");

				return results;
			} catch (ConfigurationException e) {
				e.printStackTrace();
				LOG.error(e.getMessage());
				
				return null;
			}
		}
	}
}
