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

package ivory.cascade.retrieval;

import ivory.exception.ConfigurationException;
import ivory.smrf.model.MarkovRandomField;
import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;
import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.MRFDocumentRanker;
import ivory.util.RetrievalEnvironment;

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
 * @author Lidan Wang
 */
public class CascadeThreadedQueryRunner implements CascadeQueryRunner {
	private static final Logger sLogger = Logger.getLogger(CascadeThreadedQueryRunner.class);

	private MRFBuilder mBuilder;
	private MRFExpander mExpander;
	private ExecutorService mThreadPool;
	private Map<String, Future<Accumulator[]>> mQueryResults;
	private int mNumHits;
	private Map<Integer, Float[][]> savedResults_prevStage = Maps.newHashMap(); //for all queries
	private int mK; //K value used in cascade model

	//assume no more than 1000 queries
	private float [] cascadeCostAllQueries = new float[1000];
	private float [] cascadeCostAllQueries_lastStage = new float[1000];

	public CascadeThreadedQueryRunner(MRFBuilder builder, MRFExpander expander, int numThreads,
	    int numHits, Map<Integer, Float[][]> savedResults, int K) {
		Preconditions.checkNotNull(builder);

		assert (numThreads > 0);
		assert (numHits > 0);

		mBuilder = builder;
		mExpander = expander;
		mThreadPool = Executors.newFixedThreadPool(numThreads);
		mQueryResults = Maps.newLinkedHashMap();
		mNumHits = numHits;
		savedResults_prevStage = savedResults;
		mK = K;
	}

	/**
	 * Runs a query asynchronously. Results can be fetched using
	 * {@link getResults}.
	 */
	public void runQuery(String qid, String[] query) {
		Preconditions.checkNotNull(qid);
		Preconditions.checkNotNull(query);

		Future<Accumulator[]> future = mThreadPool.submit(new ThreadTask(query, mBuilder,
				mExpander, qid, mNumHits));
		mQueryResults.put(qid, future);
	}

	/**
	 * Runs a query synchronously, waiting until completion.
	 */
	public Accumulator[] runQuery(String[] query) {
		Preconditions.checkNotNull(query);

		Future<Accumulator[]> future = mThreadPool.submit(new ThreadTask(query, mBuilder,
				mExpander, "query", mNumHits));
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
			return mQueryResults.get(qid).get();
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
		mQueryResults.clear();
	}

	/**
	 * Returns results of all queries executed.
	 */
	public Map<String, Accumulator[]> getResults() {
		Map<String, Accumulator[]> results = Maps.newLinkedHashMap();
		for (Map.Entry<String, Future<Accumulator[]>> e : mQueryResults.entrySet()) {
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


	//return cascade cost for all queries
	public float[] getCascadeCostAllQueries(){
		return cascadeCostAllQueries;
	}

	public float[] getCascadeCostAllQueries_lastStage(){
		return cascadeCostAllQueries_lastStage;
	}

	// Thread for running a query.  No need to expose implementation.
	private class ThreadTask implements Callable<Accumulator[]> {
		private String[] mQuery;
		private MRFBuilder mBuilder;
		private MRFExpander mExpander;
		private String mQid;
		private int mNumHits;

		public ThreadTask(String[] query, MRFBuilder builder, MRFExpander expander, String qid, int numHits) {
			mQuery = query;
			mBuilder = builder;
			mExpander = expander;
			mQid = qid;
			mNumHits = numHits;
		}

		public Accumulator[] call() {
			try {
				long startTime;
				long endTime;

				startTime = System.currentTimeMillis();

				// Build the MRF for this query.
				Object r = savedResults_prevStage.get(mQid);
				float [][] savedResults = null; //store docno and score
				if (r!=null){
					savedResults = (float[][]) r;
				}
				MarkovRandomField mrf = mBuilder.buildMRF(mQuery); 

				// Run initial query, if necessary.
				Accumulator[] results = null;

				float cascadeCost = -1;
				float cascadeCost_lastStage = -1;

				if (mrf.getCliques().size()==0){
				}

				else{
					if (RetrievalEnvironment.mIsNewModel){

						CascadeEval ranker = new CascadeEval (mrf, mNumHits, mQid, savedResults, mK);

						// Rank the documents using the cascade model. 
                        	                results = ranker.rank();
						cascadeCost = ranker.getCascadeCost();
					}

					else{	
						// Retrieve documents using this MRF.
						MRFDocumentRanker ranker = new MRFDocumentRanker(mrf, mNumHits);

						if (mExpander != null) {
							results = ranker.rank();
						}

						// Perform pseudo-relevance feedback, if requested.
						if (mExpander != null) {
							// Get expanded MRF.
							MarkovRandomField expandedMRF = mExpander.getExpandedMRF(mrf, results);

							// Re-rank documents according to expanded MRF.
							ranker = new MRFDocumentRanker(expandedMRF, mNumHits);
						}
	
						// Rank the documents.
						results = ranker.rank();

						//cascadeCost = ranker.getCost();
						cascadeCost = -1; //todo: later
				
					}
				}
				endTime = System.currentTimeMillis();
				sLogger.info("Processed query " + mQid + " in " + (endTime - startTime) + " ms.");

				//This stores the cascade cost for this query using the model represented by modelID
				if (cascadeCost != -1){
					//String key = BatchQueryRunner.model_ID + " "+mQid;
					//BatchQueryRunner.cascadeCosts.put(key, cascadeCost+"");

					cascadeCostAllQueries[Integer.parseInt(mQid)] = cascadeCost;
				}

				if (cascadeCost_lastStage!=-1){
					cascadeCostAllQueries_lastStage[Integer.parseInt(mQid)] = cascadeCost_lastStage;
				}

				return results;
			} catch (ConfigurationException e) {
				e.printStackTrace();
				sLogger.error(e.getMessage());
				
				return null;
			}
		}
	}
}
