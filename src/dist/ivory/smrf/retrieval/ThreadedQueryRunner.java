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

package ivory.smrf.retrieval;

import ivory.smrf.model.builder.MRFBuilder;
import ivory.smrf.model.expander.MRFExpander;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * @author Don Metzler
 *
 */
public class ThreadedQueryRunner {

	/**
	 * MRF builder
	 */
	private MRFBuilder mBuilder;

	/**
	 * MRF expander
	 */
	private MRFExpander mExpander;

	/**
	 * thread pool
	 */
	private ExecutorService mThreadPool;
	
	/**
	 * maps query ids to query results
	 */
	private Map<String,Future<Accumulator []>> mQueryResults;
	
	public ThreadedQueryRunner(MRFBuilder builder, MRFExpander expander) {
		mBuilder = builder;
		mExpander = expander;
		mThreadPool = Executors.newFixedThreadPool(1);
		mQueryResults = new HashMap<String,Future<Accumulator []>>();
	}

	public ThreadedQueryRunner(MRFBuilder builder, MRFExpander expander, int numThreads) {
		mBuilder = builder;
		mExpander = expander;
		mThreadPool = Executors.newFixedThreadPool(numThreads);
		mQueryResults = new HashMap<String,Future<Accumulator []>>();
	}
	
	public void runQuery(String qid, String query) {
		Future<Accumulator []> future = mThreadPool.submit(new QueryRunnerTask(query, mBuilder, mExpander));
		mQueryResults.put(qid, future);
	}
	
	/**
	 * @param qid
	 */
	public Accumulator [] getResults(String qid) {
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
}
