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

import ivory.server.HttpUtils;
import ivory.server.RunRetrievalBroker;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author Tamer Elsayed
 */
public class BrokerQueryRunner implements QueryRunner {
	private static final Logger sLogger = Logger.getLogger(BrokerQueryRunner.class);

	private Map<String, Accumulator[]> mQueryResults;
	private Map<String, Map<Integer, String>> mAllQueriesDocnoMapping;
	private String mBrokerAddress;
	private Map<Integer, String> mSingleQueryDocnoMapping;

	public BrokerQueryRunner(String brokerAddress) {
		mQueryResults = new HashMap<String, Accumulator[]>();
		mAllQueriesDocnoMapping = new HashMap<String, Map<Integer, String>>();

		this.mBrokerAddress = brokerAddress;
	}

	public void clearResults() {
		mQueryResults.clear();
	}

	public Accumulator[] getResults(String qid) {
		return mQueryResults.get(qid);
	}

	public Map<String, Accumulator[]> getResults() {
		// TODO Auto-generated method stub
		return mQueryResults;
	}

	private static String join(String[] terms, String sep) {
		StringBuilder sb = new StringBuilder();
		
		for ( int i=0; i<terms.length; i++ ) {
			sb.append(terms[i]);
			if ( i<terms.length-1)
				sb.append(sep);
		}
		
		return sb.toString();
	}

	public Accumulator[] runQuery(String[] query) {
		Accumulator[] results = null;
		sLogger.info("Contacting broker for query: " + query);
		try {
			String url = "http://" + mBrokerAddress
					+ RunRetrievalBroker.PlainTextQueryServlet.ACTION + "?"
					+ RunRetrievalBroker.PlainTextQueryServlet.QUERY_FIELD + "="
					+ join(query, "+");

			sLogger.info("fetching " + url);

			String textResults = HttpUtils.fetchURL(new URL(url));
			// sLogger.info("Results\n"+textResults);
			HashMap<Integer, String> m = new HashMap<Integer, String>();
			results = getAccumulator(textResults, m);
			this.mSingleQueryDocnoMapping = m;
			sLogger.info("Query done.");

		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return results;
	}

	private Accumulator[] getAccumulator(String textResults, Map<Integer, String> docnoMapping) {
		String[] lines = textResults.split("\n");

		ArrayList<Accumulator> r = new ArrayList<Accumulator>();
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].trim().isEmpty()) {
				if (i != lines.length - 1 && i != 0) {
					sLogger.error("Empty line NOT at the beginning nor the end !!! at line: " + i);
				}
				continue;
			}
			String[] s = lines[i].split("\t");

			r.add(new Accumulator(Integer.parseInt(s[0]), Double.parseDouble(s[1])));
			docnoMapping.put(Integer.parseInt(s[0]), s[2]);
		}
		Accumulator[] results = new Accumulator[r.size()];
		for (int i = 0; i < r.size(); i++)
			results[i] = r.get(i);
		return results;
	}

	public Map<Integer, String> getDocnoMapping(String qid) {
		return mAllQueriesDocnoMapping.get(qid);
	}

	public void runQuery(String qid, String[] query) {
		Accumulator[] results = runQuery(query);
		this.mQueryResults.put(qid, results);
		mAllQueriesDocnoMapping.put(qid, mSingleQueryDocnoMapping);
	}

}
