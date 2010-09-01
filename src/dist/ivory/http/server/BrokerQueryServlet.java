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

package ivory.http.server;

import ivory.smrf.retrieval.Accumulator;
import ivory.smrf.retrieval.QueryRunner;
import ivory.util.RetrievalEnvironment;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * @author Tamer Elsayed
 */
public class BrokerQueryServlet extends HttpServlet {
	private static final long serialVersionUID = -5998786589277554550L;
	private static final Logger sLogger = Logger.getLogger(BrokerQueryServlet.class);

	public static final String ACTION = "/BrokerQuery";
	public static final String QUERY_FIELD = "query";

	private QueryRunner sQueryRunner = null;
	private RetrievalEnvironment sEnv = null;

	public void setQueryRunnerAndRetrievalEnv(QueryRunner queryRunner, RetrievalEnvironment env) {
		sQueryRunner = queryRunner;
		sEnv = env;

	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		doPost(req, res);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		sLogger.info("Broker triggered servlet for running queries");
		res.setContentType("text/html");

		String query = null;
		if (req.getParameterValues(QUERY_FIELD) != null)
			query = req.getParameterValues(QUERY_FIELD)[0];

		sLogger.info("Broker raw query: " + query);

		long startTime = System.currentTimeMillis();

		String[] queryTokens = sEnv.tokenize(query);
		StringBuffer queryText = new StringBuffer();
		for (String token : queryTokens) {
			queryText.append(token);
			queryText.append(" ");
		}

		String tokenizedQuery = queryText.toString();
		sLogger.info("Tokenized query: " + tokenizedQuery);

		// run the query
		Accumulator[] results = sQueryRunner.runQuery(tokenizedQuery);
		long endTime = System.currentTimeMillis();

		sLogger.info("query execution time (ms): " + (endTime - startTime));

		StringBuffer sb = new StringBuffer();
		for (Accumulator a : results) {
			sb.append(a.docid + "\t" + a.score + "\t");
		}
		PrintWriter out = res.getWriter();
		out.print(sb.toString().trim());
		out.close();
	}

}