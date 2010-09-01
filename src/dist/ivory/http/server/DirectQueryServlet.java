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
public class DirectQueryServlet extends HttpServlet {
	private static final long serialVersionUID = -5998786589277554550L;

	private static final Logger sLogger = Logger.getLogger(DirectQueryServlet.class);

	public static final String ACTION = "/DirectQuery";
	public static final String QUERY_FIELD = "query";

	private QueryRunner sQueryRunner = null;
	private RetrievalEnvironment sEnv = null;

	public void set(QueryRunner queryRunner, RetrievalEnvironment env) {
		sQueryRunner = queryRunner;
		sEnv = env;
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		doPost(req, res);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		sLogger.info("Triggered servlet for direct querying");
		res.setContentType("text/html");
		PrintWriter out = res.getWriter();

		String query = null;
		if (req.getParameterValues(QUERY_FIELD) != null)
			query = req.getParameterValues(QUERY_FIELD)[0];

		sLogger.info("Raw query: " + query);

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
		sb.append("<html><head><title>[Server] Results</title></head>\n<body>");

		sb.append("<ol>");
		for (Accumulator a : results) {
			sb.append("<li>docno <a href=" + FetchDocnoServlet.formatRequestURL(a.docid) + ">"
					+ a.docid + "</a> (" + a.score + ")</li>\n");
		}
		sb.append("</ol>");
		sb.append("</body></html>\n");

		out.print(sb.toString());

		out.close();
	}

}