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

package ivory.http.broker;

import ivory.http.HttpUtils;
import ivory.http.server.FetchDocnoServlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * @author Tamer Elsayed
 */
public class BrokerFetchServlet extends HttpServlet {
	private static final long serialVersionUID = -5998986589277554550L;
	private static final Logger sLogger = Logger.getLogger(BrokerFetchServlet.class);

	public static final String ACTION = "/BrokerFetch";
	public static final String DOCNO_FIELD = "docno";

	private String[] serverAddresses;

	private HashMap<Integer, Integer> docnoToServerMapping = null;

	public void setRetrievalServerAddresses(String[] addresses) {
		serverAddresses = addresses;
	}

	public void setDocMapping(HashMap<Integer, Integer> mapping) {
		docnoToServerMapping = mapping;
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		doPost(req, res);
	}

	public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException,
			IOException {
		sLogger.info("Triggered servlet for fetching a document");
		res.setContentType("text/html");
		PrintWriter out = res.getWriter();

		String docno = null;
		if (req.getParameterValues(DOCNO_FIELD) != null)
			docno = req.getParameterValues(DOCNO_FIELD)[0];

		sLogger.info("Raw query: " + docno);

		Integer serverNo = docnoToServerMapping.get(Integer.parseInt(docno));
		if (serverNo == null) {
			sLogger.info("document not found in results/mapping-table!!");
			return;
		}

		long startTime = System.currentTimeMillis();
		String document = HttpUtils.fetchURL(new URL("http://" + this.serverAddresses[serverNo]
				+ FetchDocnoServlet.ACTION + "?" + FetchDocnoServlet.DOCNO_FIELD + "=" + docno));
		long endTime = System.currentTimeMillis();
		sLogger.info("document fetched in time (ms): " + (endTime - startTime));
		out.println(document);
		out.close();
	}

	public static String formatRequestURL(int docno) {
		return ACTION + "?" + DOCNO_FIELD + "=" + new Integer(docno).toString();
	}
}