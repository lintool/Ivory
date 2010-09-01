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
import ivory.http.server.BrokerQueryServlet;
import ivory.smrf.retrieval.Accumulator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
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
public class QueryServlet extends HttpServlet {
	private static final long serialVersionUID = -5998786589277554550L;
	private static final Logger sLogger = Logger.getLogger(QueryServlet.class);

	public static final String ACTION = "/search";
	public static final String QUERY_FIELD = "query";

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
		sLogger.info("Triggered servlet for running queries");
		res.setContentType("text/html");
		PrintWriter out = res.getWriter();

		String query = null;
		if (req.getParameterValues("query") != null)
			query = req.getParameterValues("query")[0];

		sLogger.info("Raw query: " + query);

		StringBuffer sb = new StringBuffer();
		sb.append("<html><head><title>Threaded Broker Results</title></head>\n<body>");

		long startTime = System.currentTimeMillis();
		OneServer[] servers = new OneServer[serverAddresses.length];
		Thread[] threads = new Thread[serverAddresses.length];
		for (int i = 0; i < serverAddresses.length; i++) {
			servers[i] = new OneServer(serverAddresses[i], query);
			threads[i] = new Thread(servers[i]);
			threads[i].start();
		}
		try {
			for (Thread thread : threads) {
				thread.join();
			}
			sLogger.info("All servers: done.");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Accumulator[] results = new Accumulator[0];
		for (int i = 0; i < servers.length; i++) {
			// results = mergeScores(results, servers[i].getResults());
			Accumulator[] serverResults = servers[i].getZNormalizedResults();
			for (Accumulator a : serverResults)
				docnoToServerMapping.put(a.docid, i);
			results = mergeScores(results, servers[i].getZNormalizedResults());
		}

		sb.append("<ol>");
		for (Accumulator a : results) {
			sb.append("<li>docno <a href=" + BrokerFetchServlet.formatRequestURL(a.docid) + ">"
					+ a.docid + "</a> (" + a.score + ")</li>\n");
		}
		sb.append("</ol>");
		sb.append("</body></html>\n");

		long endTime = System.currentTimeMillis();
		sLogger.info("query execution time (ms): " + (endTime - startTime));

		out.println(sb.toString());
		out.close();
	}

	private Accumulator[] mergeScores(Accumulator[] iScores, Accumulator[] jScores) {
		// assuming that scored documents are mutual exclusive
		Accumulator[] results = new Accumulator[iScores.length + jScores.length];
		int i = 0, j = 0, k = 0;
		while (i < iScores.length && j < jScores.length) {
			if (iScores[i].score > jScores[j].score) {
				results[k] = iScores[i];
				i++;
			} else {
				results[k] = jScores[j];
				j++;
			}
			k++;
		}
		while (i < iScores.length) {
			results[k] = iScores[i];
			i++;
			k++;
		}

		while (j < jScores.length) {
			results[k] = jScores[j];
			j++;
			k++;
		}

		return results;
	}

	private static class OneServer implements Runnable {

		String address;
		String query;
		String textResults = null;

		public OneServer() {
		}

		public OneServer(String addr, String q) {
			address = addr;
			query = q;
		}

		public void set(String addr, String q) {
			address = addr;
			query = q;
		}

		public String getTextResults() {
			return textResults;
		}

		public Accumulator[] getZNormalizedResults() {
			double sum = 0, sumSq = 0;
			if (textResults == null)
				return null;
			String[] lines = textResults.split("\t");
			Accumulator[] results = new Accumulator[lines.length / 2];
			int i = 0;
			int j = 0;
			while (i < lines.length) {
				int docid = -1;
				try {
					docid = Integer.parseInt(lines[i]);
				} catch (NumberFormatException e) {
					i++;
					continue;
				}
				i++;
				double score = Double.parseDouble(lines[i]);
				sum += score;
				sumSq += score * score;
				i++;
				results[j] = new Accumulator(docid, score);
				j++;

			}
			int n = results.length;
			double muo = sum / n;
			double sigma = Math.sqrt((sumSq - n * muo) / (n - 1));

			for (Accumulator a : results) {
				a.score = (a.score - muo) / sigma;
			}
			return results;
		}

		public Accumulator[] getMaxMinNormalizedResults() {
			double min = Double.MAX_VALUE, max = Double.MIN_VALUE;
			if (textResults == null)
				return null;
			String[] lines = textResults.split("\t");
			Accumulator[] results = new Accumulator[lines.length / 2];
			int i = 0;
			int j = 0;
			while (i < lines.length) {
				int docid = -1;
				try {
					docid = Integer.parseInt(lines[i]);
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					i++;
					continue;
				}
				i++;
				double score = Double.parseDouble(lines[i]);
				if (score > max)
					max = score;
				else if (score < min)
					min = score;
				i++;
				results[j] = new Accumulator(docid, score);
				j++;

			}
			double d = max - min;
			for (Accumulator a : results) {
				a.score = (a.score - min) / d;
			}
			return results;
		}

		public void run() {
			try {
				String url = "http://" + address + BrokerQueryServlet.ACTION + "?"
						+ BrokerQueryServlet.QUERY_FIELD + "=" + query.replaceAll(" ", "+");

				sLogger.info("fetching " + url);

				textResults = HttpUtils.fetchURL(new URL(url));
				sLogger.info(Thread.currentThread().getName() + "-" + address + ": done.");
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}

	}

}
