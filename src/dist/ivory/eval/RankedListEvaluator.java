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

package ivory.eval;

import ivory.smrf.retrieval.Accumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * <p>
 * A bundle of static methods for evaluating ranked lists. The class computes
 * standard ranked retrieval metrics such as average precision (AP), precision
 * at <i>N</i> documents (PN), etc.
 * </p>
 * 
 * <p>
 * Implemented metrics:
 * </p>
 * 
 * <ul>
 * <li><code>R-Prec</code>: R-Precision</li>
 * 
 * <li><code>PN</code>: precision at N, where N can be any value (e.g.,
 * "P5", "P10", etc.)</li>
 * 
 * <li><code>recall</code>: recall</li>
 * 
 * <li><code>APN</code>: average precision at N, where N can be any value
 * (e.g., "AP50" for AP at 50 hits)</li>
 * 
 * <li><code>AP</code>: default average precision, on entire ranked list.</li>
 * 
 * <li><code>num_ret</code>: number of hits returned.</li>
 * 
 * <li><code>num_rel</code>: number of relevant hits retrieved.</li>
 * </ul>
 * 
 * @author Jimmy Lin
 * 
 */
public class RankedListEvaluator {

	private static Logger logger = Logger.getLogger(RankedListEvaluator.class);

	public static Map<String, Double> evaluate(Accumulator[] docs, DocnoMapping mapping,
			Set<String> reldocs, String... metrics) {
		Map<String, Double> results = new HashMap<String, Double>();

		for (int i = 0; i < metrics.length; i++) {
			if (metrics[i].equals("AP")) {
				results.put("AP", computeAP(docs, mapping, reldocs));
			} else if (metrics[i].matches("AP\\d+")) {
				int n = Integer.parseInt(metrics[i].substring(3));
				results.put(metrics[i], computeAP(n, docs, mapping, reldocs));
			} else if (metrics[i].equals("R-Prec")) {
				results.put("R-Prec", computeRPrecision(docs, mapping, reldocs));
			} else if (metrics[i].matches("P\\d+")) {
				int n = Integer.parseInt(metrics[i].substring(1));
				results.put(metrics[i], computePN(n, docs, mapping, reldocs));
			} else if (metrics[i].equals("recall")) {
				results.put("recall", computeRecall(docs, mapping, reldocs));
			} else if (metrics[i].equals("num_ret")) {
				results.put("num_ret", computeNumRetrieved(docs, mapping, reldocs));
			} else if (metrics[i].equals("num_rel")) {
				results.put("num_rel", computeNumRelevant(docs, mapping, reldocs));
			} else {
				logger.warn("Warning: Unknown metric '" + metrics[i] + "'");
			}
		}

		return results;
	}

	/**
	 * Computes average precision (AP) of a ranked list.
	 * 
	 * @param docs
	 *            the list of results
	 * @param reldocs
	 *            set of the ids of the relevant documents
	 * @return average precision
	 */
	public static double computeAP(int n, Accumulator[] docs, DocnoMapping mapping,
			Set<String> reldocs) {
		if (reldocs.size() == 0)
			return 0d;

		double sum = 0.0d;
		int num_rel_ret = 0;

		for (int i = 0; i < docs.length && i < n; i++) {
			String docno = mapping.getDocid(docs[i].docno);

			if (reldocs.contains(docno)) {
				num_rel_ret++;
				double prec = num_rel_ret / (double) (i + 1);
				sum += prec;
			}
		}

		return roundTo4SigFigs(sum / (double) reldocs.size());
	}

	public static double computeAP(Accumulator[] docs, DocnoMapping mapping, Set<String> reldocs) {
		return computeAP(Integer.MAX_VALUE, docs, mapping, reldocs);
	}

	public static double computeNumRetrieved(Accumulator[] docs, DocnoMapping mapping,
			Set<String> reldocs) {
		return (double) docs.length;
	}

	public static double computeNumRelevant(Accumulator[] docs, DocnoMapping mapping,
			Set<String> reldocs) {
		int cnt = 0;
		for (int i = 0; i < docs.length; i++) {
			String docno = mapping.getDocid(docs[i].docno);
			if (reldocs.contains(docno))
				cnt++;
		}

		return (double) cnt;
	}

	/**
	 * Computes recall of a ranked list.
	 * 
	 * @param docs
	 *            the list of results
	 * @param reldocs
	 *            set of the ids of the relevant documents
	 * @return recall
	 */
	public static double computeRecall(Accumulator[] docs, DocnoMapping mapping, Set<String> reldocs) {
		int cnt = 0;
		for (int i = 0; i < docs.length; i++) {
			String docno = mapping.getDocid(docs[i].docno);
			if (reldocs.contains(docno))
				cnt++;
		}

		return (double) cnt / (double) reldocs.size();
	}

	/**
	 * Computes precision at <i>N</i> documents (PN), where <i>N</i> is a
	 * user-specified value.
	 * 
	 * @param n
	 *            the number of hits to consider
	 * @param docs
	 *            the list of results
	 * @param reldocs
	 *            set of the ids of the relevant documents
	 * @return precision at <i>N</i>
	 */
	public static double computePN(int n, Accumulator[] docs, DocnoMapping mapping,
			Set<String> reldocs) {
		if (docs.length < n) {
			logger.warn("Warning: less than " + n + " hits in an attempt to computer P" + n);
		}

		int cnt = 0;
		for (int i = 0; i < n && i < docs.length; i++) {
			String docno = mapping.getDocid(docs[i].docno);
			if (reldocs.contains(docno))
				cnt++;
		}

		return (double) cnt / (double) n;
	}

	/**
	 * Computes R-Precision, which is defined as precision at <i>R</i>, where
	 * <i>R</i> is equal to the number of relevant documents.
	 * 
	 * @param docs
	 *            the list of results
	 * @param reldocs
	 *            set of the ids of the relevant documents
	 * @return R-Precision
	 */
	public static double computeRPrecision(Accumulator[] docs, DocnoMapping mapping,
			Set<String> reldocs) {
		if (reldocs.size() == 0)
			return 0.0d;

		return computePN(reldocs.size(), docs, mapping, reldocs);
	}

	/**
	 * Returns a double value to four significant figures. This follows the NIST
	 * program <code>trec_eval</code>, which reports values to four
	 * significant figures.
	 * 
	 * @param d
	 *            the value to round.
	 * @return the value rounded to four significant figures
	 */
	public static double roundTo4SigFigs(double d) {
		return ((double) Math.round(d * 10000) / 10000);
	}

}
