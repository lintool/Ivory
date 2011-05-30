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
import java.util.Iterator;

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
 * @author Lidan Wang 
 */
public class RankedListEvaluator_new {

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

			//TODO: add NDCG here
		}

		return results;
	}


	public static double computeDCG (int n, double [] gain){
		double score = 0;

		for (int i=0; i<gain.length && i < n; i++){
			score += (Math.pow(2.0, gain[i]) - 1)/(Math.log(i+2) / Math.log(2));
		}

		return score;
	}

	/**
         * Computes ndcg (NDCG) of a ranked list (1 query only !!)
         * 
         * @param docs
         *            the list of results
         * @param reldocs
         *            set of the ids of the relevant documents
         * @return ndcg
         */

	public static double computeNDCG(int n, Accumulator[] docs, DocnoMapping mapping, Set<String> reldocs){
		if (reldocs.size()==0){
			return 0d;
		}

		double [] ratings = new double[reldocs.size()];
		double [] ratings_2 = new double[docs.length];

		HashMap doc_to_rating = new HashMap();
		
		int cnt = 0;
		int [] order = new int[reldocs.size()];

		for (Iterator<String> it = reldocs.iterator(); it.hasNext(); ){
			String s = it.next();			

			String [] tokens = s.trim().split("\\s+");
			if (tokens.length!=2){
				System.out.println("Should have both docid and rating for this relevant doc!");
				System.exit(-1);
			}

			int rating = Integer.parseInt(tokens[1]);
			doc_to_rating.put(tokens[0], rating+"");

			if (rating == 0){
				System.out.println("Should only have relevant documents here.");
				System.exit(-1);
			}
			ratings[cnt] = rating;
			cnt++;
		}

		ivory.smrf.model.constrained.ConstraintModel.Quicksort(ratings, order, 0, order.length-1);

		double [] ratings_descending = new double[ratings.length];
		for (int i=0; i<ratings_descending.length; i++){
			ratings_descending[i] = ratings[ratings.length-i-1];		
		}

		double idealDCG = computeDCG (n, ratings_descending);


		for (int i=0; i<docs.length; i++){
			String docno = mapping.getDocid(docs[i].docno);
			String r = (String)(doc_to_rating.get(docno));
			ratings_2[i] = 0;
			if (r!=null){
				ratings_2[i] = Integer.parseInt(r);
			}
		}
		double dcg = computeDCG (n, ratings_2);

		return roundTo4SigFigs(dcg/idealDCG);
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
		return ((double) Math.round(d * 10000.0) / 10000.0);
	}

}
