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

import ivory.util.DelimitedValuesFileReader;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

/**
 * <p>
 * Representation of relevance judgments. In TREC parlance, qrels are judgments
 * made by humans as to whether a document is relevant to an information need.
 * Typically, qrels are created by a process known as "pooling" in large-scale
 * system evaluations such as those at TREC.
 * </p>
 * 
 * @author Jimmy Lin
 * @author Lidan Wang
 */
public class GradedQrels extends Qrels{

	private Map<String, Map<String, String>> mQrels;

	private float topics = 0;

	/**
	 * Creates a <code>Qrels</code> object from a file
	 * 
	 * @param file
	 *            file that contains the relevance judgments
	 */
	public GradedQrels(String file) {

		super (file);

		//mQrels = new TreeMap<String, Map<String, Boolean>>();

		mQrels = new TreeMap<String, Map<String, String>>();

		DelimitedValuesFileReader iter = null;
		try {
			 iter = new DelimitedValuesFileReader(file, " ");
		} catch (RuntimeException e) {
			// probably means file not found, simply propagate
			throw e;
		}

		String[] arr;
		while ((arr = iter.nextValues()) != null) {
			String qno = arr[0];
			String docno = arr[2];
			//boolean rel = arr[3].equals("0") ? false : true;

			if (mQrels.containsKey(qno)) {
				mQrels.get(qno).put(docno, arr[3]); //rel);
			} else {
				//Map<String, Boolean> t = new HashMap<String, Boolean>();
				Map<String, String> t = new HashMap<String, String>();

				t.put(docno, arr[3]); //rel);
				mQrels.put(qno, t);
			}
		}
	}

	

	/**
	 * Determines if a document is relevant for a particular information need.
	 * 
	 * @param qid
	 *            id of the information need
	 * @param docid
	 *            id of the document to test
	 * @return <code>true</code> if the document is relevant to the
	 *         information need
	 */
	public boolean isRelevant(String qid, String docid) {

		if (!mQrels.containsKey(qid))
			return false;

		if (!mQrels.get(qid).containsKey(docid))
			return false;

		if (mQrels.get(qid).get(docid).equals("0"))
			return false;
		else
			return true;

		//return mQrels.get(qid).get(docid);
	}

	/**
	 * Returns the set of relevant documents for a particular information need.
	 * 
	 * @param qid
	 *            the id of the information need
	 * @return the set of relevant documents
	 */
	public Set<String> getReldocsForQid(String qid, boolean alsoReturnRating) {
		Set<String> set = new TreeSet<String>();

		if (!mQrels.containsKey(qid))
			return set;

		topics++;

		//for (Entry<String, Boolean> e : mQrels.get(qid).entrySet()) {
		for (Entry<String, String> e : mQrels.get(qid).entrySet()) {

			if (!(e.getValue().equals("0"))) {

				if (!alsoReturnRating)
					set.add(e.getKey());

				else{ //for NDCG, need to return rating as well
					set.add(e.getKey()+" "+e.getValue());					
				}
			}
		}

		return set;
	}

}

