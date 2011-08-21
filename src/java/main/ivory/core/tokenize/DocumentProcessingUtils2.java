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

package ivory.core.tokenize;


import ivory.core.data.dictionary.DefaultCachedFrequencySortedDictionary;
import ivory.core.data.document.TermDocVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * @author Tamer
 */
public class DocumentProcessingUtils2 {
	private static final Logger sLogger = Logger.getLogger(DocumentProcessingUtils2.class);

	public static short TF_CUT = Short.MAX_VALUE;

	public static class AnalyzedIntDocument {
		private final TreeMap<Integer, int[]> termPositions = new TreeMap<Integer, int[]>();

		public void put(int termid, int[] positions) {
			termPositions.put(termid, positions);
		}

		public void clear() {
			termPositions.clear();
		}

		public TreeMap<Integer, int[]> getPositions() {
			return termPositions;
		}
	}

	public static void getTermIDsPositionsMap(TermDocVector doc, DefaultCachedFrequencySortedDictionary termidMap, AnalyzedIntDocument analyzed){
		TermDocVector.Reader r;
		try {
			r = doc.getReader();
		} catch (IOException e1) {
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}

		analyzed.clear();
		while(r.hasMoreTerms()){
			analyzed.put(termidMap.getId(r.nextTerm()), r.getPositions());
		}
	}

	public static class AnalyzedTermDocument {
		private final Map<String, ArrayListOfInts> termPositions = new HashMap<String, ArrayListOfInts>();
		private int doclength = 0;

		public void setDocLength(int d) {
			doclength = d;
		}

		public int getDocLength() {
			return doclength;
		}

		public Iterator<Map.Entry<String, ArrayListOfInts>> iterator() {
			return termPositions.entrySet().iterator();
		}

		public Map<String, ArrayListOfInts> getTermPositionsMap() {
			return termPositions;
		}

		public void set(String term, ArrayListOfInts positions) {
			termPositions.put(term, positions);
		}

		public boolean containsTerm(String term) {
			return termPositions.containsKey(term);
		}

		public ArrayListOfInts getPositions(String term) {
			return termPositions.get(term);
		}

		public void clear() {
			termPositions.clear();
			doclength = 0;
		}
	}

	public static void analyzeDocument(Indexable doc, Tokenizer mTokenizer, AnalyzedTermDocument analyzed) {
		analyzed.clear();
		
		String text = doc.getContent();
		String[] terms = mTokenizer.processContent(text);

		// the tokenizer may return
		// terms with zero length (empty terms), and the tf may exceed the
		// capacity of a short (in which case we need to handle separately).
		// The doc length and contribution to term count is computed as the
		// sum of all tfs of indexed terms a bit later.

		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];

			// guard against bad tokenization
			if (term.length() == 0 || term.length() >= Byte.MAX_VALUE) {
				continue;
			}
	
			// remember, token position is numbered started from one...
			if (analyzed.containsTerm(term)) {
				analyzed.getPositions(term).add(i + 1);
			} else {
				ArrayListOfInts l = new ArrayListOfInts();
				l.add(i + 1);
				analyzed.set(term, l);
			}
		}

		int doclength = 0;
		Iterator<Map.Entry<String, ArrayListOfInts>> iter = analyzed.iterator();
		Map.Entry<String, ArrayListOfInts> e;
		ArrayListOfInts positions;
		while (iter.hasNext()) {
			e = iter.next();
			positions = e.getValue();
			
			// we're storing tfs as shorts, so check for overflow...
			if (positions.size() >= TF_CUT) {
				// There are a few ways to handle this... If we're getting
				// such a high tf, then it most likely means that this is a
				// junk doc. The current implementation simply skips this
				// posting...
				sLogger.warn("Error: tf of " + e.getValue()
						+ " will overflow max short value. docno=" + doc.getDocid() + ", term="
						+ e.getKey());
				iter.remove();
			} else {
				positions.trimToSize();
				doclength += positions.size();
			}
		}
		analyzed.setDocLength(doclength);
	}

	
	public static Map<String, ArrayListOfInts> getTermPositionsMap(Indexable doc, Tokenizer mTokenizer){
		// for storing the positions
		Map<String, ArrayListOfInts> positions = new HashMap<String, ArrayListOfInts>();
		
		String text = doc.getContent();
		String[] terms = mTokenizer.processContent(text);

		// the tokenizer may return
		// terms with zero length (empty terms), and the tf may exceed the
		// capacity of a short (in which case we need to handle separately).
		// The doc length and contribution to term count is computed as the
		// sum of all tfs of indexed terms a bit later.

		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];

			// guard against bad tokenization
			if (term.length() == 0)
				continue;

			if (term.length() >= Byte.MAX_VALUE)
				continue;
	
			// remember, token position is numbered started from one...
			if (positions.containsKey(term)) {
				positions.get(term).add(i + 1);
			} else {
				ArrayListOfInts l = new ArrayListOfInts();
				l.add(i + 1);
				positions.put(term, l);
			}
		}

		Iterator<Map.Entry<String, ArrayListOfInts>> it = positions.entrySet().iterator();
		Map.Entry<String, ArrayListOfInts> e;
		ArrayListOfInts positionsList;
		while(it.hasNext()){
			e = it.next();
			positionsList = e.getValue(); // positions.get(e.getKey());
			
			// we're storing tfs as shorts, so check for overflow...
			if (positionsList.size() >= TF_CUT) {
				// There are a few ways to handle this... If we're getting
				// such a high tf, then it most likely means that this is a
				// junk doc. The current implementation simply skips this
				// posting...
				sLogger.warn("Error: tf of " + e.getValue()
						+ " will overflow max short value. docno=" + doc.getDocid() + ", term="
						+ e.getKey());
				//continue;
				it.remove();
				
			}
			positionsList.trimToSize();
		}
		return positions;
	}

	public static int getDocLengthFromPositionsMap(Map<String, ArrayListOfInts> termPositionsMap){
		int dl = 0;
		for (Map.Entry<String, ArrayListOfInts> e : termPositionsMap.entrySet()) {
			dl += e.getValue().size();
		}
		return dl;
	}

	public static TreeMap<Integer, int[]> getTermIDsPositionsMap(TermDocVector doc, DefaultCachedFrequencySortedDictionary termidMap){
		TreeMap<Integer, int[]> positions = new TreeMap<Integer, int[]>();

		TermDocVector.Reader r;
		try {
			r = doc.getReader();
		} catch (IOException e1) {
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}

		while(r.hasMoreTerms()){
			positions.put(termidMap.getId(r.nextTerm()), r.getPositions());
		}

		return positions;
	}
}
