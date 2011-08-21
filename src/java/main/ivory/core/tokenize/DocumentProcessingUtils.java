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
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * @author Tamer
 */
public class DocumentProcessingUtils {
	private static final Logger sLogger = Logger.getLogger(DocumentProcessingUtils.class);
	static{
		sLogger.setLevel(Level.WARN);
	}

	public static short TF_CUT = Short.MAX_VALUE;

	// TODO: refactor this class to get rid of duplicate code
	public static int getDocLengthFromPositionsMap(Map<String, ArrayListOfInts> termPositionsMap){
		int dl = 0;
		for (Map.Entry<String, ArrayListOfInts> e : termPositionsMap.entrySet()) {
			dl += e.getValue().size();
		}
		return dl;
	}
	
	public static TreeMap<Integer, int[]> getTermIDsPositionsMap(TermDocVector doc, DefaultCachedFrequencySortedDictionary termIDMap){
		// for storing the positions
		Map<String, int[]> strPositions = new HashMap<String, int[]>();
		
		TermDocVector.Reader r = null;
		try {
			r = doc.getReader();
		} catch (IOException e1) {
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}
		String term;
		int[] tp;
		while(r.hasMoreTerms()){
			term = r.nextTerm();
			//TermPositions tp = new TermPositions();
			tp = r.getPositions();
			strPositions.put(term, tp);
		}
		// for storing the positions
		TreeMap<Integer, int[]> positions = new TreeMap<Integer, int[]>();
		
		Iterator<Map.Entry<String, int[]>> it = strPositions.entrySet().iterator();
		Map.Entry<String, int[]> e;
		int[] positionsList;
		int id;
		while(it.hasNext()){
			e = it.next();
			positionsList = e.getValue(); // positions.get(e.getKey());
			id = termIDMap.getId(e.getKey());
			if(id <= 0)	continue;
			positions.put(id, positionsList);
		}
		return positions;
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

	public static TreeMap<String, ArrayListOfInts> getTermPositionsMap(TermDocVector doc, Set<String> terms){
		// for storing the positions
		TreeMap<String, ArrayListOfInts> strPositions = new TreeMap<String, ArrayListOfInts>();
		
		TermDocVector.Reader r = null;
		try {
			r = doc.getReader();
		} catch (IOException e1) {
			throw new RuntimeException("Error getting TermDocVectorReader: "+e1.getMessage());
		}
		String term;
		int[] tp;
		while(r.hasMoreTerms()){
			term = r.nextTerm();
			//TermPositions tp = new TermPositions();
			tp = r.getPositions();
			if(terms.contains(term))
				strPositions.put(term, new ArrayListOfInts(tp));
		}
		return strPositions;
	}
}
