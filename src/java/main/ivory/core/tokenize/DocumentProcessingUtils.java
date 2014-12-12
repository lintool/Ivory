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

package ivory.core.tokenize;

import ivory.core.data.dictionary.Dictionary;
import ivory.core.data.document.TermDocVector;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfInts;

import com.google.common.collect.Maps;

import edu.umd.cloud9.collection.Indexable;

/**
 * @author Tamer Elsayed
 * @author Jimmy Lin
 */
public class DocumentProcessingUtils {
  private static final Logger LOG = Logger.getLogger(DocumentProcessingUtils.class);

  public static short TF_CUT = Short.MAX_VALUE;

  public static SortedMap<Integer, int[]> integerizeTermDocVector(TermDocVector doc,
      Dictionary termIDMap) {
    SortedMap<Integer, int[]> positions = Maps.newTreeMap();

    TermDocVector.Reader reader = null;
    try {
      reader = doc.getReader();
    } catch (IOException e1) {
      throw new RuntimeException("Error getting TermDocVectorReader: " + e1.getMessage());
    }

    while (reader.hasMoreTerms()) {
      int termid = termIDMap.getId(reader.nextTerm());
      if (termid <= 0) {
        continue;
      }

      positions.put(termid, reader.getPositions());
    }

    return positions;
  }

  public static Map<String, ArrayListOfInts> parseDocument(Indexable doc, Tokenizer tokenizer) {
    Map<String, ArrayListOfInts> positions = Maps.newHashMap();

    String text = doc.getContent();
    String[] terms = tokenizer.processContent(text);

    // The tokenizer may return terms with zero length (empty terms), and the tf may exceed the
    // capacity of a short (in which case we need to handle separately).

    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      
      // Guard against bad tokenization
      if (term.length() == 0 || term.length() >= Byte.MAX_VALUE) {
        continue;
      }

      // Remember, token position is numbered started from one...
      if (positions.containsKey(term)) {
        positions.get(term).add(i + 1);
      } else {
        ArrayListOfInts l = new ArrayListOfInts();
        l.add(i + 1);
        positions.put(term, l);
      }
    }

    int doclength = 0;
    Iterator<Map.Entry<String, ArrayListOfInts>> it = positions.entrySet().iterator();
    Map.Entry<String, ArrayListOfInts> e;
    ArrayListOfInts positionsList;
    while (it.hasNext()) {
      e = it.next();
      positionsList = e.getValue();

      // We're storing tfs as shorts, so check for overflow...
      if (positionsList.size() >= TF_CUT) {
        // There are a few ways to handle this... If we're getting such a high tf, then it most
        // likely means that this is a junk doc.
        LOG.warn("Error: tf of " + e.getValue()
            + " will overflow max short value. docno=" + doc.getDocid() + ", term="
            + e.getKey());
        it.remove();
      } else {
        positionsList.trimToSize();
        doclength += positionsList.size();
      }
    }

    if ( positions.size() == 0 ) {
      return positions;
    }

    // We're going to stick the doclength here as a special case.
    positions.put("", new ArrayListOfInts(new int[] { doclength }));
    return positions;
  }
}
