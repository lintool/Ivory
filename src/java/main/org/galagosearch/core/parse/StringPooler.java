// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.util.HashMap;

/**
 * The point of this class is to replace strings in document objects with
 * already-used copies.  This can greatly reduce the amount of memory used
 * by the system.
 *
 * @author trevor
 */
public class StringPooler {
    HashMap<String, String> pool = new HashMap<String, String>();

    /**
     * Replaces the strings within this document with strings in a
     * string pool.
     * 
     * @param document
     */
    public void transform(Document document) {
        for (int i = 0; i < document.terms.size(); i++) {
            String term = document.terms.get(i);

            if (term == null) {
                continue;
            }
            String cached = pool.get(term);

            if (cached == null) {
                term = new String(term);
                pool.put(term, term);
            } else {
                term = cached;
            }

            document.terms.set(i, term);
        }

        // The choice of 10000 is arbitrary; it seemed big enough to make a difference.
        if (pool.size() > 10000) {
            pool.clear();
        }
    }
}
