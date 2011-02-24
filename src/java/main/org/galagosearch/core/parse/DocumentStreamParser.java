// BSD License (http://www.galagosearch.org/license)


package org.galagosearch.core.parse;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface DocumentStreamParser {
    public Document nextDocument() throws IOException;
}
