// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class TrecWebParser implements DocumentStreamParser {
    BufferedReader reader;

    /** Creates a new instance of TrecWebParser */
    public TrecWebParser(BufferedReader reader) throws FileNotFoundException, IOException {
        this.reader = reader;
    }

    public String waitFor(String tag) throws IOException {
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith(tag)) {
                return line;
            }
        }

        return null;
    }

    public void close() throws IOException {
        reader.close();
        reader = null;
    }

    public String scrubUrl(String url) {
        // remove a trailing pound sign
        if (url.charAt(url.length() - 1) == '#') {
            url = url.substring(0, url.length() - 1);        // make it lowercase
        }
        url = url.toLowerCase();

        // remove a port number
        url = url.replace(":80/", "/");
        if (url.endsWith(":80")) {
            url = url.replace(":80", "");        // remove trailing slashes
        }
        while (url.charAt(url.length() - 1) == '/') {
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }

    public String readUrl() throws IOException {
        String url = reader.readLine();
        int space = url.indexOf(' ');

        if (space < 0) {
            space = url.length();
        }
        return scrubUrl(url.substring(0, space));
    }

    public Document nextDocument() throws IOException {
        String line = null;

        if (null == waitFor("<DOC>")) {
            close();
            return null;
        }

        String identifier = waitFor("<DOCNO>");
        identifier = identifier.substring(7).trim();
        identifier = identifier.substring(0, identifier.length() - 8);
        identifier = new String(identifier.trim());
        waitFor("<DOCHDR>");
        String url = readUrl();
        waitFor("</DOCHDR>");

        StringBuilder buffer = new StringBuilder(20 * 1024);

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("</DOC>")) {
                break;
            }
            buffer.append(line);
            buffer.append('\n');
        }

        Document result = new Document(identifier, buffer.toString());
        result.metadata.put("url", new String(url));
        result.metadata.put("identifier", result.identifier);

        return result;
    }
}
