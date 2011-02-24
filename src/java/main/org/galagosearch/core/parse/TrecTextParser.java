// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class TrecTextParser implements DocumentStreamParser {
    BufferedReader reader;

    /** Creates a new instance of TrecTextParser */
    public TrecTextParser(BufferedReader reader) throws FileNotFoundException, IOException {
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

    public String parseDocNumber() throws IOException {
        String allText = waitFor("<DOCNO>");

        while (allText.contains("</DOCNO>") == false) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            allText += line;
        }

        int start = allText.indexOf("<DOCNO>") + 7;
        int end = allText.indexOf("</DOCNO>");

        return new String(allText.substring(start, end).trim());
    }

    public Document nextDocument() throws IOException {
        String line;

        if (null == waitFor("<DOC>")) {
            return null;
        }
        String identifier = parseDocNumber();
        StringBuffer buffer = new StringBuffer();

        String[] startTags = {"<TEXT>", "<HEADLINE>", "<TITLE>", "<HL>", "<HEAD>",
            "<TTL>", "<DD>", "<DATE>", "<LP>", "<LEADPARA>"
        };
        String[] endTags = {"</TEXT>", "</HEADLINE>", "</TITLE>", "</HL>", "</HEAD>",
            "</TTL>", "</DD>", "</DATE>", "</LP>", "</LEADPARA>"
        };

        int inTag = -1;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("</DOC>")) {
                break;
            }
            if (line.startsWith("<")) {
                if (inTag >= 0 && line.startsWith(endTags[inTag])) {
                    inTag = -1;

                    buffer.append(line);
                    buffer.append('\n');
                } else if (inTag < 0) {
                    for (int i = 0; i < startTags.length; i++) {
                        if (line.startsWith(startTags[i])) {
                            inTag = i;
                            break;
                        }
                    }
                }
            }

            if (inTag >= 0) {
                buffer.append(line);
                buffer.append('\n');
            }
        }

        return new Document(identifier, buffer.toString());
    }
}
