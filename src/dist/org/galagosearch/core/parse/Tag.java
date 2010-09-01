// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.util.Map.Entry;
import java.util.Map;

/**
 * This class represents a tag in a XML/HTML document.
 * 
 * A tag has a tagName, an optional set of attributes, a beginning position and an
 * end position.  The positions are in terms of tokens, so if begin = 5, that means
 * the open tag is between token 5 and token 6.
 * 
 * @author trevor
 */
public class Tag implements Comparable<Tag> {
    /**
     * Constructs a tag.
     * 
     * @param tagName The tagName of the tag.
     * @param attributes Attributes of the tag.
     * @param begin Location of the start tag within the document, in tokens.
     * @param end Location of the end tag within the document, in tokens.
     */
    public Tag(String name, Map<String, String> attributes, int begin, int end) {
        this.name = truncateName(name);
        this.attributes = attributes;
        this.begin = begin;
        this.end = end;
    }

    /**
     * Truncates the tag name to be less than 256 bytes long in UTF-8
     * encoding.  IndexWriter is unable to write tag names that are
     * longer than that.
     * 
     * @param tagName
     * @return
     */

    protected String truncateName(String tagName) {
        // Most tag names have fewer than 32 characters.  If they're in
        // that range, there's no chance that the UTF-8 expansion will be
        // larger than 256 bytes, so we quit early.
        if (tagName.length() > 32) {
            // Here we convert the string to UTF-8 to check the actual
            // byte length.
            while (Utility.makeBytes(tagName).length >= 256) {
                // There's no way a tag with more than 256 chars can be small
                // enough to pass, so we trim those characters right away.
                if (tagName.length() > 256) {
                    tagName = tagName.substring(0, 256);
                } else {
                    // We want to keep as much of the tag as possible, so
                    // we strip one character at a time.
                    tagName = tagName.substring(0, tagName.length()-1);
                }
            }
        }

        return tagName;
    }

    /**
     * Compares two tags together.  Tags are ordered by the location of
     * the open tag.  If we find two tags opening at the same location, the tie
     * is broken by the location of the closing tag.
     * 
     * @param other
     * @return
     */
    public int compareTo(Tag other) {
        int deltaBegin = begin - other.begin;
        if (deltaBegin == 0) {
            return other.end - end;
        }
        return deltaBegin;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("<");
        builder.append(name);

        for (Entry<String, String> entry : attributes.entrySet()) {
            builder.append(' ');
            builder.append(entry.getKey());
            builder.append('=');
            builder.append('"');
            builder.append(entry.getValue());
            builder.append('"');
        }

        builder.append('>');
        return builder.toString();
    }
    public String name;
    public Map<String, String> attributes;
    public int begin;
    public int end;
}
