/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.olap4j.xmla.server.impl;

import java.io.IOException;
import java.util.*;

/**
 * Utility for replacing special characters
 * with escape sequences in strings.
 *
 * <p>To create a StringEscaper, create a builder.
 * Initially it is the identity transform (it leaves every character unchanged).
 * Call {@link Builder#defineEscape} as many
 * times as necessary to set up mappings, and then call {@link Builder#build}
 * to create a StringEscaper.</p>
 *
 * <p>StringEscaper is immutable, but you can call {@link #toBuilder()} to
 * get a builder back.</p>
 *
 * <p>Several escapers are pre-defined:</p>
 *
 * <dl>
 *     <dt>{@link #HTML_ESCAPER}</dt>
 *     <dd>HTML (using &amp;amp;, &amp;&lt;, etc.)</dd>
 *     <dt>{@link #XML_ESCAPER}</dt>
 *     <dd>XML (same as HTML escaper)</dd>
 *     <dt>{@link #XML_NUMERIC_ESCAPER}</dt>
 *     <dd>Uses numeric codes, e.g. &amp;#38; for &amp;.</dd>
 *     <dt>{@link #URL_ARG_ESCAPER}</dt>
 *     <dd>Converts '?' and '&amp;' in URL arguments into URL format</dd>
 *     <dt>{@link #URL_ESCAPER}</dt>
 *     <dd>Converts to URL format</dd>
 * </dl>
 */
public class StringEscaper
{
    private final String [] translationTable;

    public static final StringEscaper HTML_ESCAPER =
        new Builder()
            .defineEscape('&', "&amp;")
            .defineEscape('"', "&quot;")
            .defineEscape('\'', "&#39;") // not "&apos;"
            .defineEscape('<', "&lt;")
            .defineEscape('>', "&gt;")
            .build();

    public static final StringEscaper XML_ESCAPER = HTML_ESCAPER;

    public static final StringEscaper XML_NUMERIC_ESCAPER =
        new Builder()
            .defineEscape('&',"&#38;")
            .defineEscape('"',"&#34;")
            .defineEscape('\'',"&#39;")
            .defineEscape('<',"&#60;")
            .defineEscape('>',"&#62;")
            .defineEscape('\t',"&#9;")
            .defineEscape('\n',"&#10;")
            .defineEscape('\r',"&#13;")
            .build();

    public static final StringEscaper URL_ARG_ESCAPER =
        new Builder()
            .defineEscape('?', "%3f")
            .defineEscape('&', "%26")
            .build();

    public static final StringEscaper URL_ESCAPER =
        URL_ARG_ESCAPER.toBuilder()
            .defineEscape('%', "%%")
            .defineEscape('"', "%22")
            .defineEscape('\r', "+")
            .defineEscape('\n', "+")
            .defineEscape(' ', "+")
            .defineEscape('#', "%23")
            .build();

    /**
     * Creates a StringEscaper. Only called from Builder.
     */
    private StringEscaper(String[] translationTable) {
        this.translationTable = translationTable;
    }

    /**
     * Apply an immutable transformation to the given string.
     */
    public String escapeString(String s)
    {
        StringBuilder sb = null;
        int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            String escape;
            // codes >= 128 (e.g. Euro sign) are always escaped
            if (c > 127) {
                escape = "&#" + Integer.toString(c) + ";";
            } else if (c >= translationTable.length) {
                escape = null;
            } else {
                escape = translationTable[c];
            }
            if (escape == null) {
                if (sb != null) {
                    sb.append(c);
                }
            } else {
                if (sb == null) {
                    sb = new StringBuilder(n * 2);
                    sb.append(s.substring(0, i));
                }
                sb.append(escape);
            }
        }

        if (sb == null) {
            return s;
        } else {
            return sb.toString();
        }
    }

    /**
     * Applies an immutable transformation to the given string, writing the
     * results to a string buffer.
     */
    public void appendEscapedString(String s, StringBuffer sb)
    {
        int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            String escape;
            if (c >= translationTable.length) {
                escape = null;
            } else {
                escape = translationTable[c];
            }
            if (escape == null) {
                sb.append(c);
            } else {
                sb.append(escape);
            }
        }
    }

    /**
     * Applies an immutable transformation to the given string, writing the
     * results to an {@link Appendable} (such as a {@link StringBuilder}).
     */
    public void appendEscapedString(String s, Appendable sb) throws IOException
    {
        int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            String escape;
            if (c >= translationTable.length) {
                escape = null;
            } else {
                escape = translationTable[c];
            }
            if (escape == null) {
                sb.append(c);
            } else {
                sb.append(escape);
            }
        }
    }

    /**
     * Creates a builder from an existing escaper.
     */
    public Builder toBuilder()
    {
        return new Builder(
            new ArrayList<String>(Arrays.asList(translationTable)));
    }

    /**
     * Builder for {@link StringEscaper} instances.
     */
    public static class Builder {
        private final List<String> translationVector;

        public Builder() {
            this(new ArrayList<String>());
        }

        private Builder(List<String> translationVector) {
            this.translationVector = translationVector;
        }

        /**
         * Creates an escaper with the current state of the translation
         * table.
         *
         * @return A string escaper
         */
        public StringEscaper build() {
            return new StringEscaper(
                translationVector.toArray(
                    new String[translationVector.size()]));
        }

        /**
         * Map character "from" to escape sequence "to"
         */
        public Builder defineEscape(char from, String to) {
            int i = (int) from;
            if (i >= translationVector.size()) {
                // Extend list by adding the requisite number of nulls.
                translationVector.addAll(
                    Collections.<String>nCopies(
                        i + 1 - translationVector.size(), null));
            }
            translationVector.set(i, to);
            return this;
        }
    }
}

// End StringEscaper.java
