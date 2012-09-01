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

import java.util.*;

/**
 * Utility for replacing special characters
 * with escape sequences in strings.
 *
 * <p>Initially, a StringEscaper starts out as
 * an identity transform in the "mutable" state.  Call defineEscape as many
 * times as necessary to set up mappings, and then call makeImmutable() before
 * using appendEscapedString to actually apply the defined transform.  Or, use
 * one of the global mappings pre-defined here.</p>
 */
public class StringEscaper implements Cloneable
{
    private ArrayList translationVector;
    private String [] translationTable;

    public static StringEscaper xmlEscaper;
    public static StringEscaper xmlNumericEscaper;
    public static StringEscaper htmlEscaper;
    public static StringEscaper urlArgEscaper;
    public static StringEscaper urlEscaper;

    /**
     * Identity transform
     */
    public StringEscaper()
    {
        translationVector = new ArrayList();
    }

    /**
     * Map character "from" to escape sequence "to"
     */
    public void defineEscape(char from,String to)
    {
        int i = (int) from;
        if (i >= translationVector.size()) {
            // Extend list by adding the requisite number of nulls.
            final int count = i + 1 - translationVector.size();
            translationVector.addAll(
                new AbstractList() {
                    public Object get(int index) {
                        return null;
                    }

                    public int size() {
                        return count;
                    }
                });
        }
        translationVector.set(i, to);
    }

    /**
     * Call this before attempting to escape strings; after this,
     * defineEscape may not be called again.
     */
    public void makeImmutable()
    {
        translationTable =
            (String[]) translationVector.toArray(
                new String[translationVector.size()]);
        translationVector = null;
    }

    /**
     * Apply an immutable transformation to the given string.
     */
    public String escapeString(String s)
    {
        StringBuffer sb = null;
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
                    sb = new StringBuffer(n * 2);
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
     * Apply an immutable transformation to the given string, writing the
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

    protected Object clone()
    {
        StringEscaper clone = new StringEscaper();
        if (translationVector != null) {
            clone.translationVector = new ArrayList(translationVector);
        }
        if (translationTable != null) {
            clone.translationTable = (String[]) translationTable.clone();
        }
        return clone;
    }

    /**
     * Create a mutable escaper from an existing escaper, which may
     * already be immutable.
     */
    public StringEscaper getMutableClone()
    {
        StringEscaper clone = (StringEscaper) clone();
        if (clone.translationVector == null) {
            clone.translationVector =
                new ArrayList(Arrays.asList(clone.translationTable));
            clone.translationTable = null;
        }
        return clone;
    }

    static
    {
        htmlEscaper = new StringEscaper();
        htmlEscaper.defineEscape('&', "&amp;");
        htmlEscaper.defineEscape('"', "&quot;");
//      htmlEscaper.defineEscape('\'',"&apos;");
        htmlEscaper.defineEscape('\'', "&#39;");
        htmlEscaper.defineEscape('<', "&lt;");
        htmlEscaper.defineEscape('>', "&gt;");

        xmlNumericEscaper = new StringEscaper();
        xmlNumericEscaper.defineEscape('&',"&#38;");
        xmlNumericEscaper.defineEscape('"',"&#34;");
        xmlNumericEscaper.defineEscape('\'',"&#39;");
        xmlNumericEscaper.defineEscape('<',"&#60;");
        xmlNumericEscaper.defineEscape('>',"&#62;");

        urlArgEscaper = new StringEscaper();
        urlArgEscaper.defineEscape('?', "%3f");
        urlArgEscaper.defineEscape('&', "%26");
        urlEscaper = urlArgEscaper.getMutableClone();
        urlEscaper.defineEscape('%', "%%");
        urlEscaper.defineEscape('"', "%22");
        urlEscaper.defineEscape('\r', "+");
        urlEscaper.defineEscape('\n', "+");
        urlEscaper.defineEscape(' ', "+");
        urlEscaper.defineEscape('#', "%23");

        htmlEscaper.makeImmutable();
        xmlEscaper = htmlEscaper;
        xmlNumericEscaper.makeImmutable();
        urlArgEscaper.makeImmutable();
        urlEscaper.makeImmutable();
    }

}

// End StringEscaper.java
