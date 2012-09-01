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

import java.io.PrintWriter;

/**
 * XML utilities.
 *
 * @author jhyde
 */
public abstract class XmlUtil {
    private XmlUtil() {
        throw new RuntimeException("eek!");
    }

    /**
     * Determine if a String contains any XML special characters, return true
     * if it does.  If this function returns true, the string will need to be
     * encoded either using the stringEncodeXml function above or using a
     * CDATA section.  Note that MSXML has a nasty bug whereby whitespace
     * characters outside of a CDATA section are lost when parsing.  To
     * avoid hitting this bug, this method treats many whitespace characters
     * as "special".
     *
     * @param input the String to scan for XML special characters.
     * @return true if the String contains any such characters.
     */
    public static boolean stringHasXmlSpecials(String input) {
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
            case '<':
            case '>':
            case '"':
            case '\'':
            case '&':
            case '\t':
            case '\n':
            case '\r':
                return true;
            }
        }
        return false;
    }

    /**
     * Encodes a String for XML output, displaying it to a PrintWriter.
     * The String to be encoded is displayed, except that
     * special characters are converted into entities.
     *
     * @param input a String to convert.
     * @param out a PrintWriter to which to write the results.
     */
    public static void stringEncodeXml(String input, PrintWriter out) {
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
            case '<':
            case '>':
            case '"':
            case '\'':
            case '&':
            case '\t':
            case '\n':
            case '\r':
                out.print("&#" + (int)c + ";");
                break;
            default:
                out.print(c);
            }
        }
    }

    /** Prints an XML attribute name and value for string val. */
    public static void printAtt(PrintWriter pw, String name, String val) {
        if (val != null /* && !val.equals("") */) {
            pw.print(" ");
            pw.print(name);
            pw.print("=\"");
            pw.print(escapeForQuoting(val));
            pw.print("\"");
        }
    }

    private static String escapeForQuoting(String val) {
        return StringEscaper.xmlNumericEscaper.escapeString(val);
    }
}

// End XmlUtil.java
