/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla.impl;

import mondrian.xmla.SaxWriter;

import org.olap4j.xmla.server.impl.*;

import org.xml.sax.Attributes;

import java.io.*;
import java.util.regex.Pattern;

/**
 * Default implementation of {@link SaxWriter}.
 *
 * @author jhyde
 * @author Gang Chen
 * @since 27 April, 2003
 */
public class DefaultSaxWriter implements SaxWriter {
    /** Inside the tag of an element. */
    private static final int STATE_IN_TAG = 0;
    /** After the tag at the end of an element. */
    private static final int STATE_END_ELEMENT = 1;
    /** After the tag at the start of an element. */
    private static final int STATE_AFTER_TAG = 2;
    /** After a burst of character data. */
    private static final int STATE_CHARACTERS = 3;

    private final Appendable buf;
    private int indent;
    private final String indentStr = "  ";
    private final ArrayStack<String> stack = new ArrayStack<String>();
    private int state = STATE_END_ELEMENT;

    private final static Pattern nlPattern = Pattern.compile("\\r\\n|\\r|\\n");

    /**
     * Creates a DefaultSaxWriter writing to an {@link java.io.OutputStream}.
     */
    public DefaultSaxWriter(OutputStream stream) {
        this(new BufferedWriter(new OutputStreamWriter(stream)));
    }

    public DefaultSaxWriter(OutputStream stream, String xmlEncoding)
        throws UnsupportedEncodingException
    {
        this(new BufferedWriter(new OutputStreamWriter(stream, xmlEncoding)));
    }

    /**
     * Creates a DefaultSaxWriter without indentation.
     *
     * @param buf String builder to write to
     */
    public DefaultSaxWriter(Appendable buf) {
        this(buf, 0);
    }

    /**
     * Creates a DefaultSaxWriter.
     *
     * @param buf String builder to write to
     * @param initialIndent Initial indent (0 to write on single line)
     */
    public DefaultSaxWriter(Appendable buf, int initialIndent) {
        this.buf = buf;
        this.indent = initialIndent;
    }

    private void _startElement(
        String namespaceURI,
        String localName,
        String qName,
        Attributes atts) throws IOException
    {
        _checkTag();
        if (indent > 0) {
            buf.append(Util.nl);
        }
        for (int i = 0; i < indent; i++) {
            buf.append(indentStr);
        }
        indent++;
        buf.append('<');
        buf.append(qName);
        final int length = atts.getLength();
        for (int i = 0; i < length; i++) {
            String val = atts.getValue(i);
            if (val != null) {
                buf.append(' ');
                buf.append(atts.getQName(i));
                buf.append("=\"");
                StringEscaper.XML_NUMERIC_ESCAPER.appendEscapedString(val, buf);
                buf.append("\"");
            }
        }
        state = STATE_IN_TAG;
        assert qName != null;
        stack.add(qName);
    }

    private void _checkTag() throws IOException {
        if (state == STATE_IN_TAG) {
            state = STATE_AFTER_TAG;
            buf.append('>');
        }
    }

    private void _endElement() throws IOException
    {
        String qName = stack.pop();
        indent--;
        if (state == STATE_IN_TAG) {
            buf.append("/>");
        } else {
            if (state != STATE_CHARACTERS) {
                buf.append(Util.nl);
                for (int i = 0; i < indent; i++) {
                    buf.append(indentStr);
                }
            }
            buf.append("</");
            buf.append(qName);
            buf.append('>');
        }
        state = STATE_END_ELEMENT;
    }

    private void _characters(String s) throws IOException
    {
        _checkTag();
        StringEscaper.XML_NUMERIC_ESCAPER.appendEscapedString(s, buf);
        state = STATE_CHARACTERS;
    }

    //
    // Simplifying methods

    public void characters(String s) {
        try {
            _characters(s);
        } catch (IOException e) {
            throw new RuntimeException("Error while appending XML", e);
        }
    }

    public void startSequence(String name, String subName) {
        if (name != null) {
            startElement(name);
        } else {
            stack.push(null);
        }
    }

    public void endSequence() {
        if (stack.peek() == null) {
            stack.pop();
        } else {
            endElement();
        }
    }

    public final void textElement(String name, Object data) {
        try {
            _startElement(null, null, name, EmptyAttributes);

            // Replace line endings with spaces. IBM's DOM implementation keeps
            // line endings, whereas Sun's does not. For consistency, always
            // strip them.
            //
            // REVIEW: It would be better to enclose in CDATA, but some clients
            // might not be expecting this.
            characters(
                nlPattern.matcher(data.toString()).replaceAll(" "));
            _endElement();
        } catch (IOException e) {
            throw new RuntimeException("Error while appending XML", e);
        }
    }

    public void element(String tagName, Object... attributes) {
        startElement(tagName, attributes);
        endElement();
    }

    public void startElement(String tagName) {
        try {
            _startElement(null, null, tagName, EmptyAttributes);
        } catch (IOException e) {
            throw new RuntimeException("Error while appending XML", e);
        }
    }

    public void startElement(String tagName, Object... attributes) {
        try {
            _startElement(
                null, null, tagName, new StringAttributes(attributes));
        } catch (IOException e) {
            throw new RuntimeException("Error while appending XML", e);
        }
    }

    public void endElement() {
        try {
            _endElement();
        } catch (IOException e) {
            throw new RuntimeException("Error while appending XML", e);
        }
    }

    public void startDocument() {
        if (stack.size() != 0) {
            throw new IllegalStateException("Document already started");
        }
    }

    public void endDocument() {
        if (stack.size() != 0) {
            throw new IllegalStateException(
                "Document may have unbalanced elements");
        }
        flush();
    }

    public void completeBeforeElement(String tagName) {
        if (stack.indexOf(tagName) == -1) {
            return;
        }

        String currentTagName  = stack.peek();
        while (!tagName.equals(currentTagName)) {
            try {
                _endElement();
            } catch (IOException e) {
                throw new RuntimeException("Error while appending XML", e);
            }
            currentTagName = stack.peek();
        }
    }

    public void verbatim(String text) {
        try {
            _checkTag();
            buf.append(text);
        } catch (IOException e) {
            throw new RuntimeException("Error while appending XML", e);
        }
    }

    public void flush() {
        if (buf instanceof Writer) {
            try {
                ((Writer) buf).flush();
            } catch (IOException e) {
                throw new RuntimeException("Error while flushing XML", e);
            }
        }
    }

    private static final Attributes EmptyAttributes = new Attributes() {
        public int getLength() {
            return 0;
        }

        public String getURI(int index) {
            return null;
        }

        public String getLocalName(int index) {
            return null;
        }

        public String getQName(int index) {
            return null;
        }

        public String getType(int index) {
            return null;
        }

        public String getValue(int index) {
            return null;
        }

        public int getIndex(String uri, String localName) {
            return 0;
        }

        public int getIndex(String qName) {
            return 0;
        }

        public String getType(String uri, String localName) {
            return null;
        }

        public String getType(String qName) {
            return null;
        }

        public String getValue(String uri, String localName) {
            return null;
        }

        public String getValue(String qName) {
            return null;
        }
    };

    /**
     * List of SAX attributes based upon a string array.
     */
    public static class StringAttributes implements Attributes {
        private final Object[] strings;

        public StringAttributes(Object[] strings) {
            this.strings = strings;
        }

        public int getLength() {
            return strings.length / 2;
        }

        public String getURI(int index) {
            return null;
        }

        public String getLocalName(int index) {
            return null;
        }

        public String getQName(int index) {
            return (String) strings[index * 2];
        }

        public String getType(int index) {
            return null;
        }

        public String getValue(int index) {
            return stringValue(strings[index * 2 + 1]);
        }

        public int getIndex(String uri, String localName) {
            return -1;
        }

        public int getIndex(String qName) {
            final int count = strings.length / 2;
            for (int i = 0; i < count; i++) {
                String string = (String) strings[i * 2];
                if (string.equals(qName)) {
                    return i;
                }
            }
            return -1;
        }

        public String getType(String uri, String localName) {
            return null;
        }

        public String getType(String qName) {
            return null;
        }

        public String getValue(String uri, String localName) {
            return null;
        }

        public String getValue(String qName) {
            final int index = getIndex(qName);
            if (index < 0) {
                return null;
            } else {
                return stringValue(strings[index * 2 + 1]);
            }
        }

        private static String stringValue(Object s) {
            return s == null ? null : s.toString();
        }
    }
}

// End DefaultSaxWriter.java
