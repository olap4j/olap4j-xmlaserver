/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.xmla;

import org.olap4j.OlapConnection;
import org.olap4j.impl.LcidLocale;
import org.olap4j.metadata.Catalog;
import org.olap4j.xmla.*;

import org.olap4j.xmla.server.impl.Util;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static mondrian.xmla.XmlaConstants.*;

/**
 * Base class for an XML for Analysis schema rowset. A concrete derived class
 * should implement {@link #populateImpl}, calling {@link #addRow} for each row.
 *
 * @author jhyde
 * @see org.olap4j.xmla.RowsetDefinition
 * @since May 2, 2003
 */
abstract class Rowset<E extends Entity> {
    public static final String UUID_PATTERN =
        "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        + "[0-9a-fA-F]{12}";

    protected static final Logger LOGGER = Logger.getLogger(Rowset.class);

    protected final E e;
    protected final RowsetDefinition rowsetDefinition;
    protected final Map<String, Object> restrictions;
    protected final Map<String, String> properties;
    protected final Map<String, String> extraProperties =
        new HashMap<String, String>();
    protected final XmlaRequest request;
    protected final XmlaHandler handler;
    protected final boolean deep;

    /**
     * Creates a Rowset.
     *
     * <p>The exceptions thrown in this constructor are not produced during the
     * execution of an XMLA request and so can be ordinary exceptions and not
     * XmlaException (which are specifically for generating SOAP Fault xml).
     */
    Rowset(
        E entity,
        XmlaRequest request,
        XmlaHandler handler)
    {
        this.e = entity;
        this.rowsetDefinition = entity.def();
        this.restrictions = request.getRestrictions();
        this.properties = request.getProperties();
        this.request = request;
        this.handler = handler;
        List<Column> restrictedColumns = new ArrayList<Column>();
        for (Map.Entry<String, Object> restrictionEntry
            : restrictions.entrySet())
        {
            String restrictedColumn = restrictionEntry.getKey();
            LOGGER.debug(
                "Rowset<init>: restrictedColumn=\"" + restrictedColumn + "\"");
            final Column column =
                rowsetDefinition.lookupColumn(restrictedColumn);
            if (column == null) {
                throw Util.newError(
                    "Rowset '" + rowsetDefinition.name()
                    + "' does not contain column '" + restrictedColumn + "'");
            }
            if (!rowsetDefinition.restrictionColumns.contains(column)) {
                throw Util.newError(
                    "Rowset '" + rowsetDefinition.name()
                    + "' column '" + restrictedColumn
                    + "' does not allow restrictions");
            }
            // Check that the value is of the right type.
            final Object restriction = restrictionEntry.getValue();
            if (restriction instanceof List
                && ((List) restriction).size() > 1)
            {
                final XmlaType type = column.type;
                switch (type) {
                case StringArray:
                case EnumerationArray:
                case StringSometimesArray:
                    break; // OK
                default:
                    throw Util.newError(
                        "Rowset '" + rowsetDefinition.name()
                        + "' column '" + restrictedColumn
                        + "' can only be restricted on one value at a time");
                }
            }
            restrictedColumns.add(column);
        }
        pruneRestrictions(restrictedColumns);
        boolean deep = false;
        for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
            String propertyName = propertyEntry.getKey();
            final XmlaPropertyDefinition propertyDef =
                Util.lookup(XmlaPropertyDefinition.class, propertyName);
            if (propertyDef == null) {
                throw Util.newError(
                    "Rowset '" + rowsetDefinition.name()
                    + "' does not support property '" + propertyName + "'");
            }
            final String propertyValue = propertyEntry.getValue();
            setProperty(propertyDef, propertyValue);
            if (propertyDef == XmlaPropertyDefinition.Deep) {
                deep = Boolean.valueOf(propertyValue);
            }
        }
        this.deep = deep;
    }

    /**
     * Returns a comparator with which to sort rows of this rowset definition.
     * The sort order is defined by the
     * {@link org.olap4j.xmla.RowsetDefinition#sortColumns} field.
     * If the rowset is not sorted (i.e. the list is empty), returns null.
     */
    Comparator<Row> getComparator() {
        if (rowsetDefinition.sortColumns.isEmpty()) {
            return null;
        }
        return new Comparator<Row>() {
            public int compare(Row row1, Row row2) {
                // A faster implementation is welcome.
                for (Column sortColumn : rowsetDefinition.sortColumns) {
                    Comparable val1 = (Comparable) row1.get(sortColumn.name);
                    Comparable val2 = (Comparable) row2.get(sortColumn.name);
                    if ((val1 == null) && (val2 == null)) {
                        // columns can be optional, compare next column
                        //noinspection UnnecessaryContinue
                        continue;
                    } else if (val1 == null) {
                        return -1;
                    } else if (val2 == null) {
                        return 1;
                    } else if (val1 instanceof String
                               && val2 instanceof String)
                    {
                        int v =
                            ((String) val1).compareToIgnoreCase((String) val2);
                        // if equal (= 0), compare next column
                        if (v != 0) {
                            return v;
                        }
                    } else {
                        int v = val1.compareTo(val2);
                        // if equal (= 0), compare next column
                        if (v != 0) {
                            return v;
                        }
                    }
                }
                return 0;
            }
        };
    }

    protected void pruneRestrictions(List<Column> list) {
    }

    /**
     * Sets a property for this rowset. Called by the constructor for each
     * supplied property.<p/>
     *
     * A derived class should override this method and intercept each
     * property it supports. Any property it does not support, it should forward
     * to the base class method, which will probably throw an error.<p/>
     */
    protected void setProperty(
        XmlaPropertyDefinition propertyDef, String value)
    {
        switch (propertyDef) {
        case Format:
            break;
        case DataSourceInfo:
            break;
        case Catalog:
            break;
        case LocaleIdentifier:
            if (value != null) {
                try {
                    // First check for a numeric locale id (LCID) as used by
                    // Windows.
                    final short lcid = Short.valueOf(value);
                    final Locale locale = LcidLocale.lcidToLocale(lcid);
                    if (locale != null) {
                        extraProperties.put(
                            XmlaHandler.JDBC_LOCALE, locale.toString());
                        return;
                    }
                } catch (NumberFormatException nfe) {
                    // Since value is not a valid LCID, now see whether it is a
                    // locale name, e.g. "en_US". This behavior is an
                    // extension to the XMLA spec.
                    try {
                        Locale locale = Util.parseLocale(value);
                        extraProperties.put(
                            XmlaHandler.JDBC_LOCALE, locale.toString());
                        return;
                    } catch (RuntimeException re) {
                        // probably a bad locale string; fall through
                    }
                }
                return;
            }
            // fall through
        default:
            LOGGER.warn(
                "Warning: Rowset '" + rowsetDefinition.name()
                + "' does not support property '" + propertyDef.name()
                + "' (value is '" + value + "')");
        }
    }

    /**
     * Generates an XML schema description to the writer.
     * This is broken into top, Row definition and bottom so that on a
     * case by case basis a RowsetDefinition can redefine the Row
     * definition output. The default assumes a flat set of elements, but
     * for example, SchemaRowsets has a element with child elements.
     *
     * @param writer SAX writer
     * @see XmlaHandler#writeDatasetXmlSchema(SaxWriter, mondrian.xmla.XmlaHandler.SetType)
     */
    void writeRowsetXmlSchema(SaxWriter writer) {
        writeRowsetXmlSchemaTop(writer);
        writeRowsetXmlSchemaRowDef(writer);
        writeRowsetXmlSchemaBottom(writer);
    }

    protected void writeRowsetXmlSchemaTop(SaxWriter writer) {
        writer.startElement(
            "xsd:schema",
            "xmlns:xsd", NS_XSD,
            "xmlns", NS_XMLA_ROWSET,
            "xmlns:xsi", NS_XSI,
            "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
            "targetNamespace", NS_XMLA_ROWSET,
            "elementFormDefault", "qualified");

        writer.startElement(
            "xsd:element",
            "name", "root");
        writer.startElement("xsd:complexType");
        writer.startElement("xsd:sequence");
        writer.element(
            "xsd:element",
            "name", "row",
            "type", "row",
            "minOccurs", 0,
            "maxOccurs", "unbounded");
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType
        writer.endElement(); // xsd:element

        // MS SQL includes this in its schema section even thought
        // its not need for most queries.
        writer.startElement(
            "xsd:simpleType",
            "name", "uuid");
        writer.startElement(
            "xsd:restriction",
            "base", "xsd:string");
        writer.element(
            "xsd:pattern",
            "value", UUID_PATTERN);

        writer.endElement(); // xsd:restriction
        writer.endElement(); // xsd:simpleType
    }

    protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
        writer.startElement(
            "xsd:complexType",
            "name", "row");
        writer.startElement("xsd:sequence");
        for (Column column : rowsetDefinition.columns) {
            final String name =
                XmlaUtil.ElementNameEncoder.INSTANCE.encode(column.name);
            final String xsdType = column.type.columnType;

            Object[] attrs;
            if (column.nullable) {
                if (column.unbounded) {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "minOccurs", 0,
                        "maxOccurs", "unbounded"
                    };
                } else {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "minOccurs", 0
                    };
                }
            } else {
                if (column.unbounded) {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType,
                        "maxOccurs", "unbounded"
                    };
                } else {
                    attrs = new Object[]{
                        "sql:field", column.name,
                        "name", name,
                        "type", xsdType
                    };
                }
            }
            writer.element("xsd:element", attrs);
        }
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType
    }

    protected void writeRowsetXmlSchemaBottom(SaxWriter writer) {
        writer.endElement(); // xsd:schema
    }

    /**
     * Writes the contents of this rowset as a series of SAX events.
     */
    public final void unparse(XmlaResponse response)
        throws XmlaException, SQLException
    {
        final List<Row> rows = new ArrayList<Row>();
        populate(response, null, rows);
        final Comparator<Row> comparator = getComparator();
        if (comparator != null) {
            Collections.sort(rows, comparator);
        }
        final SaxWriter writer = response.getWriter();
        writer.startSequence(null, "row");
        for (Row row : rows) {
            emit(row, response);
        }
        writer.endSequence();
    }

    /**
     * Gathers the set of rows which match a given set of the criteria.
     */
    public final void populate(
        XmlaResponse response,
        OlapConnection connection,
        List<Row> rows)
        throws XmlaException
    {
        boolean ourConnection = false;
        XmlaHandler.Request request1 = null;
        try {
            if (needConnection() && connection == null) {
                connection = handler.getConnection(request, extraProperties);
                ourConnection = true;
            }
            request1 =
                handler.connectionFactory.startRequest(request, connection);
            populateImpl(response, connection, rows);
        } catch (SQLException e) {
            throw new XmlaException(
                UNKNOWN_ERROR_CODE,
                UNKNOWN_ERROR_FAULT_FS,
                "SqlException:",
                e);
        } finally {
            handler.connectionFactory.endRequest(request1);
            if (connection != null && ourConnection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    protected boolean needConnection() {
        return true;
    }

    /**
     * Gathers the set of rows which match a given set of the criteria.
     */
    protected abstract void populateImpl(
        XmlaResponse response,
        OlapConnection connection,
        List<Row> rows)
        throws XmlaException, SQLException;

    /**
     * Adds a {@link Row} to a result, provided that it meets the necessary
     * criteria. Returns whether the row was added.
     *
     * @param row Row
     * @param rows List of result rows
     */
    protected final boolean addRow(
        Row row,
        List<Row> rows)
        throws XmlaException
    {
        return rows.add(row);
    }

    /**
     * Emits a row for this rowset, reading fields from a
     * {@link mondrian.xmla.Rowset.Row} object.
     *
     * @param row Row
     * @param response XMLA response writer
     */
    protected void emit(Row row, XmlaResponse response)
        throws XmlaException, SQLException
    {
        SaxWriter writer = response.getWriter();

        writer.startElement("row");
        for (Column column : rowsetDefinition.columns) {
            Object value = row.get(column.name);
            if (value == null) {
                if (!column.nullable) {
                    throw new XmlaException(
                        CLIENT_FAULT_FC,
                        HSB_BAD_NON_NULLABLE_COLUMN_CODE,
                        HSB_BAD_NON_NULLABLE_COLUMN_FAULT_FS,
                        Util.newInternal(
                            "Value required for column "
                            + column.name
                            + " of rowset "
                            + rowsetDefinition.name()));
                }
            } else if (value instanceof XmlElement[]) {
                XmlElement[] elements = (XmlElement[]) value;
                for (XmlElement element : elements) {
                    emitXmlElement(writer, element);
                }
            } else if (value instanceof Object[]) {
                Object[] values = (Object[]) value;
                for (Object value1 : values) {
                    writer.startElement(column.name);
                    writer.characters(value1.toString());
                    writer.endElement();
                }
            } else if (value instanceof List) {
                List values = (List) value;
                for (Object value1 : values) {
                    if (value1 instanceof XmlElement) {
                        XmlElement xmlElement = (XmlElement) value1;
                        emitXmlElement(writer, xmlElement);
                    } else {
                        writer.startElement(column.name);
                        writer.characters(value1.toString());
                        writer.endElement();
                    }
                }
            } else if (value instanceof Rowset) {
                Rowset rowset = (Rowset) value;
                final List<Row> rows = new ArrayList<Row>();
                rowset.populate(response, null, rows);
                writer.startSequence(column.name, "row");
                for (Row row1 : rows) {
                    rowset.emit(row1, response);
                }
                writer.endSequence();
            } else {
                writer.textElement(column.name, value);
            }
        }
        writer.endElement();
    }

    private void emitXmlElement(SaxWriter writer, XmlElement element) {
        if (element.attributes == null) {
            writer.startElement(element.tag);
        } else {
            writer.startElement(element.tag, element.attributes);
        }

        if (element.text == null) {
            for (XmlElement aChildren : element.children) {
                emitXmlElement(writer, aChildren);
            }
        } else {
            writer.characters(element.text);
        }

        writer.endElement();
    }

    /**
     * Populates all of the values in an enumeration into a list of rows.
     */
    protected <E> void populate(
        Class<E> clazz, List<Row> rows,
        final Comparator<E> comparator)
        throws XmlaException
    {
        final E[] enumsSortedByName = clazz.getEnumConstants().clone();
        Arrays.sort(enumsSortedByName, comparator);
        for (E anEnum : enumsSortedByName) {
            Row row = new Row();
            for (Column column : rowsetDefinition.columns) {
                row.names.add(column.name);
                row.values.add(column.get(anEnum));
            }
            rows.add(row);
        }
    }

    /**
     * Creates a condition functor based on the restrictions on a given metadata
     * column specified in an XMLA request.
     *
     * <p>A condition is a {@link org.olap4j.xmla.server.impl.Util.Function1} whose return
     * type is boolean.
     *
     * Restrictions are used in each Rowset's discovery request. If there is no
     * restriction then the passes method always returns true.
     *
     * <p>It is known at the beginning of a
     * {@link Rowset#populate(XmlaResponse, org.olap4j.OlapConnection, java.util.List)}
     * method whether the restriction is not specified (null), a single value
     * (String) or an array of values (String[]). So, creating the conditions
     * just once at the beginning is faster than having to determine the
     * restriction status each time it is needed.
     *
     * @param column Metadata column
     * @param <E> Element type, e.g. {@link org.olap4j.metadata.Catalog} or
     *     {@link org.olap4j.metadata.Level}
     * @return Condition functor
     */
    <E> Util.Predicate1<E> makeCondition(Column column) {
        return makeCondition(
            Util.<E>identityFunction(),
            column);
    }

    /**
     * Creates a condition functor using an accessor.
     *
     * <p>The accessor gets a particular property of the element in question
     * for the column restrictions to act upon.
     *
     * @param getter Attribute accessor
     * @param column Metadata column
     * @param <E> Element type, e.g. {@link org.olap4j.metadata.Catalog} or
     *     {@link org.olap4j.metadata.Level}
     * @param <V> Value type; often {@link String}, since many restrictions
     *     work on the name or unique name of elements
     * @return Condition functor
     */
    <E, V> Util.Predicate1<E> makeCondition(
        final Util.Function1<? super E, V> getter,
        Column column)
    {
        final Object restriction = restrictions.get(column.name);

        if (restriction == null) {
            return Util.truePredicate1();
        } else if (restriction instanceof XmlaUtil.Wildcard) {
            XmlaUtil.Wildcard wildcard = (XmlaUtil.Wildcard) restriction;
            String regexp =
                Util.wildcardToRegexp(
                    Collections.singletonList(wildcard.pattern));
            final Matcher matcher = Pattern.compile(regexp).matcher("");
            return new Util.Predicate1<E>() {
                public boolean test(E element) {
                    V value = getter.apply(element);
                    return matcher.reset(String.valueOf(value)).matches();
                }
            };
        } else if (restriction instanceof List) {
            final List<V> requiredValues = (List) restriction;
            return new Util.Predicate1<E>() {
                public boolean test(E element) {
                    if (element == null) {
                        return requiredValues.contains("");
                    }
                    V value = getter.apply(element);
                    return requiredValues.contains(value);
                }
            };
        } else {
            throw Util.newInternal(
                "unexpected restriction type: " + restriction.getClass());
        }
    }

    /**
     * Returns the restriction if it is a String, or null otherwise. Does not
     * attempt two determine if the restriction is an array of Strings
     * if all members of the array have the same value (in which case
     * one could return, again, simply a single String).
     */
    String getRestrictionValueAsString(Column column) {
        final Object restriction = restrictions.get(column.name);
        if (restriction instanceof List) {
            List<String> rval = (List<String>) restriction;
            if (rval.size() == 1) {
                return rval.get(0);
            }
        }
        return null;
    }

    /**
     * Returns a column's restriction as an <code>int</code> if it
     * exists, -1 otherwise.
     */
    int getRestrictionValueAsInt(Column column) {
        final Object restriction = restrictions.get(column.name);
        if (restriction instanceof List) {
            List<String> rval = (List<String>) restriction;
            if (rval.size() == 1) {
                try {
                    return Integer.parseInt(rval.get(0));
                } catch (NumberFormatException ex) {
                    LOGGER.info(
                        "Rowset.getRestrictionValue: "
                        + "bad integer restriction \"" + rval + "\"");
                    return -1;
                }
            }
        }
        return -1;
    }

    /**
     * Returns true if there is a restriction for the given column
     * definition.
     */
    protected boolean isRestricted(Column column) {
        return (restrictions.get(column.name) != null);
    }

    protected Util.Predicate1<Catalog> catNameCond() {
        Map<String, String> properties = request.getProperties();
        final String catalogName =
            properties.get(XmlaPropertyDefinition.Catalog.name());
        if (catalogName != null) {
            return new Util.Predicate1<Catalog>() {
                public boolean test(Catalog catalog) {
                    return catalog.getName().equals(catalogName);
                }
            };
        } else {
            return Util.truePredicate1();
        }
    }

    /**
     * A set of name/value pairs, which can be output using
     * {@link Rowset#addRow}. This uses less memory than simply
     * using a HashMap and for very big data sets memory is
     * a concern.
     */
    protected static class Row {
        private final ArrayList<String> names;
        private final ArrayList<Object> values;
        Row() {
            this.names = new ArrayList<String>();
            this.values = new ArrayList<Object>();
        }

        void set(String name, Object value) {
            this.names.add(name);
            this.values.add(value);
        }

        /**
         * Retrieves the value of a field with a given name, or null if the
         * field's value is not defined.
         */
        public Object get(String name) {
            int i = this.names.indexOf(name);
            return (i < 0) ? null : this.values.get(i);
        }
    }

    /**
     * Holder for non-scalar column values of a
     * {@link mondrian.xmla.Rowset.Row}.
     */
    protected static class XmlElement {
        private final String tag;
        private final Object[] attributes;
        private final String text;
        private final XmlElement[] children;

        XmlElement(String tag, Object[] attributes, String text) {
            this(tag, attributes, text, null);
        }

        XmlElement(String tag, Object[] attributes, XmlElement[] children) {
            this(tag, attributes, null, children);
        }

        private XmlElement(
            String tag,
            Object[] attributes,
            String text,
            XmlElement[] children)
        {
            assert attributes == null || attributes.length % 2 == 0;
            this.tag = tag;
            this.attributes = attributes;
            this.text = text;
            this.children = children;
        }
    }
}

// End Rowset.java
