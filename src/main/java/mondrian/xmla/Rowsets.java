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

import org.olap4j.xmla.server.impl.Composite;
import org.olap4j.xmla.server.impl.Util;

import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Member.TreeOp;
import org.olap4j.metadata.XmlaConstants;
import org.olap4j.xmla.*;
import org.olap4j.xmla.Enumeration;

import java.sql.SQLException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.olap4j.xmla.server.impl.Util.filter;

/**
 * Various implementations of {@link Rowset}.
 */
public class Rowsets {
    static Rowset create(
        RowsetDefinition def,
        XmlaRequest request,
        XmlaHandler handler)
    {
        switch (def) {
        case DISCOVER_DATASOURCES:
            return new DiscoverDatasourcesRowset(request, handler);
        case DISCOVER_ENUMERATORS:
            return new DiscoverEnumeratorsRowset(request, handler);
        case DISCOVER_KEYWORDS:
            return new DiscoverKeywordsRowset(request, handler);
        case DISCOVER_LITERALS:
            return new DiscoverLiteralsRowset(request, handler);
        case DISCOVER_PROPERTIES:
            return new DiscoverPropertiesRowset(request, handler);
        case DISCOVER_SCHEMA_ROWSETS:
            return new DiscoverSchemaRowsetsRowset(request, handler);
        case DBSCHEMA_CATALOGS:
            return new DbschemaCatalogsRowset(request, handler);
        case DBSCHEMA_COLUMNS:
            return new DbschemaColumnsRowset(request, handler);
        case DBSCHEMA_PROVIDER_TYPES:
            return new DbschemaProviderTypesRowset(request, handler);
        case DBSCHEMA_SCHEMATA:
            return new DbschemaSchemataRowset(request, handler);
        case DBSCHEMA_TABLES:
            return new DbschemaTablesRowset(request, handler);
        case DBSCHEMA_TABLES_INFO:
            return new DbschemaTablesInfoRowset(request, handler);
        case MDSCHEMA_ACTIONS:
            return new MdschemaActionsRowset(request, handler);
        case MDSCHEMA_CUBES:
            return new MdschemaCubesRowset(request, handler);
        case MDSCHEMA_DIMENSIONS:
            return new MdschemaDimensionsRowset(request, handler);
        case MDSCHEMA_FUNCTIONS:
            return new MdschemaFunctionsRowset(request, handler);
        case MDSCHEMA_HIERARCHIES:
            return new MdschemaHierarchiesRowset(request, handler);
        case MDSCHEMA_LEVELS:
            return new MdschemaLevelsRowset(request, handler);
        case MDSCHEMA_MEASURES:
            return new MdschemaMeasuresRowset(request, handler);
        case MDSCHEMA_MEMBERS:
            return new MdschemaMembersRowset(request, handler);
        case MDSCHEMA_PROPERTIES:
            return new MdschemaPropertiesRowset(request, handler);
        case MDSCHEMA_SETS:
            return new MdschemaSetsRowset(request, handler);
        default:
            throw new AssertionError(def);
        }
    }

    /**
     * Date the schema was last modified.
     *
     * <p>TODO: currently schema grammar does not support modify date
     * so we return just some date for now.
     */
    private static final String DATE_MODIFIED = "2005-01-25T17:35:32";

    private static XmlaConstants.DBType getDBTypeFromProperty(Property prop) {
        switch (prop.getDatatype()) {
        case STRING:
            return XmlaConstants.DBType.WSTR;
        case INTEGER:
        case UNSIGNED_INTEGER:
        case DOUBLE:
            return XmlaConstants.DBType.R8;
        case BOOLEAN:
            return XmlaConstants.DBType.BOOL;
        default:
            // TODO: what type is it really, its not a string
            return XmlaConstants.DBType.WSTR;
        }
    }

    static int getDimensionType(Dimension dim) throws OlapException {
        switch (dim.getDimensionType()) {
        case MEASURE:
            return MdschemaDimensionsRowset.MD_DIMTYPE_MEASURE;
        case TIME:
            return MdschemaDimensionsRowset.MD_DIMTYPE_TIME;
        default:
            return MdschemaDimensionsRowset.MD_DIMTYPE_OTHER;
        }
    }

    public static final Util.Function1<Catalog, String> CATALOG_NAME_GETTER =
        new Util.Function1<Catalog, String>() {
            public String apply(Catalog catalog) {
                return catalog.getName();
            }
        };

    public static final Util.Function1<Schema, String> SCHEMA_NAME_GETTER =
        new Util.Function1<Schema, String>() {
            public String apply(Schema schema) {
                return schema.getName();
            }
        };

    public static final Util.Function1<MetadataElement, String>
        ELEMENT_NAME_GETTER =
        new Util.Function1<MetadataElement, String>() {
            public String apply(MetadataElement element) {
                return element.getName();
            }
        };

    public static final Util.Function1<MetadataElement, String>
        ELEMENT_UNAME_GETTER =
        new Util.Function1<MetadataElement, String>() {
            public String apply(MetadataElement element) {
                return element.getUniqueName();
            }
        };

    public static final Util.Function1<Member, Member.Type>
        MEMBER_TYPE_GETTER =
        new Util.Function1<Member, Member.Type>() {
            public Member.Type apply(Member member) {
                return member.getMemberType();
            }
        };

    public static final Util.Function1<XmlaPropertyDefinition, String>
        PROPDEF_NAME_GETTER =
        new Util.Function1<XmlaPropertyDefinition, String>() {
            public String apply(XmlaPropertyDefinition property) {
                return property.name();
            }
        };

    static void serialize(StringBuilder buf, Collection<String> strings) {
        int k = 0;
        for (String name : Util.sort(strings)) {
            if (k++ > 0) {
                buf.append(',');
            }
            buf.append(name);
        }
    }

    private static Level lookupLevel(Cube cube, String levelUniqueName) {
        for (Dimension dimension : cube.getDimensions()) {
            for (Hierarchy hierarchy : dimension.getHierarchies()) {
                for (Level level : hierarchy.getLevels()) {
                    if (level.getUniqueName().equals(levelUniqueName)) {
                        return level;
                    }
                }
            }
        }
        return null;
    }

    static Iterable<Cube> sortedCubes(Schema schema) throws OlapException {
        return Util.sort(
            schema.getCubes(),
            new Comparator<Cube>() {
                public int compare(Cube o1, Cube o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            }
        );
    }

    static Iterable<Cube> filteredCubes(
        final Schema schema,
        Util.Predicate1<Cube> cubeNameCond)
        throws OlapException
    {
        final Iterable<Cube> iterable =
            filter(sortedCubes(schema), cubeNameCond);
        if (!cubeNameCond.test(new SharedDimensionHolderCube(schema))) {
            return iterable;
        }
        return Composite.of(
            Collections.singletonList(
                new SharedDimensionHolderCube(schema)), iterable);
    }

    private static XmlaRequest wrapRequest(
        XmlaRequest request, Map<Column, String> map)
    {
        final Map<String, Object> restrictionsMap =
            new HashMap<String, Object>(request.getRestrictions());
        for (Map.Entry<Column, String> entry : map.entrySet()) {
            restrictionsMap.put(
                entry.getKey().name,
                Collections.singletonList(entry.getValue()));
        }

        return new DelegatingXmlaRequest(request) {
            @Override
            public Map<String, Object> getRestrictions() {
                return restrictionsMap;
            }
        };
    }

    /**
     * Returns an iterator over the catalogs in a connection, setting the
     * connection's catalog to each successful catalog in turn.
     *
     * @param connection Connection
     * @param conds Zero or more conditions to be applied to catalogs
     * @return Iterator over catalogs
     */
    private static Iterable<Catalog> catIter(
        final OlapConnection connection,
        final Util.Predicate1<Catalog>... conds)
    {
        return new Iterable<Catalog>() {
            public Iterator<Catalog> iterator() {
                try {
                    return new Iterator<Catalog>() {
                        final Iterator<Catalog> catalogIter =
                            Util.filter(
                                connection.getOlapCatalogs(),
                                conds).iterator();

                        public boolean hasNext() {
                            return catalogIter.hasNext();
                        }

                        public Catalog next() {
                            Catalog catalog = catalogIter.next();
                            try {
                                connection.setCatalog(catalog.getName());
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            return catalog;
                        }

                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                } catch (OlapException e) {
                    throw new RuntimeException(
                        "Failed to obtain a list of catalogs form the connection object.",
                        e);
                }
            }
        };
    }

    private static class DelegatingXmlaRequest implements XmlaRequest {
        protected final XmlaRequest request;

        public DelegatingXmlaRequest(XmlaRequest request) {
            this.request = request;
        }

        public XmlaConstants.Method getMethod() {
            return request.getMethod();
        }

        public Map<String, String> getProperties() {
            return request.getProperties();
        }

        public Map<String, Object> getRestrictions() {
            return request.getRestrictions();
        }

        public String getStatement() {
            return request.getStatement();
        }

        public String getRoleName() {
            return request.getRoleName();
        }

        public String getRequestType() {
            return request.getRequestType();
        }

        public boolean isDrillThrough() {
            return request.isDrillThrough();
        }

        public String getUsername() {
            return request.getUsername();
        }

        public String getPassword() {
            return request.getPassword();
        }

        public String getSessionId() {
            return request.getSessionId();
        }
    }

    /**
     * Dummy implementation of {@link Cube} that holds all shared dimensions
     * in a given schema. Less error-prone than requiring all generator code
     * to cope with a null Cube.
     */
    private static class SharedDimensionHolderCube implements Cube {
        private final Schema schema;

        public SharedDimensionHolderCube(Schema schema) {
            this.schema = schema;
        }

        public Schema getSchema() {
            return schema;
        }

        public NamedList<Dimension> getDimensions() {
            try {
                return schema.getSharedDimensions();
            } catch (OlapException e) {
                throw new RuntimeException(e);
            }
        }

        public NamedList<Hierarchy> getHierarchies() {
            final NamedList<Hierarchy> hierarchyList =
                new ArrayNamedListImpl<Hierarchy>() {
                    public String getName(Object o) {
                        return ((Hierarchy) o).getName();
                    }
                };
            for (Dimension dimension : getDimensions()) {
                hierarchyList.addAll(dimension.getHierarchies());
            }
            return hierarchyList;
        }

        public List<Measure> getMeasures() {
            return Collections.emptyList();
        }

        public NamedList<NamedSet> getSets() {
            throw new UnsupportedOperationException();
        }

        public Collection<Locale> getSupportedLocales() {
            throw new UnsupportedOperationException();
        }

        public Member lookupMember(List<IdentifierSegment> identifierSegments)
            throws org.olap4j.OlapException
        {
            throw new UnsupportedOperationException();
        }

        public List<Member> lookupMembers(
            Set<Member.TreeOp> treeOps,
            List<IdentifierSegment> identifierSegments)
            throws org.olap4j.OlapException
        {
            throw new UnsupportedOperationException();
        }

        public boolean isDrillThroughEnabled() {
            return false;
        }

        public String getName() {
            return "";
        }

        public String getUniqueName() {
            return "";
        }

        public String getCaption() {
            return "";
        }

        public String getDescription() {
            return "";
        }

        public boolean isVisible() {
            return false;
        }
    }

    static class DiscoverDatasourcesRowset extends Rowset<XmlaDatasource> {
        public DiscoverDatasourcesRowset(
            XmlaRequest request, XmlaHandler handler)
        {
            super(XmlaDatasource.INSTANCE, request, handler);
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException, SQLException
        {
            if (needConnection()) {
                final XmlaHandler.XmlaExtra extra =
                    handler.connectionFactory.getExtra();
                for (Map<String, Object> ds : extra.getDataSources(connection))
                {
                    Row row = new Row();
                    for (Column column : rowsetDefinition.columns) {
                        row.set(column.name, ds.get(column.name));
                    }
                    addRow(row, rows);
                }
            } else {
                // using pre-configured discover datasources response
                Row row = new Row();
                Map<String, Object> map =
                    this.handler.connectionFactory
                        .getPreConfiguredDiscoverDatasourcesResponse();
                for (Column column : rowsetDefinition.columns) {
                    row.set(column.name, map.get(column.name));
                }
                addRow(row, rows);
            }
        }

        @Override
        protected boolean needConnection() {
            // If the olap connection factory has a pre configured response,
            // we don't need to connect to find metadata. This is good.
            return this.handler.connectionFactory
                       .getPreConfiguredDiscoverDatasourcesResponse() == null;
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverSchemaRowsetsRowset
        extends Rowset<XmlaSchemaRowset>
    {
        public DiscoverSchemaRowsetsRowset(
            XmlaRequest request, XmlaHandler handler)
        {
            super(XmlaSchemaRowset.INSTANCE, request, handler);
        }

        @Override protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
            writer.startElement(
                "xsd:complexType",
                "name", "row");
            writer.startElement("xsd:sequence");
            for (Column column : rowsetDefinition.columns) {
                final String name =
                    XmlaUtil.ElementNameEncoder.INSTANCE.encode(column.name);

                if (column == e.Restrictions) {
                    writer.startElement(
                        "xsd:element",
                        "sql:field", column.name,
                        "name", name,
                        "minOccurs", 0,
                        "maxOccurs", "unbounded");
                    writer.startElement("xsd:complexType");
                    writer.startElement("xsd:sequence");
                    writer.element(
                        "xsd:element",
                        "name", "Name",
                        "type", "xsd:string",
                        "sql:field", "Name");
                    writer.element(
                        "xsd:element",
                        "name", "Type",
                        "type", "xsd:string",
                        "sql:field", "Type");

                    writer.endElement(); // xsd:sequence
                    writer.endElement(); // xsd:complexType
                    writer.endElement(); // xsd:element

                } else {
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
            }
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException
        {
            List<RowsetDefinition> rowsetDefinitions =
                new ArrayList<RowsetDefinition>(
                    Arrays.asList(RowsetDefinition.values()));
            Collections.sort(
                rowsetDefinitions,
                new Comparator<RowsetDefinition>() {
                    public int compare(
                        RowsetDefinition o1,
                        RowsetDefinition o2)
                    {
                        return o1.name().compareTo(o2.name());
                    }
                });
            for (RowsetDefinition def2 : rowsetDefinitions) {
                Row row = new Row();
                row.set(e.SchemaName.name, def2.name());

                // TODO: If we have a SchemaGuid output here
                //row.set(SchemaGuid.name, "");

                row.set(e.Restrictions.name, getRestrictions(def2));

                String desc = def2.getDescription();
                row.set(e.Description.name, (desc == null) ? "" : desc);
                addRow(row, rows);
            }
        }

        private List<XmlElement> getRestrictions(
            RowsetDefinition rowsetDefinition)
        {
            List<XmlElement> restrictionList = new ArrayList<XmlElement>();
            for (Column column : rowsetDefinition.restrictionColumns) {
                restrictionList.add(
                    new XmlElement(
                        e.Restrictions.name,
                        null,
                        new XmlElement[]{
                            new XmlElement("Name", null, column.name),
                            new XmlElement(
                                "Type",
                                null,
                                column.getColumnType())}));
            }
            return restrictionList;
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverPropertiesRowset extends Rowset<XmlaDatabaseProperty> {
        private final Util.Predicate1<XmlaPropertyDefinition> propNameCond;

        DiscoverPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaDatabaseProperty.INSTANCE, request, handler);
            propNameCond = makeCondition(PROPDEF_NAME_GETTER, e.PropertyName);
        }

        protected boolean needConnection() {
            return false;
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (XmlaPropertyDefinition prop
                : XmlaPropertyDefinition.class.getEnumConstants())
            {
                if (!propNameCond.test(prop)) {
                    continue;
                }
                Row row = new Row();
                row.set(e.PropertyName.name, prop.name());
                row.set(e.PropertyDescription.name, prop.description);
                row.set(e.PropertyType.name, prop.type.getName());
                row.set(e.PropertyAccessType.name, prop.access);
                row.set(e.IsRequired.name, false);
                row.set(e.Value.name, extra.getPropertyValue(prop));
                addRow(row, rows);
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverEnumeratorsRowset extends Rowset<XmlaEnumerator> {
        DiscoverEnumeratorsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaEnumerator.INSTANCE, request, handler);
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException
        {
            List<Enumeration> enumerators = getEnumerators();
            for (Enumeration enumerator : enumerators) {
                final List<? extends Enum> values = enumerator.getValues();
                for (Enum<?> value : values) {
                    Row row = new Row();
                    row.set(e.EnumName.name, enumerator.name);
                    row.set(e.EnumDescription.name, enumerator.description);

                    // Note: SQL Server always has EnumType string.
                    // Need type of element of array, not the array
                    // itself.
                    row.set(e.EnumType.name, "string");

                    final String name =
                        (value instanceof XmlaConstant)
                            ? ((XmlaConstant) value).xmlaName()
                            : value.name();
                    row.set(e.ElementName.name, name);

                    final String description =
                        (value instanceof XmlaConstant)
                            ? ((XmlaConstant) value).getDescription()
                            : null;
                    if (description != null) {
                        row.set(
                            e.ElementDescription.name,
                            description);
                    }

                    switch (enumerator.type) {
                    case String:
                    case StringArray:
                        // these don't have ordinals
                        break;
                    default:
                        final int ordinal =
                            (value instanceof XmlaConstant
                             && ((XmlaConstant) value).xmlaOrdinal() != -1)
                                ? ((XmlaConstant) value).xmlaOrdinal()
                                : value.ordinal();
                        row.set(e.ElementValue.name, ordinal);
                        break;
                    }
                    addRow(row, rows);
                }
            }
        }

        private static List<Enumeration> getEnumerators() {
            // Build a set because we need to eliminate duplicates.
            SortedSet<Enumeration> enumeratorSet = new TreeSet<Enumeration>(
                new Comparator<Enumeration>() {
                    public int compare(Enumeration o1, Enumeration o2) {
                        return o1.name.compareTo(o2.name);
                    }
                }
            );
            for (RowsetDefinition def2 : RowsetDefinition.values()) {
                for (Column column : def2.columns) {
                    if (column.enumeration != null) {
                        enumeratorSet.add(column.enumeration);
                    }
                }
            }
            return new ArrayList<Enumeration>(enumeratorSet);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverKeywordsRowset extends Rowset<XmlaKeyword> {
        DiscoverKeywordsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaKeyword.INSTANCE, request, handler);
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (String keyword : extra.getKeywords()) {
                Row row = new Row();
                row.set(e.Keyword.name, keyword);
                addRow(row, rows);
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DiscoverLiteralsRowset extends Rowset<XmlaLiteral> {
        DiscoverLiteralsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaLiteral.INSTANCE, request, handler);
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException
        {
            populate(
                XmlaConstants.Literal.class,
                rows,
                new Comparator<XmlaConstants.Literal>() {
                public int compare(
                    XmlaConstants.Literal o1,
                    XmlaConstants.Literal o2)
                {
                    return o1.name().compareTo(o2.name());
                }
            });
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaCatalogsRowset extends Rowset<XmlaCatalog> {
        private final Util.Predicate1<Catalog> catalogNameCond;

        DbschemaCatalogsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaCatalog.INSTANCE, request, handler);
            catalogNameCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
        }

        public void populateImpl(
            XmlaResponse response, OlapConnection connection, List<Row> rows)
            throws XmlaException, SQLException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (Catalog catalog
                : catIter(connection, catalogNameCond, catNameCond()))
            {
                for (Schema schema : catalog.getSchemas()) {
                    Row row = new Row();
                    row.set(e.CatalogName.name, catalog.getName());

                    // TODO: currently schema grammar does not support a
                    // description
                    row.set(e.Description.name, "No description available");

                    // get Role names
                    StringBuilder buf = new StringBuilder(100);
                    final List<String> roleNames =
                        extra.getSchemaRoleNames(schema);
                    serialize(buf, roleNames);
                    row.set(e.Roles.name, buf.toString());

                    // TODO: currently schema grammar does not support modify
                    // date so we return just some date for now.
                    if (false) {
                        row.set(e.DateModified.name, DATE_MODIFIED);
                    }
                    addRow(row, rows);
                }
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaColumnsRowset extends Rowset<XmlaColumn> {
        private final Util.Predicate1<Catalog> tableCatalogCond;
        private final Util.Predicate1<Cube> tableNameCond;
        private final Util.Predicate1<String> columnNameCond;

        DbschemaColumnsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaColumn.INSTANCE, request, handler);
            tableCatalogCond =
                makeCondition(CATALOG_NAME_GETTER, e.TableCatalog);
            tableNameCond = makeCondition(ELEMENT_NAME_GETTER, e.TableName);
            columnNameCond = makeCondition(e.ColumnName);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, OlapException
        {
            for (Catalog catalog
                : catIter(connection, tableCatalogCond, catNameCond()))
            {
                // By definition, mondrian catalogs have only one
                // schema. It is safe to use get(0)
                final Schema schema = catalog.getSchemas().get(0);
                final boolean emitInvisibleMembers =
                    XmlaUtil.shouldEmitInvisibleMembers(request);
                int ordinalPosition = 1;
                Row row;

                for (Cube cube : filter(sortedCubes(schema), tableNameCond)) {
                    for (Dimension dimension : cube.getDimensions()) {
                        for (Hierarchy hierarchy : dimension.getHierarchies()) {
                            ordinalPosition =
                                populateHierarchy(
                                    cube, hierarchy,
                                    ordinalPosition, rows);
                        }
                    }

                    List<Measure> rms = cube.getMeasures();
                    for (int k = 1; k < rms.size(); k++) {
                        Measure member = rms.get(k);

                        // null == true for regular cubes
                        // virtual cubes do not set the visible property
                        // on its measures so it might be null.
                        Boolean visible = (Boolean)
                            member.getPropertyValue(
                                Property.StandardMemberProperty.$visible);
                        if (visible == null) {
                            visible = true;
                        }
                        if (!emitInvisibleMembers && !visible) {
                            continue;
                        }

                        String memberName = member.getName();
                        final String columnName = "Measures:" + memberName;
                        if (!columnNameCond.test(columnName)) {
                            continue;
                        }

                        row = new Row();
                        row.set(e.TableCatalog.name, catalog.getName());
                        row.set(e.TableName.name, cube.getName());
                        row.set(e.ColumnName.name, columnName);
                        row.set(e.OrdinalPosition.name, ordinalPosition++);
                        row.set(e.ColumnHasDefault.name, false);
                        row.set(e.ColumnFlags.name, 0);
                        row.set(e.IsNullable.name, false);
                        // TODO: here is where one tries to determine the
                        // type of the column - since these are all
                        // Measures, aggregate Measures??, maybe they
                        // are all numeric? (or currency)
                        row.set(
                            e.DataType.name,
                            XmlaConstants.DBType.R8.xmlaOrdinal());
                        // TODO: 16/255 seems to be what MS SQL Server
                        // always returns.
                        row.set(e.NumericPrecision.name, 16);
                        row.set(e.NumericScale.name, 255);
                        addRow(row, rows);
                    }
                }
            }
        }

        private int populateHierarchy(
            Cube cube,
            Hierarchy hierarchy,
            int ordinalPosition,
            List<Row> rows)
        {
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();

            if (hierarchy.hasAll()) {
                Row row = new Row();
                row.set(e.TableCatalog.name, schemaName);
                row.set(e.TableName.name, cubeName);
                row.set(e.ColumnName.name, hierarchyName + ":(All)!NAME");
                row.set(e.OrdinalPosition.name, ordinalPosition++);
                row.set(e.ColumnHasDefault.name, false);
                row.set(e.ColumnFlags.name, 0);
                row.set(e.IsNullable.name, false);
                // names are always WSTR
                row.set(
                    e.DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
                row.set(e.CharacterMaximumLength.name, 0);
                row.set(e.CharacterOctetLength.name, 0);
                addRow(row, rows);

                row = new Row();
                row.set(e.TableCatalog.name, schemaName);
                row.set(e.TableName.name, cubeName);
                row.set(
                    e.ColumnName.name, hierarchyName + ":(All)!UNIQUE_NAME");
                row.set(e.OrdinalPosition.name, ordinalPosition++);
                row.set(e.ColumnHasDefault.name, false);
                row.set(e.ColumnFlags.name, 0);
                row.set(e.IsNullable.name, false);
                // names are always WSTR
                row.set(
                    e.DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
                row.set(e.CharacterMaximumLength.name, 0);
                row.set(e.CharacterOctetLength.name, 0);
                addRow(row, rows);

                if (false) {
                    // TODO: SQLServer outputs this hasall KEY column name -
                    // don't know what it's for
                    row = new Row();
                    row.set(e.TableCatalog.name, schemaName);
                    row.set(e.TableName.name, cubeName);
                    row.set(e.ColumnName.name, hierarchyName + ":(All)!KEY");
                    row.set(e.OrdinalPosition.name, ordinalPosition++);
                    row.set(e.ColumnHasDefault.name, false);
                    row.set(e.ColumnFlags.name, 0);
                    row.set(e.IsNullable.name, false);
                    // names are always BOOL
                    row.set(
                        e.DataType.name,
                        XmlaConstants.DBType.BOOL.xmlaOrdinal());
                    row.set(e.NumericPrecision.name, 255);
                    row.set(e.NumericScale.name, 255);
                    addRow(row, rows);
                }
            }

            for (Level level : hierarchy.getLevels()) {
                ordinalPosition =
                    populateLevel(
                        cube, hierarchy, level, ordinalPosition, rows);
            }
            return ordinalPosition;
        }

        private int populateLevel(
            Cube cube,
            Hierarchy hierarchy,
            Level level,
            int ordinalPosition,
            List<Row> rows)
        {
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = hierarchy.getName();
            String levelName = level.getName();

            Row row = new Row();
            row.set(e.TableCatalog.name, schemaName);
            row.set(e.TableName.name, cubeName);
            row.set(
                e.ColumnName.name,
                hierarchyName + ':' + levelName + "!NAME");
            row.set(e.OrdinalPosition.name, ordinalPosition++);
            row.set(e.ColumnHasDefault.name, false);
            row.set(e.ColumnFlags.name, 0);
            row.set(e.IsNullable.name, false);
            // names are always WSTR
            row.set(e.DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
            row.set(e.CharacterMaximumLength.name, 0);
            row.set(e.CharacterOctetLength.name, 0);
            addRow(row, rows);

            row = new Row();
            row.set(e.TableCatalog.name, schemaName);
            row.set(e.TableName.name, cubeName);
            row.set(
                e.ColumnName.name,
                hierarchyName + ':' + levelName + "!UNIQUE_NAME");
            row.set(e.OrdinalPosition.name, ordinalPosition++);
            row.set(e.ColumnHasDefault.name, false);
            row.set(e.ColumnFlags.name, 0);
            row.set(e.IsNullable.name, false);
            // names are always WSTR
            row.set(e.DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
            row.set(e.CharacterMaximumLength.name, 0);
            row.set(e.CharacterOctetLength.name, 0);
            addRow(row, rows);

/*
TODO: see above
            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name,
                hierarchyName + ":" + levelName + "!KEY");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always BOOL
            row.set(DataType.name, DBType.BOOL.ordinal());
            row.set(NumericPrecision.name, 255);
            row.set(NumericScale.name, 255);
            addRow(row, rows);
*/
            NamedList<Property> props = level.getProperties();
            for (Property prop : props) {
                String propName = prop.getName();

                row = new Row();
                row.set(e.TableCatalog.name, schemaName);
                row.set(e.TableName.name, cubeName);
                row.set(
                    e.ColumnName.name,
                    hierarchyName + ':' + levelName + '!' + propName);
                row.set(e.OrdinalPosition.name, ordinalPosition++);
                row.set(e.ColumnHasDefault.name, false);
                row.set(e.ColumnFlags.name, 0);
                row.set(e.IsNullable.name, false);

                XmlaConstants.DBType dbType = getDBTypeFromProperty(prop);
                row.set(e.DataType.name, dbType.xmlaOrdinal());

                switch (prop.getDatatype()) {
                case STRING:
                    row.set(e.CharacterMaximumLength.name, 0);
                    row.set(e.CharacterOctetLength.name, 0);
                    break;
                case INTEGER:
                case UNSIGNED_INTEGER:
                case DOUBLE:
                    // TODO: 16/255 seems to be what MS SQL Server
                    // always returns.
                    row.set(e.NumericPrecision.name, 16);
                    row.set(e.NumericScale.name, 255);
                    break;
                case BOOLEAN:
                    row.set(e.NumericPrecision.name, 255);
                    row.set(e.NumericScale.name, 255);
                    break;
                default:
                    // TODO: what type is it really, its
                    // not a string
                    row.set(e.CharacterMaximumLength.name, 0);
                    row.set(e.CharacterOctetLength.name, 0);
                    break;
                }
                addRow(row, rows);
            }
            return ordinalPosition;
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaProviderTypesRowset
        extends Rowset<XmlaProviderType>
    {
        private final Util.Predicate1<Integer> dataTypeCond;

        DbschemaProviderTypesRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaProviderType.INSTANCE, request, handler);
            dataTypeCond = makeCondition(e.DataType);
        }

        @Override
        protected boolean needConnection() {
            return false;
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException
        {
            // Identifies the (base) data types supported by the data provider.
            Row row;

            // i4
            Integer dt = XmlaConstants.DBType.I4.xmlaOrdinal();
            if (dataTypeCond.test(dt)) {
                row = new Row();
                row.set(e.TypeName.name, XmlaConstants.DBType.I4.userName);
                row.set(e.DataType.name, dt);
                row.set(e.ColumnSize.name, 8);
                row.set(e.IsNullable.name, true);
                row.set(e.Searchable.name, null);
                row.set(e.UnsignedAttribute.name, false);
                row.set(e.FixedPrecScale.name, false);
                row.set(e.AutoUniqueValue.name, false);
                row.set(e.IsLong.name, false);
                row.set(e.BestMatch.name, true);
                addRow(row, rows);
            }

            // R8
            dt = XmlaConstants.DBType.R8.xmlaOrdinal();
            if (dataTypeCond.test(dt)) {
                row = new Row();
                row.set(e.TypeName.name, XmlaConstants.DBType.R8.userName);
                row.set(e.DataType.name, dt);
                row.set(e.ColumnSize.name, 16);
                row.set(e.IsNullable.name, true);
                row.set(e.Searchable.name, null);
                row.set(e.UnsignedAttribute.name, false);
                row.set(e.FixedPrecScale.name, false);
                row.set(e.AutoUniqueValue.name, false);
                row.set(e.IsLong.name, false);
                row.set(e.BestMatch.name, true);
                addRow(row, rows);
            }

            // CY
            dt = XmlaConstants.DBType.CY.xmlaOrdinal();
            if (dataTypeCond.test(dt)) {
                row = new Row();
                row.set(e.TypeName.name, XmlaConstants.DBType.CY.userName);
                row.set(e.DataType.name, dt);
                row.set(e.ColumnSize.name, 8);
                row.set(e.IsNullable.name, true);
                row.set(e.Searchable.name, null);
                row.set(e.UnsignedAttribute.name, false);
                row.set(e.FixedPrecScale.name, false);
                row.set(e.AutoUniqueValue.name, false);
                row.set(e.IsLong.name, false);
                row.set(e.BestMatch.name, true);
                addRow(row, rows);
            }

            // BOOL
            dt = XmlaConstants.DBType.BOOL.xmlaOrdinal();
            if (dataTypeCond.test(dt)) {
                row = new Row();
                row.set(e.TypeName.name, XmlaConstants.DBType.BOOL.userName);
                row.set(e.DataType.name, dt);
                row.set(e.ColumnSize.name, 1);
                row.set(e.IsNullable.name, true);
                row.set(e.Searchable.name, null);
                row.set(e.UnsignedAttribute.name, false);
                row.set(e.FixedPrecScale.name, false);
                row.set(e.AutoUniqueValue.name, false);
                row.set(e.IsLong.name, false);
                row.set(e.BestMatch.name, true);
                addRow(row, rows);
            }

            // I8
            dt = XmlaConstants.DBType.I8.xmlaOrdinal();
            if (dataTypeCond.test(dt)) {
                row = new Row();
                row.set(e.TypeName.name, XmlaConstants.DBType.I8.userName);
                row.set(e.DataType.name, dt);
                row.set(e.ColumnSize.name, 16);
                row.set(e.IsNullable.name, true);
                row.set(e.Searchable.name, null);
                row.set(e.UnsignedAttribute.name, false);
                row.set(e.FixedPrecScale.name, false);
                row.set(e.AutoUniqueValue.name, false);
                row.set(e.IsLong.name, false);
                row.set(e.BestMatch.name, true);
                addRow(row, rows);
            }

            // WSTR
            dt = XmlaConstants.DBType.WSTR.xmlaOrdinal();
            if (dataTypeCond.test(dt)) {
                row = new Row();
                row.set(e.TypeName.name, XmlaConstants.DBType.WSTR.userName);
                row.set(e.DataType.name, dt);
                // how big are the string columns in the db
                row.set(e.ColumnSize.name, 255);
                row.set(e.LiteralPrefix.name, "\"");
                row.set(e.LiteralSuffix.name, "\"");
                row.set(e.IsNullable.name, true);
                row.set(e.CaseSensitive.name, false);
                row.set(e.Searchable.name, null);
                row.set(e.FixedPrecScale.name, false);
                row.set(e.AutoUniqueValue.name, false);
                row.set(e.IsLong.name, false);
                row.set(e.BestMatch.name, true);
                addRow(row, rows);
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaSchemataRowset extends Rowset<XmlaSchema> {
        private final Util.Predicate1<Catalog> catalogNameCond;

        DbschemaSchemataRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaSchema.INSTANCE, request, handler);
            catalogNameCond =
                makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, OlapException
        {
            for (Catalog catalog
                : catIter(connection, catalogNameCond, catNameCond()))
            {
                for (Schema schema : catalog.getSchemas()) {
                    Row row = new Row();
                    row.set(e.CatalogName.name, catalog.getName());
                    row.set(e.SchemaName.name, schema.getName());
                    row.set(e.SchemaOwner.name, "");
                    addRow(row, rows);
                }
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class DbschemaTablesRowset extends Rowset<XmlaTable> {
        private final Util.Predicate1<Catalog> tableCatalogCond;
        private final Util.Predicate1<Cube> tableNameCond;
        private final Util.Predicate1<String> tableTypeCond;

        DbschemaTablesRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaTable.INSTANCE, request, handler);
            tableCatalogCond =
                makeCondition(CATALOG_NAME_GETTER, e.TableCatalog);
            tableNameCond = makeCondition(ELEMENT_NAME_GETTER, e.TableName);
            tableTypeCond = makeCondition(e.TableType);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, OlapException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (Catalog catalog
                : catIter(connection, catNameCond(), tableCatalogCond))
            {
                // By definition, mondrian catalogs have only one
                // schema. It is safe to use get(0)
                final Schema schema = catalog.getSchemas().get(0);
                Row row;
                for (Cube cube : filter(sortedCubes(schema), tableNameCond)) {
                    String desc = cube.getDescription();
                    if (desc == null) {
                        //TODO: currently this is always null
                        desc =
                            catalog.getName() + " - "
                            + cube.getName() + " Cube";
                    }

                    if (tableTypeCond.test("TABLE")) {
                        row = new Row();
                        row.set(e.TableCatalog.name, catalog.getName());
                        row.set(e.TableName.name, cube.getName());
                        row.set(e.TableType.name, "TABLE");
                        row.set(e.Description.name, desc);
                        if (false) {
                            row.set(e.DateModified.name, DATE_MODIFIED);
                        }
                        addRow(row, rows);
                    }


                    if (tableTypeCond.test("SYSTEM TABLE")) {
                        for (Dimension dimension : cube.getDimensions()) {
                            if (dimension.getDimensionType()
                                == Dimension.Type.MEASURE)
                            {
                                continue;
                            }
                            for (Hierarchy hierarchy
                                : dimension.getHierarchies())
                            {
                                populateHierarchy(
                                    extra, cube, hierarchy, rows);
                            }
                        }
                    }
                }
            }
        }

        private void populateHierarchy(
            XmlaHandler.XmlaExtra extra,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
        {
            if (hierarchy.getName().endsWith("$Parent")) {
                // We don't return generated Parent-Child
                // hierarchies.
                return;
            }
            for (Level level : hierarchy.getLevels()) {
                populateLevel(extra, cube, hierarchy, level, rows);
            }
        }

        private void populateLevel(
            XmlaHandler.XmlaExtra extra,
            Cube cube,
            Hierarchy hierarchy,
            Level level,
            List<Row> rows)
        {
            String schemaName = cube.getSchema().getName();
            String cubeName = cube.getName();
            String hierarchyName = extra.getHierarchyName(hierarchy);
            String levelName = level.getName();

            String tableName =
                cubeName + ':' + hierarchyName + ':' + levelName;

            String desc = level.getDescription();
            if (desc == null) {
                //TODO: currently this is always null
                desc =
                    schemaName + " - "
                    + cubeName + " Cube - "
                    + hierarchyName + " Hierarchy - "
                    + levelName + " Level";
            }

            Row row = new Row();
            row.set(e.TableCatalog.name, schemaName);
            row.set(e.TableName.name, tableName);
            row.set(e.TableType.name, "SYSTEM TABLE");
            row.set(e.Description.name, desc);
            if (false) {
                row.set(e.DateModified.name, DATE_MODIFIED);
            }
            addRow(row, rows);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    // TODO: Is this needed????
    static class DbschemaTablesInfoRowset extends Rowset<XmlaTableInfo> {
        DbschemaTablesInfoRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaTableInfo.INSTANCE, request, handler);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, OlapException
        {
            for (Catalog catalog : catIter(connection, catNameCond())) {
                // By definition, mondrian catalogs have only one
                // schema. It is safe to use get(0)
                final Schema schema = catalog.getSchemas().get(0);
                //TODO: Is this cubes or tables? SQL Server returns what
                // in foodmart are cube names for TABLE_NAME
                for (Cube cube : sortedCubes(schema)) {
                    String cubeName = cube.getName();
                    String desc = cube.getDescription();
                    if (desc == null) {
                        //TODO: currently this is always null
                        desc = catalog.getName() + " - " + cubeName + " Cube";
                    }
                    //TODO: SQL Server returns 1000000 for all tables
                    int cardinality = 1000000;
                    String version = "null";

                    Row row = new Row();
                    row.set(e.TableCatalog.name, catalog.getName());
                    row.set(e.TableName.name, cubeName);
                    row.set(e.TableType.name, "TABLE");
                    row.set(e.Bookmarks.name, false);
                    row.set(e.TableVersion.name, version);
                    row.set(e.Cardinality.name, cardinality);
                    row.set(e.Description.name, desc);
                    addRow(row, rows);
                }
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaActionsRowset extends Rowset<XmlaAction> {
        MdschemaActionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaAction.INSTANCE, request, handler);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException
        {
            // mondrian doesn't support actions. It's not an error to ask for
            // them, there just aren't any
        }
    }

    public static class MdschemaCubesRowset extends Rowset<XmlaCube> {
        private final Util.Predicate1<Catalog> catalogNameCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;

        MdschemaCubesRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaCube.INSTANCE, request, handler);
            catalogNameCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
        }

        public static final String MD_CUBTYPE_CUBE = "CUBE";
        public static final String MD_CUBTYPE_VIRTUAL_CUBE = "VIRTUAL CUBE";

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogNameCond))
            {
                for (Schema schema
                    : filter(catalog.getSchemas(), schemaNameCond))
                {
                    for (Cube cube : filter(sortedCubes(schema), cubeNameCond))
                    {
                        String desc = cube.getDescription();
                        if (desc == null) {
                            desc =
                                catalog.getName() + " Schema - "
                                + cube.getName() + " Cube";
                        }

                        Row row = new Row();
                        row.set(e.CatalogName.name, catalog.getName());
                        row.set(e.SchemaName.name, schema.getName());
                        row.set(e.CubeName.name, cube.getName());
                        row.set(e.CubeType.name, extra.getCubeType(cube));
                        //row.set(CubeGuid.name, "");
                        //row.set(CreatedOn.name, "");
                        //row.set(LastSchemaUpdate.name, "");
                        //row.set(SchemaUpdatedBy.name, "");
                        //row.set(LastDataUpdate.name, "");
                        //row.set(DataUpdatedBy.name, "");
                        row.set(e.IsDrillthroughEnabled.name, true);
                        row.set(e.IsWriteEnabled.name, false);
                        row.set(e.IsLinkable.name, false);
                        row.set(e.IsSqlEnabled.name, false);
                        row.set(e.CubeCaption.name, cube.getCaption());
                        row.set(e.Description.name, desc);
                        Format formatter =
                            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                        String formattedDate =
                            formatter.format(
                                extra.getSchemaLoadDate(schema));
                        row.set(e.LastSchemaUpdate.name, formattedDate);
                        if (deep) {
                            row.set(
                                e.Dimensions.name,
                                new MdschemaDimensionsRowset(
                                    wrapRequest(
                                        request,
                                        Olap4jUtil.mapOf(
                                            e.CatalogName,
                                            catalog.getName(),
                                            e.SchemaName,
                                            schema.getName(),
                                            e.CubeName,
                                            cube.getName())),
                                    handler));
                            row.set(
                                e.Sets.name,
                                new MdschemaSetsRowset(
                                    wrapRequest(
                                        request,
                                        Olap4jUtil.mapOf(
                                            e.CatalogName,
                                            catalog.getName(),
                                            e.SchemaName,
                                            schema.getName(),
                                            e.CubeName,
                                            cube.getName())),
                                    handler));
                            row.set(
                                e.Measures.name,
                                new MdschemaMeasuresRowset(
                                    wrapRequest(
                                        request,
                                        Olap4jUtil.mapOf(
                                            e.CatalogName,
                                            catalog.getName(),
                                            e.SchemaName,
                                            schema.getName(),
                                            e.CubeName,
                                            cube.getName())),
                                    handler));
                        }
                        addRow(row, rows);
                    }
                }
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaDimensionsRowset extends Rowset<XmlaDimension> {
        private final Util.Predicate1<Catalog> catalogNameCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<Dimension> dimensionUnameCond;
        private final Util.Predicate1<Dimension> dimensionNameCond;

        MdschemaDimensionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaDimension.INSTANCE, request, handler);
            catalogNameCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            dimensionUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.DimensionUniqueName);
            dimensionNameCond =
                makeCondition(ELEMENT_NAME_GETTER, e.DimensionName);
        }

        public static final int MD_DIMTYPE_OTHER = 3;
        public static final int MD_DIMTYPE_MEASURE = 2;
        public static final int MD_DIMTYPE_TIME = 1;

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogNameCond))
            {
                populateCatalog(connection, catalog, rows);
            }
        }

        protected void populateCatalog(
            OlapConnection connection,
            Catalog catalog,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filteredCubes(schema, cubeNameCond)) {
                    populateCube(connection, catalog, cube, rows);
                }
            }
        }

        protected void populateCube(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Dimension dimension
                : filter(
                    cube.getDimensions(),
                    dimensionNameCond,
                    dimensionUnameCond))
            {
                populateDimension(
                    connection, catalog, cube, dimension, rows);
            }
        }

        protected void populateDimension(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            String desc = dimension.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + dimension.getName() + " Dimension";
            }

            Row row = new Row();
            row.set(e.CatalogName.name, catalog.getName());
            row.set(e.SchemaName.name, cube.getSchema().getName());
            row.set(e.CubeName.name, cube.getName());
            row.set(e.DimensionName.name, dimension.getName());
            row.set(e.DimensionUniqueName.name, dimension.getUniqueName());
            row.set(e.DimensionCaption.name, dimension.getCaption());
            row.set(
                e.DimensionOrdinal.name,
                cube.getDimensions().indexOf(dimension));
            row.set(e.DimensionType.name, getDimensionType(dimension));

            //Is this the number of primaryKey members there are??
            // According to microsoft this is:
            //    "The number of members in the key attribute."
            // There may be a better way of doing this but
            // this is what I came up with. Note that I need to
            // add '1' to the number inorder for it to match
            // match what microsoft SQL Server is producing.
            // The '1' might have to do with whether or not the
            // hierarchy has a 'all' member or not - don't know yet.
            // large data set total for Orders cube 0m42.923s
            Hierarchy firstHierarchy = dimension.getHierarchies().get(0);
            NamedList<Level> levels = firstHierarchy.getLevels();
            Level lastLevel = levels.get(levels.size() - 1);

            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            int n = extra.getLevelCardinality(lastLevel);
            row.set(e.DimensionCardinality.name, n + 1);

            // TODO: I think that this is just the dimension name
            row.set(e.DefaultHierarchy.name, dimension.getUniqueName());
            row.set(e.Description.name, desc);
            row.set(e.IsVirtual.name, false);
            // SQL Server always returns false
            row.set(e.IsReadWrite.name, false);
            // TODO: don't know what to do here
            // Are these the levels with uniqueMembers == true?
            // How are they mapped to specific column numbers?
            row.set(e.DimensionUniqueSettings.name, 0);
            row.set(e.DimensionIsVisible.name, dimension.isVisible());
            if (deep) {
                row.set(
                    e.Hierarchies.name,
                    new MdschemaHierarchiesRowset(
                        wrapRequest(
                            request,
                            Olap4jUtil.mapOf(
                                e.CatalogName,
                                catalog.getName(),
                                e.SchemaName,
                                cube.getSchema().getName(),
                                e.CubeName,
                                cube.getName(),
                                e.DimensionUniqueName,
                                dimension.getUniqueName())),
                        handler));
            }

            addRow(row, rows);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    public static class MdschemaFunctionsRowset
        extends Rowset<XmlaFunction>
    {
        /**
         * http://www.csidata.com/custserv/onlinehelp/VBSdocs/vbs57.htm
         */
        public enum VarType {
            Empty("Uninitialized (default)"),
            Null("Contains no valid data"),
            Integer("Integer subtype"),
            Long("Long subtype"),
            Single("Single subtype"),
            Double("Double subtype"),
            Currency("Currency subtype"),
            Date("Date subtype"),
            String("String subtype"),
            Object("Object subtype"),
            Error("Error subtype"),
            Boolean("Boolean subtype"),
            Variant("Variant subtype"),
            DataObject("DataObject subtype"),
            Decimal("Decimal subtype"),
            Byte("Byte subtype"),
            Array("Array subtype");

            VarType(String description) {
                Util.discard(description);
            }
        }

        private final Util.Predicate1<String> functionNameCond;

        MdschemaFunctionsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaFunction.INSTANCE, request, handler);
            functionNameCond = makeCondition(e.FunctionName);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (Catalog catalog : catIter(connection, catNameCond())) {
                // By definition, mondrian catalogs have only one
                // schema. It is safe to use get(0)
                final Schema schema = catalog.getSchemas().get(0);
                List<XmlaHandler.XmlaExtra.FunctionDefinition> funDefs =
                    new ArrayList<XmlaHandler.XmlaExtra.FunctionDefinition>();

                // olap4j does not support describing functions. Call an
                // auxiliary method.
                extra.getSchemaFunctionList(
                    funDefs,
                    schema,
                    functionNameCond);
                for (XmlaHandler.XmlaExtra.FunctionDefinition funDef : funDefs)
                {
                    Row row = new Row();
                    row.set(e.FunctionName.name, funDef.functionName);
                    row.set(e.Description.name, funDef.description);
                    row.set(e.ParameterList.name, funDef.parameterList);
                    row.set(e.ReturnType.name, funDef.returnType);
                    row.set(e.Origin.name, funDef.origin);
                    //row.set(LibraryName.name, "");
                    row.set(e.InterfaceName.name, funDef.interfaceName);
                    row.set(e.Caption.name, funDef.caption);
                    addRow(row, rows);
                }
            }
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaHierarchiesRowset extends Rowset<XmlaHierarchy> {
        private final Util.Predicate1<Catalog> catalogCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<Dimension> dimensionUnameCond;
        private final Util.Predicate1<Hierarchy> hierarchyUnameCond;
        private final Util.Predicate1<Hierarchy> hierarchyNameCond;

        MdschemaHierarchiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaHierarchy.INSTANCE, request, handler);
            catalogCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            dimensionUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.DimensionUniqueName);
            hierarchyUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.HierarchyUniqueName);
            hierarchyNameCond =
                makeCondition(ELEMENT_NAME_GETTER, e.HierarchyName);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogCond))
            {
                populateCatalog(connection, catalog, rows);
            }
        }

        protected void populateCatalog(
            OlapConnection connection,
            Catalog catalog,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filteredCubes(schema, cubeNameCond)) {
                    populateCube(connection, catalog, cube, rows);
                }
            }
        }

        protected void populateCube(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            int ordinal = 0;
            for (Dimension dimension : cube.getDimensions()) {
                // Must increment ordinal for all dimensions but
                // only output some of them.
                boolean genOutput = dimensionUnameCond.test(dimension);
                if (genOutput) {
                    populateDimension(
                        connection, catalog, cube, dimension, ordinal, rows);
                }
                ordinal += dimension.getHierarchies().size();
            }
        }

        protected void populateDimension(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Dimension dimension,
            int ordinal,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            final NamedList<Hierarchy> hierarchies = dimension.getHierarchies();
            for (Hierarchy hierarchy
                : filter(hierarchies, hierarchyNameCond, hierarchyUnameCond))
            {
                populateHierarchy(
                    connection,
                    catalog,
                    cube,
                    dimension,
                    hierarchy,
                    ordinal + hierarchies.indexOf(hierarchy),
                    rows);
            }
        }

        protected void populateHierarchy(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Dimension dimension,
            Hierarchy hierarchy,
            int ordinal,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            if (hierarchy.getName().endsWith("$Parent")) {
                // We don't return generated Parent-Child
                // hierarchies.
                return;
            }
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            String desc = hierarchy.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + extra.getHierarchyName(hierarchy) + " Hierarchy";
            }

            Row row = new Row();
            row.set(e.CatalogName.name, catalog.getName());
            row.set(e.SchemaName.name, cube.getSchema().getName());
            row.set(e.CubeName.name, cube.getName());
            row.set(e.DimensionUniqueName.name, dimension.getUniqueName());
            row.set(e.HierarchyName.name, hierarchy.getName());
            row.set(e.HierarchyUniqueName.name, hierarchy.getUniqueName());
            //row.set(HierarchyGuid.name, "");

            row.set(e.HierarchyCaption.name, hierarchy.getCaption());
            row.set(e.DimensionType.name, getDimensionType(dimension));
            // The number of members in the hierarchy. Because
            // of the presence of multiple hierarchies, this number
            // might not be the same as DIMENSION_CARDINALITY. This
            // value can be an approximation of the real
            // cardinality. Consumers should not assume that this
            // value is accurate.
            int cardinality = extra.getHierarchyCardinality(hierarchy);
            row.set(e.HierarchyCardinality.name, cardinality);

            row.set(
                e.DefaultMember.name,
                hierarchy.getDefaultMember().getUniqueName());
            if (hierarchy.hasAll()) {
                row.set(
                    e.AllMember.name,
                    hierarchy.getRootMembers().get(0).getUniqueName());
            }
            row.set(e.Description.name, desc);

            //TODO: only support:
            // MD_STRUCTURE_FULLYBALANCED (0)
            // MD_STRUCTURE_RAGGEDBALANCED (1)
            row.set(e.Structure.name, extra.getHierarchyStructure(hierarchy));

            row.set(e.IsVirtual.name, false);
            row.set(e.IsReadWrite.name, false);

            // NOTE that SQL Server returns '0' not '1'.
            row.set(e.DimensionUniqueSettings.name, 0);

            row.set(e.DimensionIsVisible.name, dimension.isVisible());
            row.set(e.HierarchyIsVisible.name, hierarchy.isVisible());

            row.set(e.HierarchyOrdinal.name, ordinal);

            // always true
            row.set(e.DimensionIsShared.name, true);

            row.set(
                e.ParentChild.name,
                extra.isHierarchyParentChild(hierarchy));
            if (deep) {
                row.set(
                    e.Levels.name,
                    new MdschemaLevelsRowset(
                        wrapRequest(
                            request,
                            Olap4jUtil.mapOf(
                                e.CatalogName,
                                catalog.getName(),
                                e.SchemaName,
                                cube.getSchema().getName(),
                                e.CubeName,
                                cube.getName(),
                                e.DimensionUniqueName,
                                dimension.getUniqueName(),
                                e.HierarchyUniqueName,
                                hierarchy.getUniqueName())),
                        handler));
            }
            addRow(row, rows);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaLevelsRowset extends Rowset<XmlaLevel> {
        private final Util.Predicate1<Catalog> catalogCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<Dimension> dimensionUnameCond;
        private final Util.Predicate1<Hierarchy> hierarchyUnameCond;
        private final Util.Predicate1<Level> levelUnameCond;
        private final Util.Predicate1<Level> levelNameCond;

        MdschemaLevelsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaLevel.INSTANCE, request, handler);
            catalogCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            dimensionUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.DimensionUniqueName);
            hierarchyUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.HierarchyUniqueName);
            levelUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.LevelUniqueName);
            levelNameCond = makeCondition(ELEMENT_NAME_GETTER, e.LevelName);
        }

        public static final int MDLEVEL_TYPE_UNKNOWN = 0x0000;
        public static final int MDLEVEL_TYPE_REGULAR = 0x0000;
        public static final int MDLEVEL_TYPE_ALL = 0x0001;
        public static final int MDLEVEL_TYPE_CALCULATED = 0x0002;
        public static final int MDLEVEL_TYPE_TIME = 0x0004;
        public static final int MDLEVEL_TYPE_RESERVED1 = 0x0008;
        public static final int MDLEVEL_TYPE_TIME_YEARS = 0x0014;
        public static final int MDLEVEL_TYPE_TIME_HALF_YEAR = 0x0024;
        public static final int MDLEVEL_TYPE_TIME_QUARTERS = 0x0044;
        public static final int MDLEVEL_TYPE_TIME_MONTHS = 0x0084;
        public static final int MDLEVEL_TYPE_TIME_WEEKS = 0x0104;
        public static final int MDLEVEL_TYPE_TIME_DAYS = 0x0204;
        public static final int MDLEVEL_TYPE_TIME_HOURS = 0x0304;
        public static final int MDLEVEL_TYPE_TIME_MINUTES = 0x0404;
        public static final int MDLEVEL_TYPE_TIME_SECONDS = 0x0804;
        public static final int MDLEVEL_TYPE_TIME_UNDEFINED = 0x1004;

        // TODO: move the following 2 fields to olap4j

        public static final int MDDIMENSIONS_MEMBER_KEY_UNIQUE  = 1;
        public static final int MDDIMENSIONS_MEMBER_NAME_UNIQUE = 2;

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogCond))
            {
                populateCatalog(connection, catalog, rows);
            }
        }

        protected void populateCatalog(
            OlapConnection connection,
            Catalog catalog,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filteredCubes(schema, cubeNameCond)) {
                    populateCube(connection, catalog, cube, rows);
                }
            }
        }

        protected void populateCube(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Dimension dimension
                : filter(cube.getDimensions(), dimensionUnameCond))
            {
                populateDimension(
                    connection, catalog, cube, dimension, rows);
            }
        }

        protected void populateDimension(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Hierarchy hierarchy
                : filter(dimension.getHierarchies(), hierarchyUnameCond))
            {
                populateHierarchy(
                    connection, catalog, cube, hierarchy, rows);
            }
        }

        protected void populateHierarchy(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Level level
                : filter(hierarchy.getLevels(), levelUnameCond, levelNameCond))
            {
                outputLevel(
                    connection, catalog, cube, hierarchy, level, rows);
            }
        }

        /**
         * Outputs a level.
         *
         * @param catalog Catalog name
         * @param cube Cube definition
         * @param hierarchy Hierarchy
         * @param level Level
         * @param rows List of rows to output to
         * @return whether the level is visible
         * @throws mondrian.xmla.XmlaException If error occurs
         */
        protected boolean outputLevel(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Hierarchy hierarchy,
            Level level,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            String desc = level.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + extra.getHierarchyName(hierarchy) + " Hierarchy - "
                    + level.getName() + " Level";
            }

            Row row = new Row();
            row.set(e.CatalogName.name, catalog.getName());
            row.set(e.SchemaName.name, cube.getSchema().getName());
            row.set(e.CubeName.name, cube.getName());
            row.set(
                e.DimensionUniqueName.name,
                hierarchy.getDimension().getUniqueName());
            row.set(e.HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(e.LevelName.name, level.getName());
            row.set(e.LevelUniqueName.name, level.getUniqueName());
            //row.set(LevelGuid.name, "");
            row.set(e.LevelCaption.name, level.getCaption());
            // see notes on this #getDepth()
            row.set(e.LevelNumber.name, level.getDepth());

            // Get level cardinality
            // According to microsoft this is:
            //   "The number of members in the level."
            int n = extra.getLevelCardinality(level);
            row.set(e.LevelCardinality.name, n);

            row.set(e.LevelType.name, getLevelType(level));

            // TODO: most of the time this is correct
            row.set(e.CustomRollupSettings.name, 0);

            int uniqueSettings = 0;
            if (level.getLevelType() == Level.Type.ALL) {
                uniqueSettings |= 2;
            }
            if (extra.isLevelUnique(level)) {
                uniqueSettings |= 1;
            }
            row.set(e.LevelUniqueSettings.name, uniqueSettings);
            row.set(e.LevelIsVisible.name, level.isVisible());
            row.set(e.Description.name, desc);
            addRow(row, rows);
            return true;
        }

        private int getLevelType(Level lev) {
            return lev.getLevelType().xmlaOrdinal();
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }


    public static class MdschemaMeasuresRowset
        extends Rowset<XmlaMeasure>
    {
        public static final int MDMEASURE_AGGR_UNKNOWN = 0;
        public static final int MDMEASURE_AGGR_SUM = 1;
        public static final int MDMEASURE_AGGR_COUNT = 2;
        public static final int MDMEASURE_AGGR_MIN = 3;
        public static final int MDMEASURE_AGGR_MAX = 4;
        public static final int MDMEASURE_AGGR_AVG = 5;
        public static final int MDMEASURE_AGGR_VAR = 6;
        public static final int MDMEASURE_AGGR_STD = 7;
        public static final int MDMEASURE_AGGR_CALCULATED = 127;

        private final Util.Predicate1<Catalog> catalogCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<Measure> measureUnameCond;
        private final Util.Predicate1<Measure> measureNameCond;

        MdschemaMeasuresRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaMeasure.INSTANCE, request, handler);
            catalogCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            measureNameCond = makeCondition(ELEMENT_NAME_GETTER, e.MeasureName);
            measureUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.MeasureUniqueName);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogCond))
            {
                populateCatalog(connection, catalog, rows);
            }
        }

        protected void populateCatalog(
            OlapConnection connection,
            Catalog catalog,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            // SQL Server actually includes the LEVELS_LIST row
            StringBuilder buf = new StringBuilder(100);

            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filteredCubes(schema, cubeNameCond)) {
                    buf.setLength(0);

                    int j = 0;
                    for (Dimension dimension : cube.getDimensions()) {
                        if (dimension.getDimensionType()
                            == Dimension.Type.MEASURE)
                        {
                            continue;
                        }
                        for (Hierarchy hierarchy : dimension.getHierarchies()) {
                            if (hierarchy.getName().endsWith("$Parent")) {
                                continue;
                            }
                            NamedList<Level> levels = hierarchy.getLevels();
                            Level lastLevel = levels.get(levels.size() - 1);
                            if (j++ > 0) {
                                buf.append(',');
                            }
                            buf.append(lastLevel.getUniqueName());
                        }
                    }
                    String levelListStr = buf.toString();

                    List<Member> calcMembers = new ArrayList<Member>();
                    for (Measure measure
                        : filter(
                            cube.getMeasures(),
                            measureNameCond,
                            measureUnameCond))
                    {
                        if (measure.isCalculated()) {
                            // Output calculated measures after stored
                            // measures.
                            calcMembers.add(measure);
                        } else {
                            populateMember(
                                connection, catalog,
                                measure, cube, levelListStr, rows);
                        }
                    }

                    for (Member member : calcMembers) {
                        populateMember(
                            connection, catalog, member, cube, null, rows);
                    }
                }
            }
        }

        private void populateMember(
            OlapConnection connection,
            Catalog catalog,
            Member member,
            Cube cube,
            String levelListStr,
            List<Row> rows)
            throws SQLException
        {
            Boolean visible =
                (Boolean) member.getPropertyValue(
                    Property.StandardMemberProperty.$visible);
            if (visible == null) {
                visible = true;
            }
            if (!visible && !XmlaUtil.shouldEmitInvisibleMembers(request)) {
                return;
            }

            //TODO: currently this is always null
            String desc = member.getDescription();
            if (desc == null) {
                desc =
                    cube.getName() + " Cube - "
                    + member.getName() + " Member";
            }
            final String formatString =
                (String) member.getPropertyValue(
                    Property.StandardCellProperty.FORMAT_STRING);

            Row row = new Row();
            row.set(e.CatalogName.name, catalog.getName());
            row.set(e.SchemaName.name, cube.getSchema().getName());
            row.set(e.CubeName.name, cube.getName());
            row.set(e.MeasureName.name, member.getName());
            row.set(e.MeasureUniqueName.name, member.getUniqueName());
            row.set(e.MeasureCaption.name, member.getCaption());
            //row.set(MeasureGuid.name, "");

            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            row.set(
                e.MeasureAggregator.name, extra.getMeasureAggregator(member));

            // DATA_TYPE DBType best guess is string
            XmlaConstants.DBType dbType = XmlaConstants.DBType.WSTR;
            String datatype = (String)
                member.getPropertyValue(Property.StandardCellProperty.DATATYPE);
            if (datatype != null) {
                if (datatype.equals("Integer")) {
                    dbType = XmlaConstants.DBType.I4;
                } else if (datatype.equals("Numeric")) {
                    dbType = XmlaConstants.DBType.R8;
                } else {
                    dbType = XmlaConstants.DBType.WSTR;
                }
            }
            row.set(e.DataType.name, dbType.xmlaOrdinal());
            row.set(e.MeasureIsVisible.name, visible);

            if (levelListStr != null) {
                row.set(e.LevelsList.name, levelListStr);
            }

            row.set(e.Description.name, desc);
            row.set(e.DefaultFormatString.name, formatString);
            addRow(row, rows);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef, String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaMembersRowset extends Rowset<XmlaMember> {
        private final Util.Predicate1<Catalog> catalogCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<Dimension> dimensionUnameCond;
        private final Util.Predicate1<Hierarchy> hierarchyUnameCond;
        private final Util.Predicate1<Member> memberNameCond;
        private final Util.Predicate1<Member> memberUnameCond;
        private final Util.Predicate1<Member> memberTypeCond;

        MdschemaMembersRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaMember.INSTANCE, request, handler);
            catalogCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            dimensionUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.DimensionUniqueName);
            hierarchyUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.HierarchyUniqueName);
            memberNameCond = makeCondition(ELEMENT_NAME_GETTER, e.MemberName);
            memberUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.MemberUniqueName);
            memberTypeCond = makeCondition(MEMBER_TYPE_GETTER, e.MemberType);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogCond))
            {
                populateCatalog(connection, catalog, rows);
            }
        }

        protected void populateCatalog(
            OlapConnection connection,
            Catalog catalog,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filteredCubes(schema, cubeNameCond)) {
                    if (isRestricted(e.MemberUniqueName)) {
                        // NOTE: it is believed that if MEMBER_UNIQUE_NAME is
                        // a restriction, then none of the remaining possible
                        // restrictions other than TREE_OP are relevant
                        // (or allowed??).
                        outputUniqueMemberName(
                            connection, catalog, cube, rows);
                    } else {
                        populateCube(connection, catalog, cube, rows);
                    }
                }
            }
        }

        protected void populateCube(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            if (isRestricted(e.LevelUniqueName)) {
                // Note: If the LEVEL_UNIQUE_NAME has been specified, then
                // the dimension and hierarchy are specified implicitly.
                String levelUniqueName =
                    getRestrictionValueAsString(e.LevelUniqueName);
                if (levelUniqueName == null) {
                    // The query specified two or more unique names
                    // which means that nothing will match.
                    return;
                }

                Level level = lookupLevel(cube, levelUniqueName);
                if (level != null) {
                    // Get members of this level, without access control, but
                    // including calculated members.
                    List<Member> members = level.getMembers();
                    outputMembers(connection, members, catalog, cube, rows);
                }
            } else {
                for (Dimension dimension
                    : filter(cube.getDimensions(), dimensionUnameCond))
                {
                    populateDimension(
                        connection, catalog, cube, dimension, rows);
                }
            }
        }

        protected void populateDimension(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Hierarchy hierarchy
                : filter(dimension.getHierarchies(), hierarchyUnameCond))
            {
                populateHierarchy(
                    connection, catalog, cube, hierarchy, rows);
            }
        }

        protected void populateHierarchy(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            if (isRestricted(e.LevelNumber)) {
                int levelNumber = getRestrictionValueAsInt(e.LevelNumber);
                if (levelNumber == -1) {
                    LOGGER.warn(
                        "RowsetDefinition.populateHierarchy: "
                        + "LevelNumber invalid");
                    return;
                }
                NamedList<Level> levels = hierarchy.getLevels();
                if (levelNumber >= levels.size()) {
                    LOGGER.warn(
                        "RowsetDefinition.populateHierarchy: "
                        + "LevelNumber ("
                        + levelNumber
                        + ") is greater than number of levels ("
                        + levels.size()
                        + ") for hierarchy \""
                        + hierarchy.getUniqueName()
                        + "\"");
                    return;
                }

                Level level = levels.get(levelNumber);
                List<Member> members = level.getMembers();
                outputMembers(connection, members, catalog, cube, rows);
            } else {
                // At this point we get ALL of the members associated with
                // the Hierarchy (rather than getting them one at a time).
                // The value returned is not used at this point but they are
                // now cached in the SchemaReader.
                for (Level level : hierarchy.getLevels()) {
                    outputMembers(
                        connection, level.getMembers(),
                        catalog, cube, rows);
                }
            }
        }

        /**
         * Returns whether a value contains all of the bits in a mask.
         */
        private static boolean mask(int value, int mask) {
            return (value & mask) == mask;
        }

        /**
         * Adds a member to a result list and, depending upon the
         * <code>treeOp</code> parameter, other relatives of the member. This
         * method recursively invokes itself to walk up, down, or across the
         * hierarchy.
         */
        private void populateMember(
            OlapConnection connection,
            Catalog catalog,
            Cube cube,
            Member member,
            int treeOp,
            List<Row> rows)
            throws SQLException
        {
            // Visit node itself.
            if (mask(treeOp, TreeOp.SELF.xmlaOrdinal())) {
                outputMember(connection, member, catalog, cube, rows);
            }
            // Visit node's siblings (not including itself).
            if (mask(treeOp, TreeOp.SIBLINGS.xmlaOrdinal())) {
                final List<Member> siblings;
                final Member parent = member.getParentMember();
                if (parent == null) {
                    siblings = member.getHierarchy().getRootMembers();
                } else {
                    siblings = Olap4jUtil.cast(parent.getChildMembers());
                }
                for (Member sibling : siblings) {
                    if (sibling.equals(member)) {
                        continue;
                    }
                    populateMember(
                        connection, catalog,
                        cube, sibling,
                        TreeOp.SELF.xmlaOrdinal(), rows);
                }
            }
            // Visit node's descendants or its immediate children, but not both.
            if (mask(treeOp, TreeOp.DESCENDANTS.xmlaOrdinal())) {
                for (Member child : member.getChildMembers()) {
                    populateMember(
                        connection, catalog,
                        cube, child,
                        TreeOp.SELF.xmlaOrdinal() |
                        TreeOp.DESCENDANTS.xmlaOrdinal(),
                        rows);
                }
            } else if (mask(
                    treeOp, TreeOp.CHILDREN.xmlaOrdinal()))
            {
                for (Member child : member.getChildMembers()) {
                    populateMember(
                        connection, catalog,
                        cube, child,
                        TreeOp.SELF.xmlaOrdinal(), rows);
                }
            }
            // Visit node's ancestors or its immediate parent, but not both.
            if (mask(treeOp, TreeOp.ANCESTORS.xmlaOrdinal())) {
                final Member parent = member.getParentMember();
                if (parent != null) {
                    populateMember(
                        connection, catalog,
                        cube, parent,
                        TreeOp.SELF.xmlaOrdinal() |
                        TreeOp.ANCESTORS.xmlaOrdinal(), rows);
                }
            } else if (mask(treeOp, TreeOp.PARENT.xmlaOrdinal())) {
                final Member parent = member.getParentMember();
                if (parent != null) {
                    populateMember(
                        connection, catalog,
                        cube, parent,
                        TreeOp.SELF.xmlaOrdinal(), rows);
                }
            }
        }

        @Override protected void pruneRestrictions(List<Column> list) {
            // If they've restricted TreeOp, we don't want to literally filter
            // the result on TreeOp (because it's not an output column) or
            // on MemberUniqueName (because TreeOp will have caused us to
            // generate other members than the one asked for).
            if (list.contains(e.TreeOp)) {
                list.remove(e.TreeOp);
                list.remove(e.MemberUniqueName);
            }
        }

        private void outputMembers(
            OlapConnection connection,
            List<Member> members,
            final Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws SQLException
        {
            for (Member member : members) {
                outputMember(connection, member, catalog, cube, rows);
            }
        }

        private void outputUniqueMemberName(
            final OlapConnection connection,
            final Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws SQLException
        {
            final Object unameRestrictions =
                restrictions.get(e.MemberUniqueName.name);
            List<String> list;
            if (unameRestrictions instanceof String) {
                list = Collections.singletonList((String) unameRestrictions);
            } else {
                list = (List<String>) unameRestrictions;
            }
            for (String memberUniqueName : list) {
                final IdentifierNode identifierNode =
                    IdentifierNode.parseIdentifier(memberUniqueName);
                Member member =
                    cube.lookupMember(identifierNode.getSegmentList());
                if (member == null) {
                    return;
                }
                if (isRestricted(e.TreeOp)) {
                    int treeOp = getRestrictionValueAsInt(e.TreeOp);
                    if (treeOp == -1) {
                        return;
                    }
                    populateMember(
                        connection, catalog,
                        cube, member, treeOp, rows);
                } else {
                    outputMember(connection, member, catalog, cube, rows);
                }
            }
        }

        private void outputMember(
            OlapConnection connection,
            Member member,
            final Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws SQLException
        {
            if (!memberNameCond.test(member)) {
                return;
            }
            if (!memberTypeCond.test(member)) {
                return;
            }

            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            extra.checkMemberOrdinal(member);

            // Check whether the member is visible, otherwise do not dump.
            Boolean visible =
                (Boolean) member.getPropertyValue(
                    Property.StandardMemberProperty.$visible);
            if (visible == null) {
                visible = true;
            }
            if (!visible && !XmlaUtil.shouldEmitInvisibleMembers(request)) {
                return;
            }

            final Level level = member.getLevel();
            final Hierarchy hierarchy = level.getHierarchy();
            final Dimension dimension = hierarchy.getDimension();

            int adjustedLevelDepth = level.getDepth();

            Row row = new Row();
            row.set(e.CatalogName.name, catalog.getName());
            row.set(e.SchemaName.name, cube.getSchema().getName());
            row.set(e.CubeName.name, cube.getName());
            row.set(e.DimensionUniqueName.name, dimension.getUniqueName());
            row.set(e.HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(e.LevelUniqueName.name, level.getUniqueName());
            row.set(e.LevelNumber.name, adjustedLevelDepth);
            row.set(e.MemberOrdinal.name, member.getOrdinal());
            row.set(e.MemberName.name, member.getName());
            row.set(e.MemberUniqueName.name, member.getUniqueName());
            row.set(e.MemberType.name, member.getMemberType().ordinal());
            //row.set(MemberGuid.name, "");
            row.set(e.MemberCaption.name, member.getCaption());
            row.set(
                e.ChildrenCardinality.name,
                member.getPropertyValue(
                    Property.StandardMemberProperty.CHILDREN_CARDINALITY));
            row.set(e.ChildrenCardinality.name, 100);

            if (adjustedLevelDepth == 0) {
                row.set(e.ParentLevel.name, 0);
            } else {
                row.set(e.ParentLevel.name, adjustedLevelDepth - 1);
                final Member parentMember = member.getParentMember();
                if (parentMember != null) {
                    row.set(
                        e.ParentUniqueName.name, parentMember.getUniqueName());
                }
            }

            row.set(
                e.ParentCount.name, member.getParentMember() == null ? 0 : 1);

            row.set(e.Depth.name, member.getDepth());
            addRow(row, rows);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }

    static class MdschemaSetsRowset extends Rowset<XmlaSet> {
        private final Util.Predicate1<Catalog> catalogCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<NamedSet> setUnameCond;
        private static final String GLOBAL_SCOPE = "1";

        MdschemaSetsRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaSet.INSTANCE, request, handler);
            catalogCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            setUnameCond = makeCondition(ELEMENT_UNAME_GETTER, e.SetName);
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, OlapException
        {
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogCond))
            {
                processCatalog(connection, catalog, rows);
            }
        }

        private void processCatalog(
            OlapConnection connection,
            Catalog catalog,
            List<Row> rows)
            throws OlapException
        {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filter(sortedCubes(schema), cubeNameCond)) {
                    populateNamedSets(cube, catalog, rows);
                }
            }
        }

        private void populateNamedSets(
            Cube cube,
            Catalog catalog,
            List<Row> rows)
        {
            for (NamedSet namedSet : filter(cube.getSets(), setUnameCond)) {
                Row row = new Row();
                row.set(e.CatalogName.name, catalog.getName());
                row.set(e.SchemaName.name, cube.getSchema().getName());
                row.set(e.CubeName.name, cube.getName());
                row.set(e.SetName.name, namedSet.getUniqueName());
                row.set(e.Scope.name, GLOBAL_SCOPE);
                row.set(e.Description.name, namedSet.getDescription());
                addRow(row, rows);
            }
        }
    }

    static class MdschemaPropertiesRowset extends Rowset<XmlaProperty> {
        private final Util.Predicate1<Catalog> catalogCond;
        private final Util.Predicate1<Schema> schemaNameCond;
        private final Util.Predicate1<Cube> cubeNameCond;
        private final Util.Predicate1<Dimension> dimensionUnameCond;
        private final Util.Predicate1<Hierarchy> hierarchyUnameCond;
        private final Util.Predicate1<Property> propertyNameCond;

        MdschemaPropertiesRowset(XmlaRequest request, XmlaHandler handler) {
            super(XmlaProperty.INSTANCE, request, handler);
            catalogCond = makeCondition(CATALOG_NAME_GETTER, e.CatalogName);
            schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, e.SchemaName);
            cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, e.CubeName);
            dimensionUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.DimensionUniqueName);
            hierarchyUnameCond =
                makeCondition(ELEMENT_UNAME_GETTER, e.HierarchyUniqueName);
            propertyNameCond =
                makeCondition(ELEMENT_NAME_GETTER, e.PropertyName);
        }

        protected boolean needConnection() {
            return false;
        }

        public void populateImpl(
            XmlaResponse response,
            OlapConnection connection,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            // Default PROPERTY_TYPE is MDPROP_MEMBER.
            @SuppressWarnings({"unchecked"})
            final List<String> list =
                (List<String>) restrictions.get(e.PropertyType.name);
            Set<Property.TypeFlag> typeFlags;
            if (list == null) {
                typeFlags =
                    Olap4jUtil.enumSetOf(
                        Property.TypeFlag.MEMBER);
            } else {
                typeFlags =
                    Property.TypeFlag.DICTIONARY.forMask(
                        Integer.valueOf(list.get(0)));
            }

            for (Property.TypeFlag typeFlag : typeFlags) {
                switch (typeFlag) {
                case MEMBER:
                    populateMember(rows);
                    break;
                case CELL:
                    populateCell(rows);
                    break;
                case SYSTEM:
                case BLOB:
                default:
                    break;
                }
            }
        }

        private void populateCell(List<Row> rows) {
            for (Property.StandardCellProperty property
                : Property.StandardCellProperty.values())
            {
                Row row = new Row();
                row.set(
                    e.PropertyType.name,
                    Property.TypeFlag.DICTIONARY
                        .toMask(
                            property.getType()));
                row.set(e.PropertyName.name, property.name());
                row.set(e.PropertyCaption.name, property.getCaption());
                row.set(e.DataType.name, property.getDatatype().xmlaOrdinal());
                addRow(row, rows);
            }
        }

        private void populateMember(List<Row> rows) throws SQLException {
            OlapConnection connection =
                handler.getConnection(
                    request, Collections.<String, String>emptyMap());
            for (Catalog catalog
                : catIter(connection, catNameCond(), catalogCond))
            {
                populateCatalog(catalog, rows);
            }
        }

        protected void populateCatalog(
            Catalog catalog,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
                for (Cube cube : filteredCubes(schema, cubeNameCond)) {
                    populateCube(catalog, cube, rows);
                }
            }
        }

        protected void populateCube(
            Catalog catalog,
            Cube cube,
            List<Row> rows)
            throws XmlaException, SQLException
        {
            if (cube instanceof SharedDimensionHolderCube) {
                return;
            }
            if (isRestricted(e.LevelUniqueName)) {
                // Note: If the LEVEL_UNIQUE_NAME has been specified, then
                // the dimension and hierarchy are specified implicitly.
                String levelUniqueName =
                    getRestrictionValueAsString(e.LevelUniqueName);
                if (levelUniqueName == null) {
                    // The query specified two or more unique names
                    // which means that nothing will match.
                    return;
                }
                Level level = lookupLevel(cube, levelUniqueName);
                if (level == null) {
                    return;
                }
                populateLevel(
                    catalog, cube, level, rows);
            } else {
                for (Dimension dimension
                    : filter(cube.getDimensions(), dimensionUnameCond))
                {
                    populateDimension(
                        catalog, cube, dimension, rows);
                }
            }
        }

        private void populateDimension(
            Catalog catalog,
            Cube cube,
            Dimension dimension,
            List<Row> rows)
            throws SQLException
        {
            for (Hierarchy hierarchy
                : filter(dimension.getHierarchies(), hierarchyUnameCond))
            {
                populateHierarchy(
                    catalog, cube, hierarchy, rows);
            }
        }

        private void populateHierarchy(
            Catalog catalog,
            Cube cube,
            Hierarchy hierarchy,
            List<Row> rows)
            throws SQLException
        {
            for (Level level : hierarchy.getLevels()) {
                populateLevel(catalog, cube, level, rows);
            }
        }

        private void populateLevel(
            Catalog catalog,
            Cube cube,
            Level level,
            List<Row> rows)
            throws SQLException
        {
            final XmlaHandler.XmlaExtra extra =
                handler.connectionFactory.getExtra();
            for (Property property
                : filter(extra.getLevelProperties(level), propertyNameCond))
            {
                if (extra.isPropertyInternal(property)) {
                    continue;
                }
                outputProperty(
                    extra, property, catalog, cube, level, rows);
            }
        }

        private void outputProperty(
            XmlaHandler.XmlaExtra extra,
            Property property,
            Catalog catalog,
            Cube cube,
            Level level,
            List<Row> rows)
        {
            Hierarchy hierarchy = level.getHierarchy();
            Dimension dimension = hierarchy.getDimension();

            String propertyName = property.getName();

            Row row = new Row();
            row.set(e.CatalogName.name, catalog.getName());
            row.set(e.SchemaName.name, cube.getSchema().getName());
            row.set(e.CubeName.name, cube.getName());
            row.set(e.DimensionUniqueName.name, dimension.getUniqueName());
            row.set(e.HierarchyUniqueName.name, hierarchy.getUniqueName());
            row.set(e.LevelUniqueName.name, level.getUniqueName());
            //TODO: what is the correct value here
            //row.set(MemberUniqueName.name, "");

            row.set(e.PropertyName.name, propertyName);
            // Only member properties now
            row.set(
                e.PropertyType.name,
                Property.TypeFlag.MEMBER.xmlaOrdinal());
            row.set(
                e.PropertyContentType.name,
                Property.ContentType.REGULAR.xmlaOrdinal());
            row.set(e.PropertyCaption.name, property.getCaption());
            XmlaConstants.DBType dbType = getDBTypeFromProperty(property);
            row.set(e.DataType.name, dbType.xmlaOrdinal());

            String desc =
                cube.getName() + " Cube - "
                + extra.getHierarchyName(hierarchy) + " Hierarchy - "
                + level.getName() + " Level - "
                + property.getName() + " Property";
            row.set(e.Description.name, desc);

            addRow(row, rows);
        }

        protected void setProperty(
            XmlaPropertyDefinition propertyDef,
            String value)
        {
            switch (propertyDef) {
            case Content:
                break;
            default:
                super.setProperty(propertyDef, value);
            }
        }
    }
}

// End Rowsets.java
