package org.olap4j.xmla;

import java.util.List;

/**
 * XML for Analysis entity representing a Schema.
 *
 * <p>Corresponds to the XML/A {@code DBSCHEMA_SCHEMATA} schema rowset.</p>
 */
public class XmlaSchema extends Entity {
    public static final XmlaSchema INSTANCE =
        new XmlaSchema();

    public RowsetDefinition def() {
        return RowsetDefinition.DBSCHEMA_SCHEMATA;
    }

    public List<Column> columns() {
        return list(
            CatalogName,
            SchemaName,
            SchemaOwner);
    }

    public List<Column> sortColumns() {
        return list(
            CatalogName,
            SchemaName,
            SchemaOwner);
    }

    public final Column CatalogName =
        new Column(
            "CATALOG_NAME",
            XmlaType.String,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "The provider-specific data type name.");
    public final Column SchemaName =
        new Column(
            "SCHEMA_NAME",
            XmlaType.String,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "The indicator of the data type.");
    public final Column SchemaOwner =
        new Column(
            "SCHEMA_OWNER",
            XmlaType.String,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "The length of a non-numeric column. If the data type is "
            + "numeric, this is the upper bound on the maximum precision "
            + "of the data type.");
}

// End XmlaSchema.java
