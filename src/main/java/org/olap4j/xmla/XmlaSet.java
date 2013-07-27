package org.olap4j.xmla;

import java.util.List;

/**
 * XML for Analysis entity representing a Set.
 *
 * <p>Corresponds to the XML/A {@code MDSCHEMA_SETS} schema rowset.</p>
 */
public class XmlaSet extends Entity {
    public static final XmlaSet INSTANCE =
        new XmlaSet();

    public RowsetDefinition def() {
        return RowsetDefinition.MDSCHEMA_SETS;
    }

    public List<Column> columns() {
        return list(
            CatalogName,
            SchemaName,
            CubeName,
            SetName,
            Scope);
    }

    public List<Column> sortColumns() {
        return list(
            CatalogName,
            SchemaName,
            CubeName);
    }

    public final Column CatalogName =
        new Column(
            "CATALOG_NAME",
            XmlaType.String,
            null,
            true,
            true,
            null);
    public final Column SchemaName =
        new Column(
            "SCHEMA_NAME",
            XmlaType.String,
            null,
            true,
            true,
            null);
    public final Column CubeName =
        new Column(
            "CUBE_NAME",
            XmlaType.String,
            null,
            true,
            false,
            null);
    public final Column SetName =
        new Column(
            "SET_NAME",
            XmlaType.String,
            null,
            true,
            false,
            null);
    public final Column SetCaption =
        new Column(
            "SET_CAPTION",
            XmlaType.String,
            null,
            true,
            true,
            null);
    public final Column Scope =
        new Column(
            "SCOPE",
            XmlaType.Integer,
            null,
            true,
            false,
            null);
    public final Column Description =
        new Column(
            "DESCRIPTION",
            XmlaType.String,
            null,
            false,
            true,
            "A human-readable description of the measure.");
}

// End XmlaSet.java
