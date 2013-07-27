package org.olap4j.xmla;

import java.util.Arrays;
import java.util.List;

/**
 * XML for Analysis entity representing a Schema Rowset.
 *
 * <p>Corresponds to the XML/A {@code DISCOVER_SCHEMA_ROWSETS} schema
 * rowset.</p>
 */
public class XmlaSchemaRowset extends Entity {
    public static XmlaSchemaRowset INSTANCE =
        new XmlaSchemaRowset();

    public RowsetDefinition def() {
        return RowsetDefinition.DISCOVER_SCHEMA_ROWSETS;
    }

    List<Column> columns() {
        return Arrays.asList(
            SchemaName, SchemaGuid, Restrictions, Description);
    }

    List<Column> sortColumns() {
        return list(); // not sorted
    }

    public final Column SchemaName =
        new Column(
            "SchemaName",
            XmlaType.StringArray,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "The name of the schema/request. This returns the values in "
            + "the RequestTypes enumeration, plus any additional types "
            + "supported by the provider. The provider defines rowset "
            + "structures for the additional types");
    public final Column SchemaGuid =
        new Column(
            "SchemaGuid",
            XmlaType.UUID,
            null,
            Column.NOT_RESTRICTION,
            Column.OPTIONAL,
            "The GUID of the schema.");
    public final Column Restrictions =
        new Column(
            "Restrictions",
            XmlaType.Array,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "An array of the restrictions suppoted by provider. An example "
            + "follows this table.");
    public final Column Description =
        new Column(
            "Description",
            XmlaType.String,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "A localizable description of the schema");
}

// End XmlaSchemaRowset.java
