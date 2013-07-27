package org.olap4j.xmla;

import java.util.List;

/**
 * XML for Analysis entity representing a Catalog.
 *
 * <p>Corresponds to the XML/A {@code DBSCHEMA_CATALOGS} schema rowset.</p>
 */
public class XmlaCatalog extends Entity {
    public static final XmlaCatalog INSTANCE =
        new XmlaCatalog();

    public RowsetDefinition def() {
        return RowsetDefinition.DBSCHEMA_CATALOGS;
    }

    List<Column> columns() {
        return list(
            CatalogName,
            Description,
            Roles,
            DateModified);
    }

    List<Column> sortColumns() {
        return list(CatalogName);
    }

    public final Column CatalogName =
        new Column(
            "CATALOG_NAME",
            XmlaType.String,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "Catalog name. Cannot be NULL.");
    public final Column Description =
        new Column(
            "DESCRIPTION",
            XmlaType.String,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "Human-readable description of the catalog.");
    public final Column Roles =
        new Column(
            "ROLES",
            XmlaType.String,
            null,
            Column.NOT_RESTRICTION,
            Column.REQUIRED,
            "A comma delimited list of roles to which the current user "
            + "belongs. An asterisk (*) is included as a role if the "
            + "current user is a server or database administrator. "
            + "Username is appended to ROLES if one of the roles uses "
            + "dynamic security.");
    public final Column DateModified =
        new Column(
            "DATE_MODIFIED",
            XmlaType.DateTime,
            null,
            Column.NOT_RESTRICTION,
            Column.OPTIONAL,
            "The date that the catalog was last modified.");
}

// End XmlaCatalog.java
