package org.olap4j.xmla;

import java.util.List;

/**
 * XML for Analysis entity representing a Keyword.
 *
 * <p>Corresponds to the XML/A {@code DISCOVER_KEYWORDS} schema rowset.</p>
 */
public class XmlaKeyword extends Entity {
    public static final XmlaKeyword INSTANCE =
        new XmlaKeyword();

    public RowsetDefinition def() {
        return RowsetDefinition.DISCOVER_KEYWORDS;
    }

    List<Column> columns() {
        return list(Keyword);
    }

    List<Column> sortColumns() {
        return list(); // not sorted
    }

    public final Column Keyword =
        new Column(
            "Keyword",
            XmlaType.StringSometimesArray,
            null,
            Column.RESTRICTION,
            Column.REQUIRED,
            "A list of all the keywords reserved by a provider.\n"
            + "Example: AND");
}

// End XmlaKeyword.java
