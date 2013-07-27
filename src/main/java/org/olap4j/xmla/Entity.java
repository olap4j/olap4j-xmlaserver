package org.olap4j.xmla;

import java.util.*;

/**
* Created with IntelliJ IDEA.
* User: jhyde
* Date: 7/26/13
* Time: 3:51 PM
* To change this template use File | Settings | File Templates.
*/
public abstract class Entity {
    protected static List<Column> list(Column... columns) {
        switch (columns.length) {
        case 0:
            return Collections.emptyList();
        case 1:
            return Collections.singletonList(columns[0]);
        default:
            return Collections.unmodifiableList(
                Arrays.asList(columns));
        }
    }

    // These methods create a new list each time; they should be called
    // only when constructing static objects, not at runtime.

    abstract List<Column> columns();
    abstract List<Column> sortColumns();
    public abstract RowsetDefinition def();
}

// End Entity.java
