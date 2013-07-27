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
package org.olap4j.xmla;

/**
* Type of a {@link Column} in an XML/A row set.
*/
public enum XmlaType {
    String("xsd:string"),
    StringArray("xsd:string"),
    Array("xsd:string"),
    Enumeration("xsd:string"),
    EnumerationArray("xsd:string"),
    EnumString("xsd:string"),
    Boolean("xsd:boolean"),
    StringSometimesArray("xsd:string"),
    Integer("xsd:int"),
    UnsignedInteger("xsd:unsignedInt"),
    DateTime("xsd:dateTime"),
    Rowset(null),
    Short("xsd:short"),
    UUID("uuid"),
    UnsignedShort("xsd:unsignedShort"),
    Long("xsd:long"),
    UnsignedLong("xsd:unsignedLong");

    public final String columnType;

    XmlaType(String columnType) {
        this.columnType = columnType;
    }

    boolean isEnum() {
        return this == Enumeration
           || this == EnumerationArray
           || this == EnumString;
    }

    public String getName() {
        return this == String ? "string" : name();
    }
}


// End XmlaType.java
