/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2005 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * Cut-down version of MondrianException.
 */
public class MondrianException extends RuntimeException {
    public MondrianException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getLocalizedMessage() {
        return getMessage();
    }

    public String getMessage() {
        return "Mondrian Error:" + super.getMessage();
    }
}

// End MondrianException.java
