/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import java.util.*;

/**
 * Cut down version of MondrianServer to make olap4j-xmlaserver happy.
 * Will obsolete soon.
 */
public class MondrianServer {
    private static final MondrianServer INSTANCE = new MondrianServer();

    private MondrianVersion version = new MondrianVersion() {
        public String getVersionString() {
            return null;
        }

        public int getMajorVersion() {
            return 0;
        }

        public int getMinorVersion() {
            return 0;
        }

        public String getProductName() {
            return null;
        }
    };

    /**
     * Returns the server with the given id.
     *
     * <p>If id is null, returns the catalog-less server. (The catalog-less
     * server can also be acquired using its id.)</p>
     *
     * <p>If server is not found, returns null.</p>
     *
     * @param instanceId Server instance id
     * @return Server, or null if no server with this id
     */
    public static MondrianServer forId(String instanceId) {
        return INSTANCE;
    }

    /**
     * Returns the version of this MondrianServer.
     *
     * @return Server's version
     */
    public MondrianVersion getVersion() {
        return version;
    }

    /**
     * Returns a list of MDX keywords.
     * @return list of MDX keywords
     */
    public List<String> getKeywords() {
        return Collections.emptyList();
    }

    /**
     * Description of the version of the server.
     */
    public interface MondrianVersion {
        /**
         * Returns the version string, for example "2.3.0".
         *
         * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
         * @return Version of this server
         */
        String getVersionString();

        /**
         * Returns the major part of the version number.
         *
         * <p>For example, if the full version string is "2.3.0", the major
         * version is 2.
         *
         * @return major part of the version number
         * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
         */
        int getMajorVersion();

        /**
         * Returns the minor part of the version number.
         *
         * <p>For example, if the full version string is "2.3.0", the minor
         * version is 3.
         *
         * @return minor part of the version number
         *
         * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
         */
        int getMinorVersion();

        /**
         * Retrieves the name of this database product.
         *
         * @return database product name
         * @see java.sql.DatabaseMetaData#getDatabaseProductName()
         */
        String getProductName();
    }

}

// End MondrianServer.java
