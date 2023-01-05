/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2009, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.dbunit.database;

import org.dbunit.util.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DefaultMetadataHandler implements IMetadataHandler {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DefaultMetadataHandler.class);

    public ResultSet getColumns(DatabaseMetaData databaseMetaData, String databaseName, String tableName) throws SQLException {
        return databaseMetaData.getColumns(databaseName, null, tableName, null);
    }

    public boolean matches(ResultSet resultSet, String database, String table) throws SQLException {
        return matches(resultSet, database, table, null);
    }

    public boolean matches(ResultSet columnsResultSet, String database,String table, String column) throws SQLException {
        String databaseName = columnsResultSet.getString(1);
        String tableName = columnsResultSet.getString(3);
        String columnName = columnsResultSet.getString(4);


        boolean areEqual =
                areEqualIgnoreNull(database, databaseName) &&
                        areEqualIgnoreNull(table, tableName) &&
                        areEqualIgnoreNull(column, columnName);
        return areEqual;
    }

    private boolean areEqualIgnoreNull(String value1, String value2) {
        return SQLHelper.areEqualIgnoreNull(value1, value2);
    }

    public String getSchema(ResultSet resultSet) throws SQLException {
        if (logger.isTraceEnabled())
            logger.trace("getColumns(resultSet={}) - start", resultSet);

        String schemaName = resultSet.getString(2);
        return schemaName;
    }

    public boolean tableExists(DatabaseMetaData metaData, String schemaName, String tableName) throws SQLException {
        ResultSet tableRs = metaData.getTables(null, schemaName, tableName, null);
        try {
            return tableRs.next();
        } finally {
            SQLHelper.close(tableRs);
        }
    }

    public ResultSet getTables(DatabaseMetaData metaData, String schemaName, String[] tableType)
            throws SQLException {
        if (logger.isTraceEnabled())
            logger.trace("getTables(metaData={}, schemaName={}, tableType={}) - start",
                    new Object[]{metaData, schemaName, tableType});

        return metaData.getTables(null, schemaName, "%", tableType);
    }

    public ResultSet getPrimaryKeys(DatabaseMetaData metaData, String schemaName, String tableName)
            throws SQLException {

        ResultSet resultSet = metaData.getPrimaryKeys(null, schemaName, tableName);
        return resultSet;
    }

}
