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
package org.dbunit.ext.mysql;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dbunit.database.IMetadataHandler;
import org.dbunit.util.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlMetadataHandler implements IMetadataHandler {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(MySqlMetadataHandler.class);

    //(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern
    public ResultSet getColumns(DatabaseMetaData databaseMetaData, String databaseName, String tableName) throws SQLException {
        return databaseMetaData.getColumns(databaseName, null, tableName,null);
    }
    
    public boolean matches(ResultSet resultSet,  String database, String table)    throws SQLException {
        return matches(resultSet, database, table, null);
    }

    public boolean matches(ResultSet columnsResultSet, String database,  String table, String column) throws SQLException {
        String databaseName = columnsResultSet.getString(1);
        String tableName = columnsResultSet.getString(3);
        String columnName = columnsResultSet.getString(4);
        return areEqualIgnoreNull(database, databaseName) && areEqualIgnoreNull(table, tableName) && areEqualIgnoreNull(column, columnName);
    }

    private boolean areEqualIgnoreNull(String value1, String value2) {
        return SQLHelper.areEqualIgnoreNull(value1, value2);
    }

    public String getSchema(ResultSet resultSet) throws SQLException {
        String catalogName = resultSet.getString(1);
        String schemaName = resultSet.getString(2);
        
        // Fix schema/catalog for mysql. Normally the schema is not set but only the catalog is set
        if(schemaName == null && catalogName != null) {
            logger.debug("Using catalogName '" + catalogName + "' as schema since the schema is null but the catalog is set (probably in a MySQL environment).");
            schemaName = catalogName;
        }
        return schemaName;
    }

    public boolean tableExists(DatabaseMetaData metaData, String schema, String tableName) 
    throws SQLException 
    {
        ResultSet tableRs = metaData.getTables(schema, null, tableName, null);
        try 
        {
            return tableRs.next();
        }
        finally
        {
            SQLHelper.close(tableRs);
        }
    }

    public ResultSet getTables(DatabaseMetaData metaData, String schemaName, String[] tableType) throws SQLException {
        return metaData.getTables(schemaName, null, "%", tableType);
    }

    public ResultSet getPrimaryKeys(DatabaseMetaData metaData, String schemaName, String tableName) throws SQLException {

        ResultSet resultSet = metaData.getPrimaryKeys(
                schemaName, null, tableName);
        return resultSet;
    }

}
