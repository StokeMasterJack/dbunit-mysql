/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
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

import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.util.QualifiedTableName;
import org.dbunit.util.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Container for the metadata for one database table. The metadata is initialized
 * using a {@link IDatabaseConnection}.
 *
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @see ITableMetaData
 * @since Mar 8, 2002
 */
public class DatabaseTableMetaData extends AbstractTableMetaData {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseTableMetaData.class);

    /**
     * Table name, potentially qualified
     */
    private final QualifiedTableName _qualifiedTableNameSupport;
    private final String _originalTableName;
    private final IDatabaseConnection _connection;
    private Column[] _columns;
    private Column[] _primaryKeys;
    //added by hzhan032
    private IColumnFilter lastKeyFilter;


    DatabaseTableMetaData(final String tableName, IDatabaseConnection connection, boolean validate) throws DataSetException {
        _connection = connection;
        _originalTableName = tableName;
        _qualifiedTableNameSupport = new QualifiedTableName(_originalTableName, _connection.getDatabase());
    }

    public static ITableMetaData createMetaData(String tableName, ResultSet resultSet, IDataTypeFactory dataTypeFactory) throws DataSetException, SQLException {
        return new ResultSetTableMetaData(tableName, resultSet, dataTypeFactory, false);
    }


    public static ITableMetaData createMetaData(String tableName, ResultSet resultSet, IDatabaseConnection connection) throws SQLException, DataSetException {
        return new ResultSetTableMetaData(tableName, resultSet, connection, false);
    }

    private String[] getPrimaryKeyNames() throws SQLException {
        logger.debug("getPrimaryKeyNames() - start");

        String schemaName = _qualifiedTableNameSupport.getDatabase();
        String tableName = _qualifiedTableNameSupport.getTable();

        Connection connection = _connection.getConnection();
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        DatabaseConfig config = _connection.getConfig();
        IMetadataHandler metadataHandler = (IMetadataHandler) config.getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);

        ResultSet resultSet = metadataHandler.getPrimaryKeys(databaseMetaData, schemaName, tableName);

        List list = new ArrayList();
        try {
            while (resultSet.next()) {
                String name = resultSet.getString(4);
                int sequence = resultSet.getInt(5);
                list.add(new PrimaryKeyData(name, sequence));
            }
        } finally {
            resultSet.close();
        }

        Collections.sort(list);
        String[] keys = new String[list.size()];
        for (int i = 0; i < keys.length; i++) {
            PrimaryKeyData data = (PrimaryKeyData) list.get(i);
            keys[i] = data.getName();
        }

        return keys;
    }

    private class PrimaryKeyData implements Comparable {
        private final String _name;
        private final int _index;

        public PrimaryKeyData(String name, int index) {
            _name = name;
            _index = index;
        }

        public String getName() {
            logger.debug("getName() - start");

            return _name;
        }

        public int getIndex() {
            return _index;
        }

        ////////////////////////////////////////////////////////////////////////
        // Comparable interface

        public int compareTo(Object o) {
            PrimaryKeyData data = (PrimaryKeyData) o;
            return getIndex() - data.getIndex();
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // ITableMetaData interface

    public String getTableName() {
        return this._originalTableName;
    }

    public Column[] getColumns()  {
        if (_columns == null) {
            try {
                // qualified names support
                String databaseName = _qualifiedTableNameSupport.getDatabase();
                String tableName = _qualifiedTableNameSupport.getTable();

                Connection jdbcConnection = _connection.getConnection();
                DatabaseMetaData databaseMetaData = jdbcConnection.getMetaData();

                DatabaseConfig config = _connection.getConfig();

                IMetadataHandler metadataHandler = (IMetadataHandler) config.getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);
                ResultSet resultSet = metadataHandler.getColumns(databaseMetaData, databaseName, tableName);

                try {
                    IDataTypeFactory dataTypeFactory = super.getDataTypeFactory(_connection);
                    boolean datatypeWarning = config.getFeature(  DatabaseConfig.FEATURE_DATATYPE_WARNING);

                    List columnList = new ArrayList();
                    while (resultSet.next()) {
                        // Check for exact table/schema name match because
                        // databaseMetaData.getColumns() uses patterns for the lookup
                        boolean match = metadataHandler.matches(resultSet, databaseName, tableName);
                        if (match) {
                            org.dbunit.dataset.Column column = null;
                            try {
                                column = org.dbunit.util.SQLHelper.createColumn(resultSet, dataTypeFactory, datatypeWarning);
                            } catch (org.dbunit.dataset.datatype.DataTypeException e) {
                                throw new RuntimeException(e);
                            }
                            if (column != null) {
                                columnList.add(column);
                            }
                        } else {
                            System.err.println("Skipping <schema.table> '" + resultSet.getString(2) + "." + resultSet.getString(3) + "' because names do not exactly match.");
                        }
                    }

                    if (columnList.size() == 0) {
                        logger.warn("No columns found for table '" + tableName + "' that are supported by dbunit. " +
                                "Will return an empty column list");
                    }

                    _columns = (Column[]) columnList.toArray(new Column[0]);
                } finally {
                    resultSet.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return _columns;
    }

    private boolean primaryKeyFilterChanged(IColumnFilter keyFilter) {
        return (keyFilter != lastKeyFilter);
    }

    public Column[] getPrimaryKeys() throws DataSetException {
        logger.debug("getPrimaryKeys() - start");
        DatabaseConfig config = _connection.getConfig();
        IColumnFilter primaryKeysFilter = (IColumnFilter) config.getProperty(
                DatabaseConfig.PROPERTY_PRIMARY_KEY_FILTER);

        if (_primaryKeys == null || primaryKeyFilterChanged(primaryKeysFilter)) {
            try {
                lastKeyFilter = primaryKeysFilter;
                if (primaryKeysFilter != null) {
                    _primaryKeys = Columns.getColumns(getTableName(), getColumns(),
                            primaryKeysFilter);
                } else {
                    String[] pkNames = getPrimaryKeyNames();
                    _primaryKeys = Columns.getColumns(pkNames, getColumns());
                }
            } catch (SQLException e) {
                throw new DataSetException(e);
            }
        }
        return _primaryKeys;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Object class
    public String toString() {
        try {
            String tableName = getTableName();
            String columns = Arrays.asList(getColumns()).toString();
            String primaryKeys = Arrays.asList(getPrimaryKeys()).toString();
            return "table=" + tableName + ", cols=" + columns + ", pk=" + primaryKeys + "";
        } catch (DataSetException e) {
            return super.toString();
        }
    }
}
