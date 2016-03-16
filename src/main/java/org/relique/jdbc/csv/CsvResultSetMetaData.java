/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *  Copyright (C) 2008, 2011  Mario Frasca
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * This class implements the java.sql.ResultSetMetaData JDBC interface for the
 * CsvJdbc driver.
 */
public class CsvResultSetMetaData implements ResultSetMetaData
{

	/** Names of columns */
	private String[] columnNames;
	private String[] columnLabels;
	private String[] columnTypes;
	private int[] columnDisplaySizes;
	/** Name of table */
	private String tableName;

	/**
	 * Constructor for the CsvResultSetMetaData object
	 * 
	 * @param tableName
	 *            Name of table
	 * @param columnTypes
	 *            Names of columns in table
	 */
	CsvResultSetMetaData(String tableName, String[] columnNames,
			String[] columnLabels, String[] columnTypes,
			int[] columnDisplaySizes)
	{
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.columnLabels = columnLabels;
		this.columnTypes = columnTypes;
		this.columnDisplaySizes = columnDisplaySizes;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException
	{
		return columnTypes[column - 1];
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return columnTypes.length;
	}

	@Override
	public String getCatalogName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException
	{
		return columnDisplaySizes[column - 1];
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException
	{
		// all columns are uppercase
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException
	{
		// the implementation doesn't support the where clause
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException
	{
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException
	{
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException
	{
		return false;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException
	{
		// SQL column numbers start at 1
		return columnLabels[column - 1];
	}

	@Override
	public String getColumnName(int column) throws SQLException
	{
		// SQL column numbers start at 1
		return columnNames[column - 1];
	}

	@Override
	public String getSchemaName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException
	{
		// All the fields are text, should this throw an SQLException?
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException
	{
		// All the fields are text, should this throw an SQLException?
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException
	{
		return tableName;
	}

	private Map<String, Integer> typeNameToTypeCode = new HashMap<String, Integer>()
	{
		private static final long serialVersionUID = -8819579540085202365L;

		{
			put("String", new Integer(Types.VARCHAR));
			put("Boolean", new Integer(Types.BOOLEAN));
			put("Byte", new Integer(Types.TINYINT));
			put("Short", new Integer(Types.SMALLINT));
			put("Int", new Integer(Types.INTEGER));
			put("Integer", new Integer(Types.INTEGER));
			put("Long", new Integer(Types.BIGINT));
			put("Float", new Integer(Types.FLOAT));
			put("Double", new Integer(Types.DOUBLE));
			put("BigDecimal", new Integer(Types.DECIMAL));
			put("Date", new Integer(Types.DATE));
			put("Time", new Integer(Types.TIME));
			put("Timestamp", new Integer(Types.TIMESTAMP));
			put("Blob", new Integer(Types.BLOB));
			put("Clob", new Integer(Types.CLOB));
			put("expression", new Integer(Types.BLOB));
		}
	};

	@Override
	public int getColumnType(int column) throws SQLException
	{
		String columnTypeName = getColumnTypeName(column);
		Integer value = typeNameToTypeCode.get(columnTypeName);
		return value.intValue();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException
	{
		return columnTypes[column - 1];
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
