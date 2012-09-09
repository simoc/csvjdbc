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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 *This class implements the ResultSetMetaData interface for the CsvJdbc driver.
 * 
 * @author Jonathan Ackerman
 * @author JD Evora
 * @version $Id: CsvResultSetMetaData.java,v 1.10 2011/10/17 13:47:40 simoc Exp $
 */
public class CsvResultSetMetaData implements ResultSetMetaData {

	/** Names of columns */
	private String []columnNames;
	private String []columnLabels;
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
	CsvResultSetMetaData(String tableName, String []columnNames, String []columnLabels,
			String[] columnTypes, int []columnDisplaySizes) {
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.columnLabels = columnLabels;
		this.columnTypes = columnTypes;
		this.columnDisplaySizes = columnDisplaySizes;
	}

	/**
	 * Returns the name of the class for the specified column. Always returns
	 * String.
	 * 
	 * @param column
	 *            The column number
	 * @return The name of the class for the requested column
	 * @exception SQLException
	 *                Thrown if there was a problem
	 */
	public String getColumnClassName(int column) throws SQLException {
		return columnTypes[column - 1];
	}

	/**
	 * Returns the number of columns in the table.
	 * 
	 * @return The number of columns in the table
	 * @exception SQLException
	 *                Thrown if there is a a problem
	 */
	public int getColumnCount() throws SQLException {
		return columnTypes.length;
	}

	/**
	 * Returns the name of the catalog for the specified column. Returns "".
	 * 
	 * @param column
	 *            The column to get the catalog for
	 * @return The catalog name (always "")
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public String getCatalogName(int column) throws SQLException {
		return "";
	}

	/**
	 * Returns the display column size for the specified column. Always returns
	 * 20.
	 * 
	 * @param column
	 *            The column to get the size of
	 * @return The size of the requested column
	 * @exception SQLException
	 *                Thrown if there is a problem.
	 */
	public int getColumnDisplaySize(int column) throws SQLException {
		return columnDisplaySizes[column - 1];
	}

	/**
	 * Gets the auto increment flag for the specified column.
	 * 
	 * @param column
	 *            The column to get the flag for
	 * @return The autoIncrement flag (always false)
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	/**
	 * Returns the case sensitivity flag for the specified column
	 * 
	 * @param column
	 *            The column to return the flag for
	 * @return The caseSensitive flag (always false)
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public boolean isCaseSensitive(int column) throws SQLException {
		// all columns are uppercase
		return false;
	}

	/**
	 * Returns the searchable flag for the specified column
	 * 
	 * @param column
	 *            the column to return the flag form
	 * @return The searchable flag (always false)
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public boolean isSearchable(int column) throws SQLException {
		// the implementation doesn't support the where clause
		return false;
	}

	/**
	 * Returns the currency flag for the specified column
	 * 
	 * @param column
	 *            The column to get the flag for
	 * @return The currency flag (always false)
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	/**
	 * Returns the nullable flag for the specified column
	 * 
	 * @param column
	 *            The column to return the flag for
	 * @return The nullable flag (always unknown)
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public int isNullable(int column) throws SQLException {
		return ResultSetMetaData.columnNullableUnknown;
	}

	/**
	 * Returns the signed flag for the specified column
	 * 
	 * @param column
	 *            The column to return the flag for
	 * @return The signed flag (always false)
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public boolean isSigned(int column) throws SQLException {
		return false;
	}

	/**
	 * Returns a comment regarding the specified column
	 * 
	 * @param column
	 *            The column to get the label for
	 * @return the label for the specified column
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public String getColumnLabel(int column) throws SQLException {
		// SQL column numbers start at 1
		return columnLabels[column - 1];
	}

	/**
	 * Returns the name of the specified column
	 * 
	 * @param column
	 *            The column to get the name of
	 * @return The name of the column
	 * @exception SQLException
	 *                Thrown if there is a problem
	 */
	public String getColumnName(int column) throws SQLException {
		// SQL column numbers start at 1
		return columnNames[column - 1];
	}

	/**
	 * Comments to be done
	 */
	public String getSchemaName(int column) throws SQLException {
		return "";
	}

	/**
	 * Comments to be done
	 */
	public int getPrecision(int column) throws SQLException {
		// All the fields are text, should this throw an SQLException?
		return 0;
	}

	/**
	 * Comments to be done
	 */
	public int getScale(int column) throws SQLException {
		// All the fields are text, should this throw an SQLException?
		return 0;
	}

	/**
	 * Comments to be done
	 */
	public String getTableName(int column) throws SQLException {
		return tableName;
	}

	private Map typeNameToTypeCode = new HashMap() {
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

	/**
	 * Comments to be done
	 */
	public int getColumnType(int column) throws SQLException {
		String columnTypeName = getColumnTypeName(column);
		Integer value = (Integer) typeNameToTypeCode
				.get(columnTypeName);
		return value.intValue();
	}

	/**
	 * Comments to be done
	 */
	public String getColumnTypeName(int column) throws SQLException {
		return columnTypes[column - 1];
	}

	/**
	 * Comments to be done
	 */
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	/**
	 * Comments to be done
	 */
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	/**
	 * Comments to be done
	 */
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	public boolean isWrapperFor(Class arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public Object unwrap(Class arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}
