/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
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

/**
 *This class implements the ResultSetMetaData interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @author     JD Evora
 * @version    $Id: CsvResultSetMetaData.java,v 1.3 2001/12/02 00:25:05 jackerm Exp $
 */
public class CsvResultSetMetaData implements ResultSetMetaData
{
  /** Default value for getColumnDisplaySize */
  final static int DISPLAY_SIZE = 20;
  /** Names of columns */
  protected String[] columnNames;
  /** Name of table */
  protected String tableName;

  /**Constructor for the CsvResultSetMetaData object
   *
   * @param  tableName    Name of table
   * @param  columnNames  Names of columns in table
   */
  CsvResultSetMetaData(String tableName, String[] columnNames)
  {
    this.tableName = tableName;
    this.columnNames = columnNames;
  }


  /**Returns the name of the class for the specified column. Always returns
   * String.
   *
   * @param  column            The column number
   * @return                   The name of the class for the requested column
   * @exception  SQLException  Thrown if there was a problem
   */
  public String getColumnClassName(int column) throws SQLException
  {
    return String.class.getName();
  }


  /** Returns the number of columns in the table.
   *
   * @return                   The number of columns in the table
   * @exception  SQLException  Thrown if there is a a problem
   */
  public int getColumnCount() throws SQLException
  {
    return columnNames.length;
  }


  /** Returns the name of the catalog for the specified column. Returns "".
   *
   * @param  column            The column to get the catalog for
   * @return                   The catalog name (always "")
   * @exception  SQLException  Thrown if there is a problem
   */
  public String getCatalogName(int column) throws SQLException
  {
    return "";
  }


  /**Returns the display column size for the specified column. Always returns 20.
   *
   * @param  column            The column to get the size of
   * @return                   The size of the requested column
   * @exception  SQLException  Thrown if there is a problem.
   */
  public int getColumnDisplaySize(int column) throws SQLException
  {
    return DISPLAY_SIZE;
  }


  /**Gets the auto increment falg for the specfied column.
   *
   * @param  column            The column to get the flag for
   * @return                   The autoIncrement flag (always false)
   * @exception  SQLException  Thrown if there is a problem
   */
  public boolean isAutoIncrement(int column) throws SQLException
  {
    return false;
  }


  /**Returns the case sensitivity flag for the specfied column
   *
   * @param  column            The column to return the flag for
   * @return                   The caseSensitive flag (always false)
   * @exception  SQLException  Thrown if there is a problem
   */
  public boolean isCaseSensitive(int column) throws SQLException
  {
    //all columns are uppercase
    return false;
  }


  /** Returns the searchable flag for the specified column
   *
   * @param  column            the column to return the flag form
   * @return                   The searchable flag (always false)
   * @exception  SQLException  Thrown if there is a problem
   */
  public boolean isSearchable(int column) throws SQLException
  {
    // the implementation doesn't support the where clause
    return false;
  }


  /**Returns the currency flag for the specified column
   *
   * @param  column            The column to get the flag for
   * @return                   The currency flag (always false)
   * @exception  SQLException  Thrown if there is a problem
   */
  public boolean isCurrency(int column) throws SQLException
  {
    return false;
  }


  /** Returns the nullable flag for the specfied column
   *
   * @param  column            The column to return the flag for
   * @return                   The nullable flag (always unknown)
   * @exception  SQLException  Thrown if there is a problem
   */
  public int isNullable(int column) throws SQLException
  {
    return ResultSetMetaData.columnNullableUnknown;
  }


  /**Returns the signed flag for the specfied column
   *
   * @param  column            The column to return the flag for
   * @return                   The signed flag (always false)
   * @exception  SQLException  Thrown if there is a problem
   */
  public boolean isSigned(int column) throws SQLException
  {
    return false;
  }


  /** Returns the label for the specified column
   *
   * @param  column            The column to get the label for
   * @return                   the label for the specified column
   * @exception  SQLException  Thrown if there is a problem
   */
  public String getColumnLabel(int column) throws SQLException
  {
    return columnNames[column];
  }


  /**Returns the name of the specified column
   *
   * @param  column            The column to get the name of
   * @return                   The name of the column
   * @exception  SQLException  Thrown if there is a problem
   */
  public String getColumnName(int column) throws SQLException
  {
    return columnNames[column];
  }


  /**Comments to be done
   */
  public String getSchemaName(int column) throws SQLException
  {
    return "";
  }


  /**Comments to be done
   */
  public int getPrecision(int column) throws SQLException
  {
    // All the fields are text, should this throw an SQLException?
    return 0;
  }


  /**Comments to be done
   */
  public int getScale(int column) throws SQLException
  {
    // All the fields are text, should this throw an SQLException?
    return 0;
  }


  /**Comments to be done
   */
  public String getTableName(int column) throws SQLException
  {
    return tableName;
  }


  /**Comments to be done
   */
  public int getColumnType(int column) throws SQLException
  {
    return Types.VARCHAR;
  }


  /**Comments to be done
   */
  public String getColumnTypeName(int column) throws SQLException
  {
    return String.class.getName();
    // ??
  }


  /**Comments to be done
   */
  public boolean isReadOnly(int column) throws SQLException
  {
    return true;
  }


  /**Comments to be done
   */
  public boolean isWritable(int column) throws SQLException
  {
    return false;
  }


  /**Comments to be done
   */
  public boolean isDefinitelyWritable(int column) throws SQLException
  {
    return false;
  }

}

