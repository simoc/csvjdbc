/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *
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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * This class an abstract class that contains the common functionality 
 * of the Scrollable and Non Scrollable Reader
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @author     Stuart Mottram (fritto)
 * @author     Jason Bedell
 * @author     Tomasz Skutnik
 * @author     Chetan Gupta
 * @created    01 March 2004
 * @version    $Id: CSVReaderAdapter.java,v 1.1 2004/04/09 11:17:13 gupta_chetan Exp $
 */

public abstract class CSVReaderAdapter
{
  protected String[] columnNames;
  protected String[] columns;
  protected java.lang.String buf = null;
  protected char separator = ',';
  protected boolean suppressHeaders = false;
  protected String tableName;
  protected String fileName;
  protected String charset = null;

  /**
   *Gets the columnNames attribute of the CsvReader object
   *
   * @return    The columnNames value
   * @since
   */
  public String[] getColumnNames()
  {
    return columnNames;
  }


  public String getTableName() {
      if(tableName != null)
          return tableName;

      int lastSlash = 0;
      for(int i = fileName.length()-1; i >= 0; i--)
          if(fileName.charAt(i) == '/' || fileName.charAt(i) == '\\') {
            lastSlash = i;
            break;
          }
      tableName = fileName.substring(lastSlash+1, fileName.length() - 4);
      return tableName;
  }

  /**
   * Get the value of the column at the specified index.
   *
   * @param  columnIndex  Description of Parameter
   * @return              The column value
   * @since
   */

  public String getColumn(int columnIndex) throws SQLException
  {
      if (columnIndex >= columns.length)
      {
          return null;
      }
      return columns[columnIndex];
  }

  /**
   * Get value from column at specified name.
   * If the column name is not found, throw an error.
   *
   * @param  columnName     Description of Parameter
   * @return                The column value
   * @exception  SQLException  Description of Exception
   * @since
   */

  public String getColumn(String columnName) throws SQLException
  {
    for (int loop = 0; loop < columnNames.length; loop++)
    {
      if (columnName.equalsIgnoreCase(columnNames[loop]) || columnName.equalsIgnoreCase(getTableName() + "." + columnNames[loop]))
      {
        return getColumn(loop);
      }
    }
    throw new SQLException("Column '" + columnName + "' not found.");
  }

  /***************************************************************************/
  
  public abstract boolean next() throws SQLException;

  public abstract void close();
  
  protected abstract String[] parseCsvLine(String line) throws SQLException;

    //---------------------------------------------------------------------
    // Traversal/Positioning
    //---------------------------------------------------------------------

    /**
     * Retrieves whether the cursor is before the first row in
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is before the first row;
     * <code>false</code> if the cursor is at any other position or the
     * result set contains no rows
     * @exception SQLException if a database access error occurs
     */
    public boolean isBeforeFirst() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.isBeforeFirst() unsupported");
    }

    /**
     * Retrieves whether the cursor is after the last row in
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is after the last row;
     * <code>false</code> if the cursor is at any other position or the
     * result set contains no rows
     * @exception SQLException if a database access error occurs
     */
    public boolean isAfterLast() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.isAfterLast() unsupported");
    }

    /**
     * Retrieves whether the cursor is on the first row of
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on the first row;
     * <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isFirst() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.isFirst() unsupported");
    }

    /**
     * Retrieves whether the cursor is on the last row of
     * this <code>ResultSet</code> object.
     * Note: Calling the method <code>isLast</code> may be expensive
     * because the JDBC driver
     * might need to fetch ahead one row in order to determine
     * whether the current row is the last row in the result set.
     *
     * @return <code>true</code> if the cursor is on the last row;
     * <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isLast() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.isLast() unsupported");
    }

    /**
     * Moves the cursor to the front of
     * this <code>ResultSet</code> object, just before the
     * first row. This method has no effect if the result set contains no rows.
     *
     * @exception SQLException if a database access error
     * occurs or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public void beforeFirst() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.beforeFirst() unsupported");
    }

    /**
     * Moves the cursor to the end of
     * this <code>ResultSet</code> object, just after the
     * last row. This method has no effect if the result set contains no rows.
     * @exception SQLException if a database access error
     * occurs or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public void afterLast() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.afterLast() unsupported");
    }

    /**
     * Moves the cursor to the first row in
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     * <code>false</code> if there are no rows in the result set
     * @exception SQLException if a database access error
     * occurs or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean first() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.first() unsupported");
    }

    /**
     * Moves the cursor to the last row in
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     * <code>false</code> if there are no rows in the result set
     * @exception SQLException if a database access error
     * occurs or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean last() throws SQLException {
          throw new UnsupportedOperationException("ResultSet.last() unsupported");
    }

    /**
     * Retrieves the current row number.  The first row is number 1, the
     * second number 2, and so on.
     *
     * @return the current row number; <code>0</code> if there is no current row
     * @exception SQLException if a database access error occurs
     */
    public int getRow() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.getRow() unsupported");
    }

    /**
     * Moves the cursor to the given row number in
     * this <code>ResultSet</code> object.
     *
     * <p>If the row number is positive, the cursor moves to
     * the given row number with respect to the
     * beginning of the result set.  The first row is row 1, the second
     * is row 2, and so on.
     *
     * <p>If the given row number is negative, the cursor moves to
     * an absolute row position with respect to
     * the end of the result set.  For example, calling the method
     * <code>absolute(-1)</code> positions the
     * cursor on the last row; calling the method <code>absolute(-2)</code>
     * moves the cursor to the next-to-last row, and so on.
     *
     * <p>An attempt to position the cursor beyond the first/last row in
     * the result set leaves the cursor before the first row or after
     * the last row.
     *
     * <p><B>Note:</B> Calling <code>absolute(1)</code> is the same
     * as calling <code>first()</code>. Calling <code>absolute(-1)</code>
     * is the same as calling <code>last()</code>.
     *
     * @param row the number of the row to which the cursor should move.
     *        A positive number indicates the row number counting from the
     *        beginning of the result set; a negative number indicates the
     *        row number counting from the end of the result set
     * @return <code>true</code> if the cursor is on the result set;
     * <code>false</code> otherwise
     * @exception SQLException if a database access error
     * occurs, or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean absolute(int row) throws SQLException {
	        throw new UnsupportedOperationException(
	                "ResultSet.absolute() unsupported");
    }

    /**
     * Moves the cursor a relative number of rows, either positive or negative.
     * Attempting to move beyond the first/last row in the
     * result set positions the cursor before/after the
     * the first/last row. Calling <code>relative(0)</code> is valid, but does
     * not change the cursor position.
     *
     * <p>Note: Calling the method <code>relative(1)</code>
     * is identical to calling the method <code>next()</code> and
     * calling the method <code>relative(-1)</code> is identical
     * to calling the method <code>previous()</code>.
     *
     * @param rows an <code>int</code> specifying the number of rows to
     *        move from the current row; a positive number moves the cursor
     *        forward; a negative number moves the cursor backward
     * @return <code>true</code> if the cursor is on a row;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs,
     *            there is no current row, or the result set type is
     *            <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean relative(int rows) throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.relative() unsupported");
    }

    /**
     * Moves the cursor to the previous row in this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     * <code>false</code> if it is off the result set
     * @exception SQLException if a database access error
     * occurs or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean previous() throws SQLException {
          throw new UnsupportedOperationException(
                "ResultSet.previous() unsupported");
    }

}

