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

import java.sql.*;
import java.math.BigDecimal;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Calendar;

/**
 *This class implements the ResultSet interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @created    25 November 2001
 * @version    $Id: CsvResultSet.java,v 1.3 2001/12/02 00:25:05 jackerm Exp $
 */
public class CsvResultSet implements ResultSet
{
  protected CsvStatement statement;
  protected CsvReader reader;
  protected String[] columnNames;
  protected String tableName;
  protected ResultSetMetaData resultSetMetaData;


  /**
   *Constructor for the CsvResultSet object
   *
   * @param  statement    Description of Parameter
   * @param  reader       Description of Parameter
   * @param  columnNames  Description of Parameter
   * @since
   */
  protected CsvResultSet(CsvStatement statement, CsvReader reader, String tableName,String[] columnNames)
  {
    this.statement = statement;
    this.reader = reader;
    this.tableName = tableName;
    this.columnNames = columnNames;

    if (columnNames[0].equals("*"))
    {
      this.columnNames = reader.getColumnNames();
    }
  }


  /**
   *Sets the fetchDirection attribute of the CsvResultSet object
   *
   * @param  p0                The new fetchDirection value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setFetchDirection(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Sets the fetchSize attribute of the CsvResultSet object
   *
   * @param  p0                The new fetchSize value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setFetchSize(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the string attribute of the CsvResultSet object
   *
   * @param  columnIndex       Description of Parameter
   * @return                   The string value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public String getString(int columnIndex) throws SQLException
  {
    try
    {
      return reader.getColumn(columnNames[columnIndex]);
    }
    catch (Exception e)
    {
      throw new SQLException(e.getMessage());
    }
  }


  /**
   *Gets the string attribute of the CsvResultSet object
   *
   * @param  columnName        Description of Parameter
   * @return                   The string value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public String getString(String columnName) throws SQLException
  {
    columnName = columnName.toUpperCase();

    for (int loop = 0; loop < columnNames.length; loop++)
    {
      if (columnName.equals(columnNames[loop]))
      {
        return getString(loop);
      }
    }

    throw new SQLException("Column '" + columnName + "' not found.");
  }


  /**
   *Gets the statement attribute of the CsvResultSet object
   *
   * @return                   The statement value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Statement getStatement() throws SQLException
  {
    return statement;
  }


  /**
   *Gets the boolean attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The boolean value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean getBoolean(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the byte attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The byte value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public byte getByte(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the short attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The short value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public short getShort(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the int attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The int value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getInt(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the long attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The long value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public long getLong(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the float attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The float value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public float getFloat(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the double attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The double value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public double getDouble(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the bigDecimal attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The bigDecimal value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public BigDecimal getBigDecimal(int p0, int p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the bytes attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The bytes value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public byte[] getBytes(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the date attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The date value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Date getDate(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the time attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The time value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Time getTime(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the timestamp attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The timestamp value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Timestamp getTimestamp(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the asciiStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The asciiStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public InputStream getAsciiStream(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the unicodeStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The unicodeStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public InputStream getUnicodeStream(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the binaryStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The binaryStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public InputStream getBinaryStream(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the boolean attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The boolean value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean getBoolean(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the byte attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The byte value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public byte getByte(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the short attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The short value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public short getShort(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the int attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The int value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getInt(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the long attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The long value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public long getLong(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the float attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The float value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public float getFloat(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the double attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The double value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public double getDouble(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the bigDecimal attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The bigDecimal value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public BigDecimal getBigDecimal(String p0, int p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the bytes attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The bytes value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public byte[] getBytes(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the date attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The date value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Date getDate(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the time attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The time value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Time getTime(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the timestamp attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The timestamp value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Timestamp getTimestamp(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the asciiStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The asciiStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public InputStream getAsciiStream(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the unicodeStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The unicodeStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public InputStream getUnicodeStream(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the binaryStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The binaryStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public InputStream getBinaryStream(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the warnings attribute of the CsvResultSet object
   *
   * @return                   The warnings value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public SQLWarning getWarnings() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the cursorName attribute of the CsvResultSet object
   *
   * @return                   The cursorName value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public String getCursorName() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the metaData attribute of the CsvResultSet object
   *
   * @return                   The metaData value
   * @exception  SQLException  Description of Exception
   * @since
   */
   public ResultSetMetaData getMetaData() throws SQLException
   {
     if (resultSetMetaData == null)
     {
       resultSetMetaData = new CsvResultSetMetaData(tableName,columnNames);
     }

     return resultSetMetaData;
  }


  /**
   *Gets the object attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The object value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Object getObject(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the object attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The object value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Object getObject(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the characterStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The characterStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Reader getCharacterStream(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the characterStream attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The characterStream value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Reader getCharacterStream(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the bigDecimal attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The bigDecimal value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public BigDecimal getBigDecimal(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the bigDecimal attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The bigDecimal value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public BigDecimal getBigDecimal(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the beforeFirst attribute of the CsvResultSet object
   *
   * @return                   The beforeFirst value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean isBeforeFirst() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the afterLast attribute of the CsvResultSet object
   *
   * @return                   The afterLast value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean isAfterLast() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the first attribute of the CsvResultSet object
   *
   * @return                   The first value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean isFirst() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the last attribute of the CsvResultSet object
   *
   * @return                   The last value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean isLast() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the row attribute of the CsvResultSet object
   *
   * @return                   The row value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the fetchDirection attribute of the CsvResultSet object
   *
   * @return                   The fetchDirection value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getFetchDirection() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the fetchSize attribute of the CsvResultSet object
   *
   * @return                   The fetchSize value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getFetchSize() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the type attribute of the CsvResultSet object
   *
   * @return                   The type value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getType() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the concurrency attribute of the CsvResultSet object
   *
   * @return                   The concurrency value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getConcurrency() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the object attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The object value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Object getObject(int p0, Map p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the ref attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The ref value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Ref getRef(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the blob attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The blob value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Blob getBlob(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the clob attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The clob value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Clob getClob(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the array attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The array value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Array getArray(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the object attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The object value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Object getObject(String p0, Map p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the ref attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The ref value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Ref getRef(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the blob attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The blob value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Blob getBlob(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the clob attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The clob value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Clob getClob(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the array attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @return                   The array value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Array getArray(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the date attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The date value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Date getDate(int p0, Calendar p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the date attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The date value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Date getDate(String p0, Calendar p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the time attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The time value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Time getTime(int p0, Calendar p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the time attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The time value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Time getTime(String p0, Calendar p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the timestamp attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The timestamp value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Timestamp getTimestamp(int p0, Calendar p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the timestamp attribute of the CsvResultSet object
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @return                   The timestamp value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Timestamp getTimestamp(String p0, Calendar p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean next() throws SQLException
  {
    try
    {
      return reader.next();
    }
    catch (Exception e)
    {
      throw new SQLException("Error reading data. Message was: " + e);
    }
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void close() throws SQLException
  {
    reader.close();
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean wasNull() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void clearWarnings() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int findColumn(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void beforeFirst() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void afterLast() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean first() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean last() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean absolute(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean relative(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean previous() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean rowUpdated() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean rowInserted() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean rowDeleted() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateNull(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBoolean(int p0, boolean p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateByte(int p0, byte p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateShort(int p0, short p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateInt(int p0, int p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateLong(int p0, long p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateFloat(int p0, float p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateDouble(int p0, double p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBigDecimal(int p0, BigDecimal p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateString(int p0, String p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBytes(int p0, byte[] p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateDate(int p0, Date p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateTime(int p0, Time p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateTimestamp(int p0, Timestamp p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateAsciiStream(int p0, InputStream p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBinaryStream(int p0, InputStream p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateCharacterStream(int p0, Reader p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateObject(int p0, Object p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateObject(int p0, Object p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateNull(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBoolean(String p0, boolean p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateByte(String p0, byte p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateShort(String p0, short p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateInt(String p0, int p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateLong(String p0, long p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateFloat(String p0, float p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateDouble(String p0, double p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBigDecimal(String p0, BigDecimal p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateString(String p0, String p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBytes(String p0, byte[] p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateDate(String p0, Date p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateTime(String p0, Time p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateTimestamp(String p0, Timestamp p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateAsciiStream(String p0, InputStream p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateBinaryStream(String p0, InputStream p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateCharacterStream(String p0, Reader p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @param  p2                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateObject(String p0, Object p1, int p2) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  p0                Description of Parameter
   * @param  p1                Description of Parameter
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateObject(String p0, Object p1) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void insertRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void updateRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void deleteRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void refreshRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void cancelRowUpdates() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void moveToInsertRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void moveToCurrentRow() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }
}

