/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2001  Jonathan Ackerman

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package org.relique.jdbc.csv;

import java.sql.*;
import java.math.BigDecimal;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Calendar;

/**This class implements the ResultSet interface for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @version $Id: CsvResultSet.java,v 1.1 2001/01/23 09:17:48 jackerm Exp $
 */
public class CsvResultSet implements ResultSet
{
  protected CsvStatement statement;
  protected CsvReader reader;
  protected String[] columnNames;

  protected CsvResultSet(CsvStatement statement,CsvReader reader,String[] columnNames)
  {
    this.statement = statement;
    this.reader = reader;
    this.columnNames = columnNames;

    if (columnNames[0].equals("*"))
      this.columnNames = reader.getColumnNames();
  }

  public boolean next() throws SQLException
  {
    try
    {
      return reader.next();
    }
    catch(Exception e)
    {
      throw new SQLException("Error reading data. Message was: " +e);
    }
  }

  public void close() throws SQLException
  {
    reader.close();
  }

  public String getString(int columnIndex) throws SQLException
  {
    try
    {
      return reader.getColumn(columnNames[columnIndex]);
    }
    catch(Exception e)
    {
      throw new SQLException(e.getMessage());
    }
  }

  public String getString(String columnName) throws SQLException
  {
    columnName = columnName.toUpperCase();

    for (int loop=0; loop < columnNames.length; loop++)
     if (columnName.equals(columnNames[loop]))
       return getString(loop);

    throw new SQLException("Column '" +columnName + "' not found.");
  }

  public Statement getStatement() throws SQLException
  {
    return statement;
  }

  public boolean wasNull() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean getBoolean(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public byte getByte(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public short getShort(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public int getInt(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public long getLong(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public float getFloat(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public double getDouble(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public BigDecimal getBigDecimal(int p0, int p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public byte[] getBytes(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Date getDate(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Time getTime(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Timestamp getTimestamp(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public InputStream getAsciiStream(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public InputStream getUnicodeStream(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public InputStream getBinaryStream(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean getBoolean(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public byte getByte(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public short getShort(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public int getInt(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public long getLong(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public float getFloat(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public double getDouble(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public BigDecimal getBigDecimal(String p0, int p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public byte[] getBytes(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Date getDate(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Time getTime(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Timestamp getTimestamp(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public InputStream getAsciiStream(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public InputStream getUnicodeStream(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public InputStream getBinaryStream(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public SQLWarning getWarnings() throws SQLException { throw new SQLException("Not Supported !"); }
  public void clearWarnings() throws SQLException { throw new SQLException("Not Supported !"); }
  public String getCursorName() throws SQLException { throw new SQLException("Not Supported !"); }
  public ResultSetMetaData getMetaData() throws SQLException { throw new SQLException("Not Supported !"); }
  public Object getObject(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Object getObject(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public int findColumn(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Reader getCharacterStream(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Reader getCharacterStream(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public BigDecimal getBigDecimal(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public BigDecimal getBigDecimal(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean isBeforeFirst() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean isAfterLast() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean isFirst() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean isLast() throws SQLException { throw new SQLException("Not Supported !"); }
  public void beforeFirst() throws SQLException { throw new SQLException("Not Supported !"); }
  public void afterLast() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean first() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean last() throws SQLException { throw new SQLException("Not Supported !"); }
  public int getRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean absolute(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean relative(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean previous() throws SQLException { throw new SQLException("Not Supported !"); }
  public void setFetchDirection(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public int getFetchDirection() throws SQLException { throw new SQLException("Not Supported !"); }
  public void setFetchSize(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public int getFetchSize() throws SQLException { throw new SQLException("Not Supported !"); }
  public int getType() throws SQLException { throw new SQLException("Not Supported !"); }
  public int getConcurrency() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean rowUpdated() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean rowInserted() throws SQLException { throw new SQLException("Not Supported !"); }
  public boolean rowDeleted() throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateNull(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBoolean(int p0, boolean p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateByte(int p0, byte p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateShort(int p0, short p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateInt(int p0, int p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateLong(int p0, long p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateFloat(int p0, float p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateDouble(int p0, double p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBigDecimal(int p0, BigDecimal p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateString(int p0, String p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBytes(int p0, byte[] p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateDate(int p0, Date p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateTime(int p0, Time p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateTimestamp(int p0, Timestamp p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateAsciiStream(int p0, InputStream p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBinaryStream(int p0, InputStream p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateCharacterStream(int p0, Reader p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateObject(int p0, Object p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateObject(int p0, Object p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateNull(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBoolean(String p0, boolean p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateByte(String p0, byte p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateShort(String p0, short p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateInt(String p0, int p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateLong(String p0, long p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateFloat(String p0, float p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateDouble(String p0, double p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBigDecimal(String p0, BigDecimal p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateString(String p0, String p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBytes(String p0, byte[] p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateDate(String p0, Date p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateTime(String p0, Time p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateTimestamp(String p0, Timestamp p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateAsciiStream(String p0, InputStream p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateBinaryStream(String p0, InputStream p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateCharacterStream(String p0, Reader p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateObject(String p0, Object p1, int p2) throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateObject(String p0, Object p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public void insertRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public void updateRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public void deleteRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public void refreshRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public void cancelRowUpdates() throws SQLException { throw new SQLException("Not Supported !"); }
  public void moveToInsertRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public void moveToCurrentRow() throws SQLException { throw new SQLException("Not Supported !"); }
  public Object getObject(int p0, Map p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Ref getRef(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Blob getBlob(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Clob getClob(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Array getArray(int p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Object getObject(String p0, Map p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Ref getRef(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Blob getBlob(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Clob getClob(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Array getArray(String p0) throws SQLException { throw new SQLException("Not Supported !"); }
  public Date getDate(int p0, Calendar p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Date getDate(String p0, Calendar p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Time getTime(int p0, Calendar p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Time getTime(String p0, Calendar p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Timestamp getTimestamp(int p0, Calendar p1) throws SQLException { throw new SQLException("Not Supported !"); }
  public Timestamp getTimestamp(String p0, Calendar p1) throws SQLException { throw new SQLException("Not Supported !"); }
}