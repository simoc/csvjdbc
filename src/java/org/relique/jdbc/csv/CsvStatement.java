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
import java.io.File;

/**This class implements the Statement interface for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @version $Id: CsvStatement.java,v 1.1 2001/01/23 09:17:48 jackerm Exp $
 */
public class CsvStatement implements Statement
{
  private CsvConnection connection;

  protected CsvStatement(CsvConnection connection)
  {
    DriverManager.println("CsvJdbc - CsvStatement() - connection=" + connection);
    this.connection = connection;
  }

  public ResultSet executeQuery(String sql) throws SQLException
  {
    DriverManager.println("CsvJdbc - CsvStatement:executeQuery() - sql= " +sql);

    SqlParser parser = new SqlParser();
    try
    {
      parser.parse(sql);
    }
    catch(Exception e)
    {
      throw new SQLException("Syntax Error. " + e.getMessage());
    }

    String fileName = connection.getFilePath() + parser.getTableName() + ".csv";
    File checkFile = new File(fileName);

    if (!checkFile.exists())
      throw new SQLException("Cannot open data file '" + fileName + "'  !");

    if (!checkFile.canRead())
      throw new SQLException("Data file '" + fileName + "'  not readable !");


    CsvReader reader;

    try
    {
      reader = new CsvReader(fileName);
    }
    catch(Exception e)
    {
      throw new SQLException("Error reading data file. Message was: " + e);
    }

    return new CsvResultSet(this,reader,parser.getColumnNames());
  }

  public int executeUpdate(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void close() throws SQLException
  {
    // ignore call
  }

  public int getMaxFieldSize() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setMaxFieldSize(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int getMaxRows() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setMaxRows(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setEscapeProcessing(boolean p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int getQueryTimeout() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setQueryTimeout(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void cancel() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public SQLWarning getWarnings() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void clearWarnings() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setCursorName(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public boolean execute(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public ResultSet getResultSet() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int getUpdateCount() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public boolean getMoreResults() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setFetchDirection(int p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int getFetchDirection() throws SQLException

  {
    throw new SQLException("Not Supported !");
  }

  public void setFetchSize(int p0) throws SQLException

  {
    throw new SQLException("Not Supported !");
  }

  public int getFetchSize() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int getResultSetConcurrency() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int getResultSetType() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void addBatch(String p0) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void clearBatch() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public int[] executeBatch() throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public Connection getConnection() throws SQLException
  {
    return connection;
  }


}