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
import java.util.Map;
import java.util.Hashtable;

/**This class implements the Connection interface for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @version $Id: CsvConnection.java,v 1.1 2001/01/23 09:17:48 jackerm Exp $
 */
public class CsvConnection implements Connection
{
  private String filePath=null;

  protected CsvConnection(String filePath)
  {
    DriverManager.println("CsvJdbc - CsvConnection() - filePath=" + filePath);
    this.filePath = filePath;
  }

  protected String getFilePath()
  {
    return filePath;
  }

  public Statement createStatement() throws SQLException
  {
    return new CsvStatement(this);
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public CallableStatement prepareCall(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public String nativeSQL(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException
  {
    // Don't do anything with commits & rollbacks
  }

  public boolean getAutoCommit() throws SQLException
  {
    // always false
    return false;
  }

  public void commit() throws SQLException
  {
    // Don't do anything with commits & rollbacks
  }

  public void rollback() throws SQLException
  {
    // Don't do anything with commits & rollbacks
  }

  public void close() throws SQLException
  {
    // ignore close
  }

  public boolean isClosed() throws SQLException
  {
    return false;
  }

  public DatabaseMetaData getMetaData() throws SQLException
  {
    throw new SQLException("NYI");
  }

  public void setReadOnly(boolean readOnly) throws SQLException
  {
    // ignore this call
  }

  public boolean isReadOnly() throws SQLException
  {
    // always readonly
    return true;
  }

  public void setCatalog(String catalog) throws SQLException
  {
    // ignore this call
  }

  public String getCatalog() throws SQLException
  {
    return null;
  }

  public void setTransactionIsolation(int level) throws SQLException
  {
    // ignore this call
  }

  public int getTransactionIsolation() throws SQLException
  {
    return Connection.TRANSACTION_NONE;
  }

  public SQLWarning getWarnings() throws SQLException
  {
    return null;
  }

  public void clearWarnings() throws SQLException
  {
    // ignore this call
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }

  public Map getTypeMap() throws SQLException
  {
    return new Hashtable();
  }

  public void setTypeMap(Map map) throws SQLException
  {
    // ignore this call
  }
}