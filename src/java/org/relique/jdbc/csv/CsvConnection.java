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

import java.sql.*;
import java.util.Map;
import java.util.Hashtable;

/**
 * This class implements the Connection interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @created    25 November 2001
 * @version    $Id: CsvConnection.java,v 1.2 2001/12/01 22:35:13 jackerm Exp $
 */

public class CsvConnection implements Connection
{
  private String filePath = null;
  private java.lang.String extension = ".csv";
  private char seperator;
  private boolean suppressHeaders;


  /**
   *Constructor for the CsvConnection object
   *
   * @param  filePath  Description of Parameter
   * @since
   */
  protected CsvConnection(String filePath)
  {
    DriverManager.println("CsvJdbc - CsvConnection() - filePath=" + filePath);
    this.filePath = filePath;
  }


  /**
   *Constructor for the CsvConnection object
   *
   * @param  filePath  Description of Parameter
   * @param  info      Description of Parameter
   * @since
   */
  protected CsvConnection(String filePath, java.util.Properties info)
  {
    DriverManager.println("CsvJdbc - CsvConnection() - filePath=" + filePath);
    this.filePath = filePath;

    // check for properties
    char sep = ',';
    boolean suph = false;
    String ext = ".csv";
    if (info.containsKey("seperator"))
    {
      sep = ((Character)info.get("seperator")).charValue();
    }
    if (info.containsKey("suppressHeaders"))
    {
      suph = ((Boolean)info.get("suppressHeaders")).booleanValue();
    }
    if (info.containsKey("extension"))
    {
      ext = (String)info.get("extension");
    }

    DriverManager.println("CsvJdbc - CsvConnection() - seperator=" + String.valueOf(seperator));
    this.seperator = sep;
    DriverManager.println("CsvJdbc - CsvConnection() - suppressHeaders=" + String.valueOf(suppressHeaders));
    this.suppressHeaders = suph;
    DriverManager.println("CsvJdbc - CsvConnection() - extension=" + ext);
    this.extension = ext;
  }


  /**
   *Sets the autoCommit attribute of the CsvConnection object
   *
   * @param  autoCommit        The new autoCommit value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setAutoCommit(boolean autoCommit) throws SQLException { }


  /**
   *Sets the readOnly attribute of the CsvConnection object
   *
   * @param  readOnly          The new readOnly value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setReadOnly(boolean readOnly) throws SQLException { }


  /**
   *Sets the catalog attribute of the CsvConnection object
   *
   * @param  catalog           The new catalog value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setCatalog(String catalog) throws SQLException { }


  /**
   *Sets the transactionIsolation attribute of the CsvConnection object
   *
   * @param  level             The new transactionIsolation value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setTransactionIsolation(int level) throws SQLException { }


  /**
   *Sets the typeMap attribute of the CsvConnection object
   *
   * @param  map               The new typeMap value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void setTypeMap(Map map) throws SQLException { }


  /**
   *Gets the autoCommit attribute of the CsvConnection object
   *
   * @return                   The autoCommit value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean getAutoCommit() throws SQLException
  {
    // always false
    return false;
  }


  /**
   *Gets the closed attribute of the CsvConnection object
   *
   * @return                   The closed value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean isClosed() throws SQLException
  {
    return false;
  }


  /**
   *Gets the metaData attribute of the CsvConnection object
   *
   * @return                   The metaData value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public DatabaseMetaData getMetaData() throws SQLException
  {
    throw new SQLException("NYI");
  }


  /**
   *Gets the readOnly attribute of the CsvConnection object
   *
   * @return                   The readOnly value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean isReadOnly() throws SQLException
  {
    // always reexpecting ;adonly
    return true;
  }


  /**
   *Gets the catalog attribute of the CsvConnection object
   *
   * @return                   The catalog value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public String getCatalog() throws SQLException
  {
    return null;
  }


  /**
   *Gets the transactionIsolation attribute of the CsvConnection object
   *
   * @return                   The transactionIsolation value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public int getTransactionIsolation() throws SQLException
  {
    return Connection.TRANSACTION_NONE;
  }


  /**
   *Gets the warnings attribute of the CsvConnection object
   *
   * @return                   The warnings value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public SQLWarning getWarnings() throws SQLException
  {
    return null;
  }


  /**
   *Gets the typeMap attribute of the CsvConnection object
   *
   * @return                   The typeMap value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Map getTypeMap() throws SQLException
  {
    return new Hashtable();
  }


  /**
   *Description of the Method
   *
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Statement createStatement() throws SQLException
  {
    return new CsvStatement(this);
  }


  /**
   *Description of the Method
   *
   * @param  sql               Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public PreparedStatement prepareStatement(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  sql               Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public CallableStatement prepareCall(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  sql               Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public String nativeSQL(String sql) throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void commit() throws SQLException { }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void rollback() throws SQLException { }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void close() throws SQLException { }


  /**
   *Description of the Method
   *
   * @exception  SQLException  Description of Exception
   * @since
   */
  public void clearWarnings() throws SQLException { }


  /**
   *Description of the Method
   *
   * @param  resultSetType         Description of Parameter
   * @param  resultSetConcurrency  Description of Parameter
   * @return                       Description of the Returned Value
   * @exception  SQLException      Description of Exception
   * @since
   */
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
       throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  sql                   Description of Parameter
   * @param  resultSetType         Description of Parameter
   * @param  resultSetConcurrency  Description of Parameter
   * @return                       Description of the Returned Value
   * @exception  SQLException      Description of Exception
   * @since
   */
  public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency)
       throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Description of the Method
   *
   * @param  sql                   Description of Parameter
   * @param  resultSetType         Description of Parameter
   * @param  resultSetConcurrency  Description of Parameter
   * @return                       Description of the Returned Value
   * @exception  SQLException      Description of Exception
   * @since
   */
  public CallableStatement prepareCall(
      String sql,
      int resultSetType,
      int resultSetConcurrency)
       throws SQLException
  {
    throw new SQLException("Not Supported !");
  }


  /**
   *Gets the filePath attribute of the CsvConnection object
   *
   * @return    The filePath value
   * @since
   */
  protected String getFilePath()
  {
    return filePath;
  }


  /**
   * Insert the method's description here.
   *
   * Creation date: (14-11-2001 8:49:37)
   *
   * @return    java.lang.String
   * @since
   */
  protected String getExtension()
  {
    return extension;
  }


  /**
   * Insert the method's description here.
   *
   * Creation date: (13-11-2001 10:49:00)
   *
   * @return    char
   * @since
   */
  protected char getSeperator()
  {
    return seperator;
  }


  /**
   * Insert the method's description here.
   *
   * Creation date: (13-11-2001 10:49:00)
   *
   * @return    boolean
   * @since
   */
  protected boolean isSuppressHeaders()
  {
    return suppressHeaders;
  }
}

