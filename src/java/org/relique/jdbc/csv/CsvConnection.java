/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.Hashtable;

/**
 * This class implements the Connection interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @author     Michael Maraya
 * @version    $Id: CsvConnection.java,v 1.5 2002/08/24 22:30:06 mmaraya Exp $
 */
public class CsvConnection implements Connection {

    /** Directory where the CSV files to use are located */
    private String path;

    /** File extension to use */
    private String extension = CsvDriver.DEFAULT_EXTENSION;

    /** Field separator to use */
    private char separator = CsvDriver.DEFAULT_SEPARATOR;

    /** Should headers be suppressed */
    private boolean suppressHeaders = CsvDriver.DEFAULT_SUPPRESS;

    /** Collection of all created Statements */
    private Vector statements = new Vector();

    /**
     * Creates a new CsvConnection that takes the supplied path
     * @param path directory where the CSV files are located
     */
    protected CsvConnection(String path) {
        // validate argument(s)
        if(path == null || path.length() == 0) {
            throw new IllegalArgumentException(
                    "'path' argument may not be empty or null");
        }
        this.path = path;
    }

    /**
     * Creates a new CsvConnection that takes the supplied path and properties
     * @param path directory where the CSV files are located
     * @param info set of properties containing custom options
     */
    protected CsvConnection(String path, Properties info) {
        this(path);
        // check for properties
        if(info != null) {
            // set the file extension to be used
            if(info.getProperty(CsvDriver.FILE_EXTENSION) != null) {
                extension = info.getProperty(CsvDriver.FILE_EXTENSION);
            }
            // set the separator character to be used
            if(info.getProperty(CsvDriver.SEPARATOR) != null) {
                separator = info.getProperty(CsvDriver.SEPARATOR).charAt(0);
            }
            // set the header suppression flag
            if(info.getProperty(CsvDriver.SUPPRESS_HEADERS) != null) {
                suppressHeaders = Boolean.valueOf(info.getProperty(
                        CsvDriver.SUPPRESS_HEADERS)).booleanValue();
            }
        }
    }

    /**
     * Creates a <code>Statement</code> object for sending
     * SQL statements to the database.
     * SQL statements without parameters are normally
     * executed using <code>Statement</code> objects. If the same SQL statement
     * is executed many times, it may be more efficient to use a
     * <code>PreparedStatement</code> object.
     * <P>
     * Result sets created using the returned <code>Statement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     *
     * @return a new default <code>Statement</code> object
     * @exception SQLException if a database access error occurs
     */
    public Statement createStatement() throws SQLException {
        CsvStatement statement = new CsvStatement(this);
        statements.add(statement);
        return statement;
    }

    /**
     * Creates a <code>PreparedStatement</code> object for sending
     * parameterized SQL statements to the database.
     * <P>
     * A SQL statement with or without IN parameters can be
     * pre-compiled and stored in a <code>PreparedStatement</code> object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     *
     * <P><B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method <code>prepareStatement</code> will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the <code>PreparedStatement</code>
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain <code>SQLException</code> objects.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     * parameter placeholders
     * @return a new default <code>PreparedStatement</code> object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database access error occurs
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
      throw new UnsupportedOperationException(
              "Connection.prepareStatement(String) unsupported");
    }

    /**
     * Creates a <code>CallableStatement</code> object for calling
     * database stored procedures.
     * The <code>CallableStatement</code> object provides
     * methods for setting up its IN and OUT parameters, and
     * methods for executing the call to a stored procedure.
     *
     * <P><B>Note:</B> This method is optimized for handling stored
     * procedure call statements. Some drivers may send the call
     * statement to the database when the method <code>prepareCall</code>
     * is done; others
     * may wait until the <code>CallableStatement</code> object
     * is executed. This has no
     * direct effect on users; however, it does affect which method
     * throws certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>CallableStatement</code>
     * object will by default be type <code>TYPE_FORWARD_ONLY</code>
     * and have a concurrency level of <code>CONCUR_READ_ONLY</code>.
     *
     * @param sql an SQL statement that may contain one or more '?'
     * parameter placeholders. Typically this  statement is a JDBC
     * function call escape string.
     * @return a new default <code>CallableStatement</code> object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database access error occurs
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException(
                "Connection.prepareCall(String) unsupported");
    }

    /**
     * Converts the given SQL statement into the system's native SQL grammar.
     * A driver may convert the JDBC SQL grammar into its system's
     * native SQL grammar prior to sending it. This method returns the
     * native form of the statement that the driver would have sent.
     *
     * @param sql an SQL statement that may contain one or more '?'
     * parameter placeholders
     * @return the native form of this statement
     * @exception SQLException if a database access error occurs
     */
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException(
                "Connection.nativeSQL(String) unsupported");
    }

    /**
     * Sets this connection's auto-commit mode to the given state.
     * If a connection is in auto-commit mode, then all its SQL
     * statements will be executed and committed as individual
     * transactions.  Otherwise, its SQL statements are grouped into
     * transactions that are terminated by a call to either
     * the method <code>commit</code> or the method <code>rollback</code>.
     * By default, new connections are in auto-commit
     * mode.
     * <P>
     * The commit occurs when the statement completes or the next
     * execute occurs, whichever comes first. In the case of
     * statements returning a <code>ResultSet</code> object,
     * the statement completes when the last row of the
     * <code>ResultSet</code> object has been retrieved or the
     * <code>ResultSet</code> object has been closed. In advanced cases, a
     * single statement may return multiple results as well as output
     * parameter values. In these cases, the commit occurs when all results and
     * output parameter values have been retrieved.
     * <P>
     * <B>NOTE:</B>  If this method is called during a transaction, the
     * transaction is committed.
     *
     * @param autoCommit <code>true</code> to enable auto-commit mode;
     *         <code>false</code> to disable it
     * @exception SQLException if a database access error occurs
     * @see #getAutoCommit
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException(
                "Connection.setAutoCommit(boolean) unsupported");
    }

    /**
     * Retrieves the current auto-commit mode for this <code>Connection</code>
     * object.
     *
     * @return the current state of this <code>Connection</code> object's
     *         auto-commit mode
     * @exception SQLException if a database access error occurs
     * @see #setAutoCommit
     */
    public boolean getAutoCommit() throws SQLException {
        throw new UnsupportedOperationException(
                "Connection.getAutoCommit() unsupported");
    }

    /**
     * Makes all changes made since the previous
     * commit/rollback permanent and releases any database locks
     * currently held by this <code>Connection</code> object.
     * This method should be
     * used only when auto-commit mode has been disabled.
     *
     * @exception SQLException if a database access error occurs or this
     *            <code>Connection</code> object is in auto-commit mode
     * @see #setAutoCommit
     */
    public void commit() throws SQLException {
        throw new UnsupportedOperationException(
                "Connection.commit() unsupported");
    }

    /**
     * Undoes all changes made in the current transaction
     * and releases any database locks currently held
     * by this <code>Connection</code> object. This method should be
     * used only when auto-commit mode has been disabled.
     *
     * @exception SQLException if a database access error occurs or this
     *            <code>Connection</code> object is in auto-commit mode
     * @see #setAutoCommit
     */
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException(
                "Connection.rollback() unsupported");
    }

    /**
     * Releases this <code>Connection</code> object's database and JDBC
     * resources immediately instead of waiting for them to be automatically
     * released.
     * <P>
     * Calling the method <code>close</code> on a <code>Connection</code>
     * object that is already closed is a no-op.
     * <P>
     * <B>Note:</B> A <code>Connection</code> object is automatically
     * closed when it is garbage collected. Certain fatal errors also
     * close a <code>Connection</code> object.
     *
     * @exception SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        // close all created statements
        for(Enumeration i = statements.elements(); i.hasMoreElements(); ) {
            CsvStatement statement = (CsvStatement)i.nextElement();
            statement.close();
        }
    }


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
    return path;
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
    return separator;
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

