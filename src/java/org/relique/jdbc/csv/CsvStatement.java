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
import java.io.File;
import java.util.Enumeration;
import java.util.Vector;

/**
 * This class implements the Statement interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @author     Michael Maraya
 * @author     Tomasz Skutnik
 * @author     Gupta Chetan
 * @author     Christoph Langer
 * @created    25 November 2001
 * @version    $Id: CsvStatement.java,v 1.10 2005/01/08 21:30:14 jackerm Exp $
 */

public class CsvStatement implements Statement
{
    private CsvConnection connection;
    private Vector resultSets = new Vector();
    
    protected int isScrollable = ResultSet.TYPE_SCROLL_INSENSITIVE;

    /**
     *Constructor for the CsvStatement object
     *
     * @param  connection  Description of Parameter
     * @since
     */
    protected CsvStatement(CsvConnection connection, int isScrollable)
    {
        DriverManager.println("CsvJdbc - CsvStatement() - connection=" + connection);
        DriverManager.println("CsvJdbc - CsvStatement() - Asked for " + (isScrollable==ResultSet.TYPE_SCROLL_SENSITIVE?"Scrollable":"Not Scrollable"));
        this.connection = connection;
        this.isScrollable = isScrollable;
    }


    /**
     *Sets the maxFieldSize attribute of the CsvStatement object
     *
     * @param  p0                The new maxFieldSize value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void setMaxFieldSize(int p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Sets the maxRows attribute of the CsvStatement object
     *
     * @param  p0                The new maxRows value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void setMaxRows(int p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Sets the escapeProcessing attribute of the CsvStatement object
     *
     * @param  p0                The new escapeProcessing value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void setEscapeProcessing(boolean p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Sets the queryTimeout attribute of the CsvStatement object
     *
     * @param  p0                The new queryTimeout value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void setQueryTimeout(int p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Sets the cursorName attribute of the CsvStatement object
     *
     * @param  p0                The new cursorName value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void setCursorName(String p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Sets the fetchDirection attribute of the CsvStatement object
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
     *Sets the fetchSize attribute of the CsvStatement object
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
     *Gets the maxFieldSize attribute of the CsvStatement object
     *
     * @return                   The maxFieldSize value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getMaxFieldSize() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the maxRows attribute of the CsvStatement object
     *
     * @return                   The maxRows value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getMaxRows() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the queryTimeout attribute of the CsvStatement object
     *
     * @return                   The queryTimeout value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getQueryTimeout() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the warnings attribute of the CsvStatement object
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
     *Gets the resultSet attribute of the CsvStatement object
     *
     * @return                   The resultSet value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public ResultSet getResultSet() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the updateCount attribute of the CsvStatement object
     *
     * @return                   The updateCount value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getUpdateCount() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the moreResults attribute of the CsvStatement object
     *
     * @return                   The moreResults value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public boolean getMoreResults() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the fetchDirection attribute of the CsvStatement object
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
     *Gets the fetchSize attribute of the CsvStatement object
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
     *Gets the resultSetConcurrency attribute of the CsvStatement object
     *
     * @return                   The resultSetConcurrency value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getResultSetConcurrency() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Gets the resultSetType attribute of the CsvStatement object
     *
     * @return                   The resultSetType value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int getResultSetType() throws SQLException
    {
        return this.isScrollable;
    }


    /**
     *Gets the connection attribute of the CsvStatement object
     *
     * @return                   The connection value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public Connection getConnection() throws SQLException
    {
        return connection;
    }


    /**
     *Description of the Method
     *
     * @param  sql               Description of Parameter
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public ResultSet executeQuery(String sql) throws SQLException
    {
        DriverManager.println("CsvJdbc - CsvStatement:executeQuery() - sql= " + sql);
        SqlParser parser = new SqlParser();
        try
            {
                parser.parse(sql);
            }
        catch (Exception e)
            {
                throw new SQLException("Syntax Error. " + e.getMessage());
            }

			  DriverManager.println("Connection Path: " + connection.getPath());
			  DriverManager.println("Parser Table Name: " + parser.getTableName()); 
			  DriverManager.println("Connection Extension: " + connection.getExtension());
        
        String fileName = connection.getPath() + parser.getTableName() + connection.getExtension();
        
			  DriverManager.println("CSV file name: " + fileName);
        
        File checkFile = new File(fileName);

        if (!checkFile.exists())
            {
                throw new SQLException("Cannot open data file '" + fileName + "'  !");
            }

        if (!checkFile.canRead())
            {
                throw new SQLException("Data file '" + fileName + "'  not readable !");
            }

        CSVReaderAdapter reader;
        try
            {
                if (isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE){
                  reader = new CSVScrollableReader(fileName, connection.getSeperator(),
                                       connection.isSuppressHeaders(), connection.getCharset());
                } else {
                  reader = new CsvReader(fileName, connection.getSeperator(),
                                       connection.isSuppressHeaders(), connection.getCharset(),connection.getQuotechar(),connection.getHeaderline());
                }
            }
        catch (Exception e)
            {
            throw new SQLException("Error reading data file. Message was: " + e);
            }

        CsvResultSet resultSet = new CsvResultSet(this,reader,
                                                  parser.getTableName(), parser.getColumnNames(), this.isScrollable,parser.getWhereColumn(), parser.getWhereValue());
        resultSets.add(resultSet);

        return resultSet;
    }


    /**
     *Description of the Method
     *
     * @param  sql               Description of Parameter
     * @return                   Description of the Returned Value
     * @exception  SQLException  Description of Exception
     * @since
     */
    public int executeUpdate(String sql) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     * Releases this <code>Statement</code> object's database
     * and JDBC resources immediately instead of waiting for
     * this to happen when it is automatically closed.
     * It is generally good practice to release resources as soon as
     * you are finished with them to avoid tying up database
     * resources.
     * <P>
     * Calling the method <code>close</code> on a <code>Statement</code>
     * object that is already closed has no effect.
     * <P>
     * <B>Note:</B> A <code>Statement</code> object is automatically closed
     * when it is garbage collected. When a <code>Statement</code> object is
     * closed, its current <code>ResultSet</code> object, if one exists, is
     * also closed.
     *
     * @exception SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        // close all result sets
        for(Enumeration i = resultSets.elements(); i.hasMoreElements(); ) {
            CsvResultSet resultSet = (CsvResultSet)i.nextElement();
            resultSet.close();
        }
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void cancel() throws SQLException
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
    public boolean execute(String p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Adds a feature to the Batch attribute of the CsvStatement object
     *
     * @param  p0                The feature to be added to the Batch attribute
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void addBatch(String p0) throws SQLException
    {
        throw new SQLException("Not Supported !");
    }


    /**
     *Description of the Method
     *
     * @exception  SQLException  Description of Exception
     * @since
     */
    public void clearBatch() throws SQLException
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
    public int[] executeBatch() throws SQLException
    {
        throw new SQLException("Not Supported !");
    }

    //---------------------------------------------------------------------
    // JDBC 3.0
    //---------------------------------------------------------------------

    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("Statement.getMoreResults(int) unsupported");
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("Statement.getGeneratedKeys() unsupported");
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Statement.executeUpdate(String,int) unsupported");
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Statement.executeUpdate(String,int[]) unsupported");
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Statement.executeUpdate(String,String[]) unsupported");
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Statement.execute(String,int) unsupported");
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Statement.execute(String,int[]) unsupported");
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Statement.execute(String,String[]) unsupported");
    }

    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("Statement.getResultSetHoldability() unsupported");
    }
}

