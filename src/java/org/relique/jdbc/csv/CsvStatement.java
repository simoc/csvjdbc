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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;

import org.relique.io.CryptoFilter;
import org.relique.io.DataReader;
import org.relique.io.EncryptedFileInputStream;
import org.relique.io.FileSetInputStream;
import org.relique.io.ListDataReader;
import org.relique.io.TableReader;
import org.relique.jdbc.dbf.DbfReader;

/**
 * This class implements the Statement interface for the CsvJdbc driver.
 * 
 * @author Jonathan Ackerman
 * @author Sander Brienen
 * @author Michael Maraya
 * @author Tomasz Skutnik
 * @author Chetan Gupta
 * @author Christoph Langer
 * @created 25 November 2001
 * @version $Id: CsvStatement.java,v 1.48 2011/11/01 07:55:49 simoc Exp $
 */

public class CsvStatement implements Statement {
	private CsvConnection connection;
	protected ResultSet lastResultSet = null;
	private int maxRows = 0;
	private int fetchSize = 1;

	protected int isScrollable = ResultSet.TYPE_SCROLL_INSENSITIVE;
	/**
	 *Constructor for the CsvStatement object
	 * 
	 * @param connection
	 *            Description of Parameter
	 * @since
	 */
	protected CsvStatement(CsvConnection connection, int isScrollable) {
		DriverManager.println("CsvJdbc - CsvStatement() - connection="
				+ connection);
		DriverManager
				.println("CsvJdbc - CsvStatement() - Asked for "
						+ (isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE ? "Scrollable"
								: "Not Scrollable"));
		this.connection = connection;
		this.isScrollable = isScrollable;
	}

	/**
	 *Sets the maxFieldSize attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new maxFieldSize value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setMaxFieldSize(int p0) throws SQLException {
		throw new SQLException("setMaxFieldSize(int " + p0 + ") not Supported !");
	}

	/**
	 *Sets the maxRows attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new maxRows value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setMaxRows(int max) throws SQLException {
		maxRows = max;
	}

	/**
	 *Sets the escapeProcessing attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new escapeProcessing value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setEscapeProcessing(boolean p0) throws SQLException {
		throw new SQLException("setEscapeProcessing(boolean " + p0 + ") not Supported !");
	}

	/**
	 *Sets the queryTimeout attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new queryTimeout value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setQueryTimeout(int p0) throws SQLException {
		throw new SQLException("setQueryTimeout(int " + p0 + ") not Supported !");
	}

	/**
	 *Sets the cursorName attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new cursorName value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setCursorName(String p0) throws SQLException {
		throw new SQLException("setCursorName(String \"" + p0 + "\") not Supported !");
	}

	/**
	 *Sets the fetchDirection attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new fetchDirection value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setFetchDirection(int p0) throws SQLException {
		throw new SQLException("setFetchDirection(int " + p0 + ") not Supported !");
	}

	/**
	 *Sets the fetchSize attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The new fetchSize value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void setFetchSize(int rows) throws SQLException {
		this.fetchSize = rows;
	}

	/**
	 *Gets the maxFieldSize attribute of the CsvStatement object
	 * 
	 * @return The maxFieldSize value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getMaxFieldSize() throws SQLException {
		throw new SQLException("getMaxFieldSize() not Supported !");
	}

	/**
	 *Gets the maxRows attribute of the CsvStatement object
	 * 
	 * @return The maxRows value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getMaxRows() throws SQLException {
		return maxRows;
	}

	/**
	 *Gets the queryTimeout attribute of the CsvStatement object
	 * 
	 * @return The queryTimeout value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getQueryTimeout() throws SQLException {
		return 0;
	}

	/**
	 *Gets the warnings attribute of the CsvStatement object
	 * 
	 * @return The warnings value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("getWarnings() not Supported !");
	}

	public ResultSet getResultSet() throws SQLException {
		return lastResultSet;
	}

	public int getUpdateCount() throws SQLException {
		/*
		 * Driver is read-only, so no updates are possible.
		 */
		return -1;
	}

	public boolean getMoreResults() throws SQLException {
		try {
			/*
			 * Close any ResultSet that is currently open.
			 */
			if (lastResultSet != null)
				lastResultSet.close();
		} finally {
			lastResultSet = null;
		}
		return false;
	}

	/**
	 *Gets the fetchDirection attribute of the CsvStatement object
	 * 
	 * @return The fetchDirection value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getFetchDirection() throws SQLException {
		throw new SQLException("getFetchDirection() not Supported !");
	}

	/**
	 *Gets the fetchSize attribute of the CsvStatement object
	 * 
	 * @return The fetchSize value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getFetchSize() throws SQLException {
		return fetchSize;
	}

	/**
	 *Gets the resultSetConcurrency attribute of the CsvStatement object
	 * 
	 * @return The resultSetConcurrency value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLException("getResultSetConcurrency() not Supported !");
	}

	/**
	 *Gets the resultSetType attribute of the CsvStatement object
	 * 
	 * @return The resultSetType value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int getResultSetType() throws SQLException {
		return this.isScrollable;
	}

	/**
	 *Gets the connection attribute of the CsvStatement object
	 * 
	 * @return The connection value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public Connection getConnection() throws SQLException {
		return connection;
	}

	/**
	 *Description of the Method
	 * 
	 * @param sql
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		DriverManager.println("CsvJdbc - CsvStatement:executeQuery() - sql= "
				+ sql);
		
		/*
		 * Close any previous ResultSet, as required by JDBC.
		 */
		try {
			if (lastResultSet != null)
				lastResultSet.close();
		} finally {
			lastResultSet = null;
		}

		SqlParser parser = new SqlParser();
		try {
			parser.parse(sql);
		} catch (Exception e) {
			throw new SQLException("Syntax Error. " + e.getMessage());
		}

		return executeParsedQuery(parser);
	}

	protected ResultSet executeParsedQuery(SqlParser parser)
			throws SQLException {
		String path = connection.getPath();
		TableReader tableReader = connection.getTableReader();
		if (path != null)
			DriverManager.println("Connection Path: " + path);
		else
			DriverManager.println("Connection TableReader: " + tableReader.getClass().getName());
		DriverManager.println("Parser Table Name: " + parser.getTableName());
		DriverManager.println("Connection Extension: "
				+ connection.getExtension());

		DataReader reader = null;
		String fileName = null;
		String tableName = parser.getTableName();
		if (tableName == null) {
			/*
			 * Create an empty dataset with one row.
			 */
			String []columnNames = new String[0];
			String []columnTypes = new String[0];
			ArrayList rows = new ArrayList();
			rows.add(new Object[0]);
			reader = new ListDataReader(columnNames, columnTypes, rows);
		} else {
			if (path != null && (!connection.isIndexedFiles())) {
				fileName = path + tableName	+ connection.getExtension();
	
				DriverManager.println("CSV file name: " + fileName);
	
				File checkFile = new File(fileName);
	
				if (!checkFile.exists()) {
					throw new SQLException("Cannot open data file '" + fileName
							+ "'  !");
				}
	
				if (!checkFile.canRead()) {
					throw new SQLException("Data file '" + fileName
							+ "'  not readable !");
				}
			}
			
			try {
				if(connection.getExtension().equalsIgnoreCase(".dbf")) {
					reader = new DbfReader(fileName, parser.getTableAlias());
				} else {
					BufferedReader input;
					if (tableReader == null) {
						InputStream in;
						CryptoFilter filter = connection.getDecryptingCodec();
						if (connection.isIndexedFiles()) {
							String fileNamePattern = parser.getTableName()
									+ connection.getFileNamePattern()
									+ connection.getExtension();
							String[] nameParts = connection.getNameParts();
							String dirName = connection.getPath();
							in = new FileSetInputStream(dirName, fileNamePattern,
									nameParts, connection.getSeparator(), connection.isFileTailPrepend(),
									connection.isSuppressHeaders(), filter, connection.getSkipLeadingDataLines() + connection.getTransposedLines());
						} else if (filter==null) {
							in = new FileInputStream(fileName);
						} else {
							filter.reset();
							in = new EncryptedFileInputStream(fileName, filter);
						}
						if (connection.getCharset() != null) {
							input = new BufferedReader(new InputStreamReader(in, connection
									.getCharset()));
						} else {
							input = new BufferedReader(new InputStreamReader(in));
						}
					} else {
						/*
						 * Reader for table comes from user-provided class.
						 */
						input = new BufferedReader(tableReader.getReader(this, tableName));
					}
	
					String headerline = connection.getHeaderline(tableName);
					CsvRawReader rawReader = new CsvRawReader(input, parser.getTableAlias(), connection.getSeparator(),
							connection.isSuppressHeaders(), connection.getQuotechar(),
							connection.getCommentChar(), headerline,
							connection.getExtension(), connection.getTrimHeaders(),
							connection.getSkipLeadingLines(), connection
							.isIgnoreUnparseableLines(), connection
							.getDecryptingCodec(), connection
							.isDefectiveHeaders(), connection
							.getSkipLeadingDataLines(), connection.getQuoteStyle(),
							connection.getFixedWidthColumns());
					reader = new CsvReader(rawReader, connection.getTransposedLines(), connection.getTransposedFieldsToSkip(), headerline);
				}
			} catch (IOException e) {
				throw new SQLException("Error reading data file. Message was: " + e);
			} catch (SQLException e) {
				throw e;
			} catch (Exception e) {
				e.printStackTrace();
				throw new SQLException("Error initializing DataReader: " + e);
			}
		}

		CsvResultSet resultSet = null;
		try {
			resultSet = new CsvResultSet(this, reader, tableName,
					parser.getColumns(), parser.isDistinct(),
					this.isScrollable, parser.getWhereClause(),
					parser.getOrderByColumns(),
					parser.getLimit(), parser.getOffset(),
					connection.getColumnTypes(tableName), 
					connection.getSkipLeadingLines());
			lastResultSet = resultSet;
		} catch (ClassNotFoundException e) {
			DriverManager.println("" + e);
		}

		return resultSet;
	}

	/**
	 *Description of the Method
	 * 
	 * @param sql
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int executeUpdate(String sql) throws SQLException {
		throw new SQLException("executeUpdate(String \"" + sql + "\") not Supported !");
	}

	public void close() throws SQLException {
		try {
			if (lastResultSet != null)
				lastResultSet.close();
		} finally {
			lastResultSet = null;
		}
		connection.removeStatement(this);
	}

	/**
	 *Description of the Method
	 * 
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void cancel() throws SQLException {
		throw new SQLException("cancel() not Supported !");
	}

	/**
	 *Description of the Method
	 * 
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void clearWarnings() throws SQLException {
		throw new SQLException("clearWarnings() not Supported !");
	}

	/**
	 *Description of the Method
	 * 
	 * @param p0
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public boolean execute(String p0) throws SQLException {
		try {
			executeQuery(p0);
			return true;
		} catch (Exception e) {
			throw new SQLException("execute(String \"" + p0 + "\") not Supported !");
		}
	}

	/**
	 *Adds a feature to the Batch attribute of the CsvStatement object
	 * 
	 * @param p0
	 *            The feature to be added to the Batch attribute
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void addBatch(String p0) throws SQLException {
		throw new SQLException("addBatch(String \"" + p0 + "\") not Supported !");
	}

	/**
	 *Description of the Method
	 * 
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public void clearBatch() throws SQLException {
		throw new SQLException("clearBatch() not Supported !");
	}

	/**
	 *Description of the Method
	 * 
	 * @return Description of the Returned Value
	 * @exception SQLException
	 *                Description of Exception
	 * @since
	 */
	public int[] executeBatch() throws SQLException {
		throw new SQLException("executeBatch() not Supported !");
	}

	// ---------------------------------------------------------------------
	// JDBC 3.0
	// ---------------------------------------------------------------------

	public boolean getMoreResults(int current) throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.getMoreResults(int) unsupported");
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.getGeneratedKeys() unsupported");
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.executeUpdate(String,int) unsupported");
	}

	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.executeUpdate(String,int[]) unsupported");
	}

	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.executeUpdate(String,String[]) unsupported");
	}

	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.execute(String,int) unsupported");
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.execute(String,int[]) unsupported");
	}

	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.execute(String,String[]) unsupported");
	}

	public int getResultSetHoldability() throws SQLException {
		throw new UnsupportedOperationException(
				"Statement.getResultSetHoldability() unsupported");
	}

	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub

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
