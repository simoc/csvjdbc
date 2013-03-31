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
 * This class implements the java.sql.Statement JDBC interface for the
 * CsvJdbc driver.
 */
public class CsvStatement implements Statement
{
	private CsvConnection connection;
	protected ResultSet lastResultSet = null;
	private int maxRows = 0;
	private int fetchSize = 1;
	private int fetchDirection = ResultSet.FETCH_FORWARD;
	private boolean closed;

	protected int scrollType = ResultSet.TYPE_SCROLL_INSENSITIVE;

	/**
	 * Creates new Statement for use in executing SQL statements.
	 * 
	 * @param connection
	 *            database connection to create statement for.
	 * @param isScrollable
	 *            either ResultSet.TYPE_SCROLL_INSENSITIVE or
	 *            ResultSet.TYPE_SCROLL_SENSITIVE.
	 */
	protected CsvStatement(CsvConnection connection, int scrollType)
	{
		DriverManager.println("CsvJdbc - CsvStatement() - connection="
				+ connection);
		DriverManager.println("CsvJdbc - CsvStatement() - Asked for "
				+ (scrollType == ResultSet.TYPE_SCROLL_SENSITIVE ? "Scrollable"
						: "Not Scrollable"));
		this.connection = connection;
		this.scrollType = scrollType;
	}

    protected void checkOpen() throws SQLException
    {
    	if (closed)
    		throw new SQLException("Statement is already closed");
    }

	@Override
	public void setMaxFieldSize(int p0) throws SQLException
	{
		throw new SQLException("setMaxFieldSize(int " + p0
				+ ") not Supported !");
	}

	@Override
	public void setMaxRows(int max) throws SQLException
	{
		checkOpen();

		maxRows = max;
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
		throw new SQLException("setEscapeProcessing(boolean " + enable
				+ ") not Supported !");
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException
	{
		throw new SQLException("setQueryTimeout(int " + seconds + ") not Supported !");
	}

	@Override
	public void setCursorName(String name) throws SQLException
	{
		throw new SQLException("setCursorName(String \"" + name
				+ "\") not Supported !");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		checkOpen();

		if (direction == ResultSet.FETCH_FORWARD ||
			direction == ResultSet.FETCH_REVERSE ||
			direction == ResultSet.FETCH_UNKNOWN)
		{
			this.fetchDirection = direction;
		}
		else
		{
			throw new SQLException("setFetchDirection: direction not supported: " + direction);
		}
	}

	@Override
	public void setFetchSize(int rows) throws SQLException
	{
		checkOpen();

		this.fetchSize = rows;
	}

	@Override
	public int getMaxFieldSize() throws SQLException
	{
		throw new SQLException("getMaxFieldSize() not Supported !");
	}

	@Override
	public int getMaxRows() throws SQLException
	{
		checkOpen();

		return maxRows;
	}

	@Override
	public int getQueryTimeout() throws SQLException
	{
		checkOpen();

		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		checkOpen();

		return null;
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		checkOpen();

		return lastResultSet;
	}

	@Override
	public int getUpdateCount() throws SQLException
	{
		checkOpen();

		/*
		 * Driver is read-only, so no updates are possible.
		 */
		return -1;
	}

	@Override
	public boolean getMoreResults() throws SQLException
	{
		checkOpen();

		try
		{
			/*
			 * Close any ResultSet that is currently open.
			 */
			if (lastResultSet != null)
				lastResultSet.close();
		}
		finally
		{
			lastResultSet = null;
		}
		return false;
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		checkOpen();

		return fetchDirection;
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		checkOpen();

		return fetchSize;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		checkOpen();
		
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetType() throws SQLException
	{
		checkOpen();

		return this.scrollType;
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		checkOpen();

		return connection;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
		checkOpen();

		DriverManager.println("CsvJdbc - CsvStatement:executeQuery() - sql= " + sql);

		/*
		 * Close any previous ResultSet, as required by JDBC.
		 */
		try
		{
			if (lastResultSet != null)
				lastResultSet.close();
		}
		finally
		{
			lastResultSet = null;
		}

		SqlParser parser = new SqlParser();
		try
		{
			parser.parse(sql);
		}
		catch (Exception e)
		{
			throw new SQLException("Syntax Error. " + e.getMessage());
		}

		return executeParsedQuery(parser);
	}

	protected ResultSet executeParsedQuery(SqlParser parser)
			throws SQLException
	{
		String path = connection.getPath();
		TableReader tableReader = connection.getTableReader();
		if (path != null)
			DriverManager.println("Connection Path: " + path);
		else
			DriverManager.println("Connection TableReader: " + tableReader.getClass().getName());
		DriverManager.println("Parser Table Name: " + parser.getTableName());
		DriverManager.println("Connection Extension: " + connection.getExtension());

		DataReader reader = null;
		String fileName = null;
		String tableName = parser.getTableName();
		if (tableName == null)
		{
			/*
			 * Create an empty dataset with one row.
			 */
			String[] columnNames = new String[0];
			String[] columnTypes = new String[0];
			ArrayList<Object[]> rows = new ArrayList<Object[]>();
			rows.add(new Object[0]);
			reader = new ListDataReader(columnNames, columnTypes, rows);
		}
		else
		{
			if (path != null && (!connection.isIndexedFiles()))
			{
				fileName = path + tableName + connection.getExtension();

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
			}

			try
			{
				if (connection.getExtension().equalsIgnoreCase(".dbf"))
				{
					reader = new DbfReader(fileName, parser.getTableAlias());
				}
				else
				{
					BufferedReader input;
					if (tableReader == null)
					{
						InputStream in;
						CryptoFilter filter = connection.getDecryptingCodec();
						if (connection.isIndexedFiles())
						{
							String fileNamePattern = parser.getTableName()
									+ connection.getFileNamePattern()
									+ connection.getExtension();
							String[] nameParts = connection.getNameParts();
							String dirName = connection.getPath();
							in = new FileSetInputStream(dirName,
									fileNamePattern, nameParts,
									connection.getSeparator(),
									connection.isFileTailPrepend(),
									connection.isSuppressHeaders(), filter,
									connection.getSkipLeadingDataLines()
											+ connection.getTransposedLines());
						}
						else if (filter == null)
						{
							in = new FileInputStream(fileName);
						}
						else
						{
							filter.reset();
							in = new EncryptedFileInputStream(fileName, filter);
						}
						if (connection.getCharset() != null)
						{
							input = new BufferedReader(new InputStreamReader(in, connection.getCharset()));
						}
						else
						{
							input = new BufferedReader(new InputStreamReader(in));
						}
					}
					else
					{
						/*
						 * Reader for table comes from user-provided class.
						 */
						input = new BufferedReader(tableReader.getReader(this, tableName));
					}

					String headerline = connection.getHeaderline(tableName);
					CsvRawReader rawReader = new CsvRawReader(input,
							parser.getTableAlias(), connection.getSeparator(),
							connection.isSuppressHeaders(),
							connection.getQuotechar(),
							connection.getCommentChar(), headerline,
							connection.getExtension(),
							connection.getTrimHeaders(),
							connection.getSkipLeadingLines(),
							connection.isIgnoreUnparseableLines(),
							connection.getDecryptingCodec(),
							connection.isDefectiveHeaders(),
							connection.getSkipLeadingDataLines(),
							connection.getQuoteStyle(),
							connection.getFixedWidthColumns());
					reader = new CsvReader(rawReader,
							connection.getTransposedLines(),
							connection.getTransposedFieldsToSkip(), headerline);
				}
			}
			catch (IOException e)
			{
				throw new SQLException("Error reading data file: " + e);
			}
			catch (SQLException e)
			{
				throw e;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				throw new SQLException("Error initializing DataReader: " + e);
			}
		}

		CsvResultSet resultSet = null;
		try
		{
			resultSet = new CsvResultSet(this, reader, tableName,
					parser.getColumns(), parser.isDistinct(), this.scrollType,
					parser.getWhereClause(), parser.getGroupByColumns(),
					parser.getHavingClause(), parser.getOrderByColumns(),
					parser.getLimit(), parser.getOffset(),
					connection.getColumnTypes(tableName),
					connection.getSkipLeadingLines());
			lastResultSet = resultSet;
		}
		catch (ClassNotFoundException e)
		{
			DriverManager.println("" + e);
		}

		return resultSet;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException
	{
		throw new SQLException("executeUpdate(String \"" + sql + "\") not Supported !");
	}

	@Override
	public void close() throws SQLException
	{
		try
		{
			if (lastResultSet != null)
				lastResultSet.close();
		}
		finally
		{
			lastResultSet = null;
			closed = true;
		}
		connection.removeStatement(this);
	}

	@Override
	public void cancel() throws SQLException
	{
		throw new SQLException("cancel() not Supported !");
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		checkOpen();
	}

	@Override
	public boolean execute(String sql) throws SQLException
	{
		checkOpen();

		try
		{
			executeQuery(sql);
			return true;
		}
		catch (Exception e)
		{
			throw new SQLException("execute(String \"" + sql + "\") not Supported !");
		}
	}

	@Override
	public void addBatch(String sql) throws SQLException
	{
		throw new SQLException("addBatch(String \"" + sql + "\") not Supported !");
	}

	@Override
	public void clearBatch() throws SQLException
	{
		throw new SQLException("clearBatch() not Supported !");
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		throw new SQLException("executeBatch() not Supported !");
	}

	// ---------------------------------------------------------------------
	// JDBC 3.0
	// ---------------------------------------------------------------------

	@Override
	public boolean getMoreResults(int current) throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.getMoreResults(int) unsupported");
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.getGeneratedKeys() unsupported");
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.executeUpdate(String,int) unsupported");
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.executeUpdate(String,int[]) unsupported");
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.executeUpdate(String,String[]) unsupported");
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.execute(String,int) unsupported");
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.execute(String,int[]) unsupported");
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.execute(String,String[]) unsupported");
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.getResultSetHoldability() unsupported");
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public boolean isPoolable() throws SQLException
	{
		checkOpen();

		return false;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException
	{
		checkOpen();
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isCloseOnCompletion() throws SQLException
	{
		checkOpen();

		return false;
	}

	public void closeOnCompletion() throws SQLException
	{
		throw new UnsupportedOperationException(
				"Statement.closeOnCompletion() not supported");
	}
}
