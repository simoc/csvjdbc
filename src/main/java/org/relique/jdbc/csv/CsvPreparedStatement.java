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
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class CsvPreparedStatement extends CsvStatement implements PreparedStatement
{
	private Object[] parameters;
	private String templateQuery;
	private SqlParser parser;

	protected CsvPreparedStatement(CsvConnection connection, String sql,
		int resultSetType) throws SQLException
	{
		super(connection, resultSetType);

		parser = new SqlParser();
		try
		{
			parser.parse(sql);
		}
		catch (Exception e)
		{
			throw new SQLException(CsvResources.getString("syntaxError") + ": " + e.getMessage());
		}

		parameters = new Object[parser.getPlaceholdersCount() + 1];
		templateQuery = sql;
	}

	private void checkParameterIndex(int parameterIndex) throws SQLException
	{
		if (parameterIndex < 1 || parameterIndex >= parameters.length)
			throw new SQLException(CsvResources.getString("parameterIndex") + ": " + parameterIndex);
	}

	@Override
	public void addBatch() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void clearParameters() throws SQLException
	{
		for (int i = 1; i < parameters.length; i++)
		{
			parameters[i] = null;
		}
	}

	@Override
	public boolean execute() throws SQLException
	{
		checkOpen();

		CsvDriver.writeLog("CsvPreparedStatement:executeQuery() - sql= " + templateQuery);

		setTimeoutMillis();
		cancelled = false;

		ResultSet resultSet = executeQuery();
		lastResultSet = resultSet;

		return true;
	}

	@Override
	public ResultSet executeQuery() throws SQLException
	{

		checkOpen();

		CsvDriver.writeLog("CsvPreparedStatement:executeQuery() - sql= " + templateQuery);

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

		setTimeoutMillis();
		cancelled = false;

		parser.setPlaceholdersValues(parameters);
		return executeParsedQuery(parser);
	}

	@Override
	public int executeUpdate() throws SQLException
	{
		checkOpen();

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": executeUpdate()");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		checkOpen();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		checkOpen();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setAsciiStream(int,InputStream,int)");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setAsciiStream(int,InputStream,long)");
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setBinaryStream(int,InputStream,int)");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setBinaryStream(int,InputStream,long)");
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setBlob(int parameterIndex, InputStream x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setBlob(int parameterIndex, InputStream x, long length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setBlob(int,InputStream,long)");
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = Boolean.valueOf(x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = Byte.valueOf(x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setBytes(int,byte[])");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader x, int length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setCharacterStream(int,Reader,int)");

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader x, long length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setAsciiStream(int,InputStream,long)");

	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setClob(int parameterIndex, Reader x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setClob(int parameterIndex, Reader x, long length) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setClob(int,Reader,long)");
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setDate(int,Date,Calendar)");
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = new Double(x);

	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = Float.valueOf(x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = Integer.valueOf(x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = Long.valueOf(x);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader x, long length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setNCharacterStream(int,Reader,long)");
	}

	@Override
	public void setNClob(int parameterIndex, NClob x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setNClob(int parameterIndex, Reader x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setNClob(int parameterIndex, Reader x, long length) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setNClob(int,Reader,long)");
	}

	@Override
	public void setNString(int parameterIndex, String x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setNString(int,String)");
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setNull(int,int)");
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setNull(int,int,String)");
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setObject(int,Object,int)");
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setObject(int,Object,int,int)");
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = Short.valueOf(x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setTime(int,Time,Calendar)");
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setTimestamp(int,Timestamp,Calendar)");
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		this.parameters[parameterIndex] = x;
	}

	@Override
	@Deprecated
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException
	{
		checkOpen();
		checkParameterIndex(parameterIndex);

		throw new SQLException(CsvResources.getString("methodNotSupported") + ": setUnicodeStream(int,InputStream,int)");
	}
}
