package org.relique.jdbc.csv;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.DriverManager;
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

public class CsvPreparedStatement extends CsvStatement implements
		PreparedStatement {

	private Object[] parameters;
	private String templateQuery;
	private SqlParser parser;

	protected CsvPreparedStatement(CsvConnection connection, String sql, int isScrollable) throws SQLException {
		super(connection, isScrollable);

		parser = new SqlParser();
		try {
			parser.parse(sql);
		} catch (Exception e) {
			throw new SQLException("Syntax Error. " + e.getMessage());
		}

		parameters = new Object[parser.getPlaceholdersCount() + 1];
		templateQuery = sql;
	}

	@Override
	public void addBatch() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearParameters() throws SQLException {
		for(int i=1; i<parameters.length; i++) {
			parameters[i] = null;
		}
	}

	@Override
	public boolean execute() throws SQLException {
		throw new SQLException("execute() not Supported !");
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		DriverManager.println("CsvJdbc - CsvStatement:executeQuery() - sql= "
				+ templateQuery);

		/*
		 * Close any previous ResultSet, as required by JDBC.
		 */
		try {
			if (lastResultSet != null)
				lastResultSet.close();
		} finally {
			lastResultSet = null;
		}

		parser.setPlaceholdersValues(parameters);
		return executeParsedQuery(parser);
	}

	@Override
	public int executeUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setArray(int arg0, Array arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBlob(int arg0, Blob arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setBlob(int arg0, InputStream arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBoolean(int arg0, boolean arg1) throws SQLException {
		this.parameters[arg0] = Boolean.valueOf(arg1);
	}

	@Override
	public void setByte(int arg0, byte arg1) throws SQLException {
		this.parameters[arg0] = Byte.valueOf(arg1);
	}

	@Override
	public void setBytes(int arg0, byte[] arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int arg0, Clob arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setClob(int arg0, Reader arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDate(int arg0, Date arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDouble(int arg0, double arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setFloat(int arg0, float arg1) throws SQLException {
		this.parameters[arg0] = Float.valueOf(arg1);
	}

	@Override
	public void setInt(int arg0, int arg1) throws SQLException {
		this.parameters[arg0] = Integer.valueOf(arg1);
	}

	@Override
	public void setLong(int arg0, long arg1) throws SQLException {
		this.parameters[arg0] = Long.valueOf(arg1);
	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int arg0, NClob arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setNClob(int arg0, Reader arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNString(int arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNull(int arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNull(int arg0, int arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setObject(int arg0, Object arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setObject(int arg0, Object arg1, int arg2, int arg3)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRef(int arg0, Ref arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setRowId(int arg0, RowId arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setShort(int arg0, short arg1) throws SQLException {
		this.parameters[arg0] = Short.valueOf(arg1);
	}

	@Override
	public void setString(int arg0, String arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setTime(int arg0, Time arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setURL(int arg0, URL arg1) throws SQLException {
		this.parameters[arg0] = arg1;
	}

	@Override
	@Deprecated
	public void setUnicodeStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

}
