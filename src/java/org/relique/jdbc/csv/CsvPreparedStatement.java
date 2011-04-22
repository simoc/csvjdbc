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
import java.util.Vector;

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
			e.printStackTrace();
			throw new SQLException("Syntax Error. " + e.getMessage());
		}

		parameters = new Object[parser.getPlaceholdersCount() + 1];
		templateQuery = sql;
	}

	public void addBatch() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void clearParameters() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean execute() throws SQLException {
		throw new SQLException("Not Supported !");
	}

	public ResultSet executeQuery() throws SQLException {
		DriverManager.println("CsvJdbc - CsvStatement:executeQuery() - sql= "
				+ templateQuery);
		parser.setPlaceholdersValues(parameters);
		return executeParsedQuery(parser);
	}

	public int executeUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public ParameterMetaData getParameterMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setArray(int arg0, Array arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBinaryStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBlob(int arg0, Blob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBlob(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBoolean(int arg0, boolean arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setByte(int arg0, byte arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBytes(int arg0, byte[] arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCharacterStream(int arg0, Reader arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int arg0, Clob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setDate(int arg0, Date arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setDouble(int arg0, double arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setFloat(int arg0, float arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setInt(int arg0, int arg1) throws SQLException {
		this.parameters[arg0] = Integer.valueOf(arg1);
	}

	public void setLong(int arg0, long arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int arg0, NClob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNString(int arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNull(int arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNull(int arg0, int arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setObject(int arg0, Object arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setObject(int arg0, Object arg1, int arg2, int arg3)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setRef(int arg0, Ref arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setRowId(int arg0, RowId arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setShort(int arg0, short arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setString(int arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTime(int arg0, Time arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setURL(int arg0, URL arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setUnicodeStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub

	}

}
