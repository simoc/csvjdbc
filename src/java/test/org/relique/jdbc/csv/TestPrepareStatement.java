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
package test.org.relique.jdbc.csv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Mario Frasca
 * @version $Id: TestPrepareStatement.java,v 1.2 2011/04/22 10:40:46 mfrasca Exp
 *          $
 */
public class TestPrepareStatement extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;

	protected void setUp() {
		filePath = System.getProperty(SAMPLE_FILES_LOCATION_PROPERTY);
		if (filePath == null)
			filePath = RunTests.DEFAULT_FILEPATH;
		assertNotNull("Sample files location property not set !", filePath);

		// load CSV driver
		try {
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		} catch (ClassNotFoundException e) {
			fail("Driver is not in the CLASSPATH -> " + e);
		}

	}

	protected void tearDown() {
		// and delete the files when ready
	}

	/**
	 * using a wrong codec will cause an exception.
	 * 
	 * @throws SQLException
	 */
	public void testCanPrepareStatement() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		try {
			conn.prepareStatement(queryString);
		} catch (UnsupportedOperationException e) {
			fail("can't prepareStatement!");
		}
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void testCanUsePreparedStatement() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 1);
		prepstmt.setInt(2, 3);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(2), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(3), results
				.getObject("id"));
		assertFalse(results.next());
	}

	public void testShortParameter() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Short,String,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM sample4 WHERE id = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setShort(1, (short)3);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
	}

	public void testLongParameter() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Long,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM banks WHERE BLZ = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		long blz = 10020200;
		prepstmt.setLong(1, blz);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column BANK_NAME is wrong", "BHF-BANK (Berlin)", results.getString("BANK_NAME"));
		assertFalse(results.next());
	}

	public void testFloatParameter() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM numeric WHERE C5 < ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setFloat(1, (float)3);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column C5 is wrong", "0.0", results.getString("C5"));
		assertFalse(results.next());
	}

	public void testDoubleParameter() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM numeric WHERE C6 < ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setDouble(1, 1000.0);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column C6 is wrong", "-0.0", results.getString("C6"));
		assertFalse(results.next());
	}

	public void testCanReuseAPreparedStatement() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 1);
		prepstmt.setInt(2, 3);
		ResultSet results1 = prepstmt.executeQuery();

		assertTrue(results1.next());
		assertTrue(results1.next());
		assertTrue(results1.next());
		assertFalse(results1.next());

		prepstmt.setInt(1, 30);
		prepstmt.setInt(2, 50);
		ResultSet results2 = prepstmt.executeQuery();

		assertTrue(results1.isClosed());
		assertTrue(results2.next());
		assertEquals("Integer column ID is wrong", new Integer(41), results2
				.getObject("id"));
		assertFalse(results2.next());
	}

	public void testCanUsePreparedStatementOnStrings() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE job = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setString(1, "Project Manager");
		ResultSet results = prepstmt.executeQuery();
		
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(3), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(4), results
				.getObject("id"));
		assertFalse(results.next());
		
		prepstmt.setString(1, "Office Employee");
		results = prepstmt.executeQuery();
		
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(6), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(7), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(8), results
				.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(9), results
				.getObject("id"));
		assertFalse(results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testNoWhereClause() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		String queryString = "SELECT * FROM sample5";
		try {
			conn.prepareStatement(queryString);
		} catch (UnsupportedOperationException e) {
			fail("can't prepareStatement!");
		}
	}

	public void testTableReader() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM airport where code=?");
		stmt.setString(1, "CDG");
		ResultSet results = stmt.executeQuery();
		assertTrue(results.next());
		assertEquals("NAME wrong", "Paris Charles De Gaulle", results.getString("NAME"));
		assertFalse(results.next());
	}
	
	public void testPreparedStatementWithOrderBy() throws SQLException {
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM sample5 where id > ? order by id";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 7);
		ResultSet results = prepstmt.executeQuery();
		assertTrue(results.next());
		assertEquals("column ID is wrong", 8, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("column ID is wrong", 9, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("column ID is wrong", 41, results.getInt("ID"));
		assertFalse(results.next());
		results.close();
	}
}