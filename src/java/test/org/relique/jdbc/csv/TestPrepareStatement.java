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
}