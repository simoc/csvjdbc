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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.TestCase;

/**
 * This class tests SQL LIMIT OFFSET keywords in the CsvJdbc driver.
 */
public class TestLimitOffset extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;
	private DateFormat toUTC;

	public TestLimitOffset(String name) {
		super(name);
	}

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
		toUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		toUTC.setTimeZone(TimeZone.getTimeZone("UTC"));  

	}
	
	public void testLimitRows() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 limit 4");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 1, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertFalse(results.next());
	}

	public void testLimitHigherThanRowCount() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample4 limit 999");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 1, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 4, results.getInt("ID"));
		assertFalse(results.next());
	}

	public void testLimitWhere() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 where id > 6 limit 3");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 7, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertFalse(results.next());
	}

	public void testLimitOrderBy() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id, name from sample5 order by id desc limit 2");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 9, results.getInt("ID"));
		assertFalse(results.next());
	}
	
	public void testOffsetRows() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 limit 9 offset 8");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 9, results.getInt("ID"));
		assertFalse(results.next());
	}
	
	public void testOffsetNoResults() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 limit 99 offset 99");
		assertFalse(results.next());
	}
	
	public void testOffsetWhere() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 where id > 2 limit 3 offset 4");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 6, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 7, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertFalse(results.next());
	}
	
	public void testOffsetOrderBy() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select * from sample5 order by id limit 99 offset 5");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 6, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 7, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 9, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertFalse(results.next());
	}
	
	public void testBadOffset() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		try {
			ResultSet results = stmt
				.executeQuery("select * from sample5 limit 5 offset -1");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertTrue(e.toString().startsWith("java.sql.SQLException: Syntax Error."));
		}
	}
}
