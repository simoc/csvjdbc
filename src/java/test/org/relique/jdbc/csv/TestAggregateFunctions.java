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
 * This class tests SQL aggregate functions in the CsvJdbc driver.
 */
public class TestAggregateFunctions extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;
	private DateFormat toUTC;

	public TestAggregateFunctions(String name) {
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

	public void testCountStar() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT COUNT(*) FROM sample");
		assertTrue(results.next());
		assertEquals("Incorrect count", 6, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testCountColumn() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT count(ID) FROM sample");
		assertTrue(results.next());
		assertEquals("Incorrect count", "6", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testCountInvalidColumn() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try {
			stmt.executeQuery("SELECT count(XXXX) FROM sample");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column name: XXXX", "" + e);
		}
	}

	public void testCountPlusColumn() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try {
			stmt.executeQuery("SELECT ID, count(ID) FROM sample");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Query columns cannot be combined with aggregate functions", "" + e);
		}
	}

	public void testCountWhere() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT count(*) C FROM sample where ID like '%234'");
		assertTrue(results.next());
		assertEquals("Incorrect count", 2, results.getInt("C"));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testCountNoResults() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT count(*) FROM sample where ID like 'ZZZ%'");
		assertTrue(results.next());
		assertEquals("Incorrect count", 0, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testCountInWhereClause() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try {
			stmt.executeQuery("SELECT * FROM sample where count(*)=1");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Aggregate functions not allowed in WHERE clause", "" + e);
		}
	}

	public void testMax() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MAX(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect max", 6, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMaxWhere() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select max(name) from sample8 where id < 3");
		assertTrue(results.next());
		assertEquals("Incorrect max", "Mauricio Hernandez", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMaxNoResults() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MAX(Name) from sample8 where ID = 999");
		assertTrue(results.next());
		assertEquals("Incorrect max", null, results.getObject(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testMaxDate() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select max(d) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect max", "2010-03-28", results.getObject(1).toString());
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testMin() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MIN(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect min", 1, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testMinWhere() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select min(upper(name)) from sample8 where id < 3");
		assertTrue(results.next());
		assertEquals("Incorrect min", "JUAN PABLO MORALES", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	public void testMinMax() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MIN(ID), MAX(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect min", 1, results.getInt(1));
		assertEquals("Incorrect max", 6, results.getInt(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
}