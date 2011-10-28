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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.TestCase;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Mario Frasca
 * @version $Id: TestDbfDriver.java,v 1.3 2011/10/28 19:03:28 simoc Exp $
 */
public class TestDbfDriver extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;
	private DateFormat toUTC;

	public TestDbfDriver(String name) {
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

	/**
	 * This creates several sentences with where and tests they work
	 * 
	 * @throws SQLException
	 */
	public void testGetAll() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample");
		assertTrue(results.next());
		assertEquals("The name is wrong", "Gianni", results
				.getString("Name"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Reinout", results
				.getString("Name"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Alex", results
				.getString("Name"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Gianni", results
				.getString("Name"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Mario", results
				.getString("Name"));
		assertFalse(results.next());
	}

	public void testWhereOp() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample WHERE key = 'op'");
		assertTrue(results.next());
		assertEquals("The name is wrong", "Gianni", results
				.getString("Name"));
		assertEquals("The name is wrong", "debian", results
				.getString("value"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Reinout", results
				.getString("Name"));
		assertEquals("The name is wrong", "ubuntu", results
				.getString("value"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Alex", results
				.getString("Name"));
		assertEquals("The name is wrong", "windows", results
				.getString("value"));
		assertFalse(results.next());
	}

	public void testWhereTodo() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample WHERE key = 'todo'");
		assertTrue(results.next());
		assertEquals("The name is wrong", "Gianni", results
				.getString("Name"));
		assertEquals("The name is wrong", "none", results
				.getString("value"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Mario", results
				.getString("Name"));
		assertEquals("The name is wrong", "sleep", results
				.getString("value"));
		assertFalse(results.next());
	}

	public void testTypedColumns() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM fox_samp");
		assertTrue(results.next());
		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("Incorrect Table Name", "fox_samp", metadata
				.getTableName(0));
		assertEquals("Incorrect Column Count", 57, metadata.getColumnCount());
		assertEquals("Incorrect Column Type", Types.VARCHAR, metadata.getColumnType(1));
		assertEquals("Incorrect Column Type", Types.BOOLEAN, metadata.getColumnType(2));
		assertEquals("Incorrect Column Type", Types.DOUBLE, metadata.getColumnType(3));
	}
	

}