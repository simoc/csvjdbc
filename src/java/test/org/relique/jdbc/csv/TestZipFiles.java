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

import java.io.File;
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
 * Tests reading of database tables from ZIP files.
 */
public class TestZipFiles extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;
	private DateFormat toUTC;
	private static String TEST_ZIP_FILENAME_1 = "olympic-medals.zip";
	private static String TEST_ZIP_FILENAME_2 = "encodings.zip";

	public TestZipFiles(String name) {
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

	public void testConnectionName() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_1);
		String url = conn.getMetaData().getURL();
		assertTrue(url.startsWith("jdbc:relique:csv:zip:"));
		assertTrue(url.endsWith(TEST_ZIP_FILENAME_1));
	}

	/**
	 * @throws SQLException
	 */
	public void testSelect() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Short,String,String,Short,Short,Short");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_1, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM medals2004");
		assertTrue(results.next());
		assertEquals("The YEAR is wrong", 2004, results.getShort(1));
		assertEquals("The COUNTRY is wrong", "United States", results.getString(2));
		assertEquals("The CODE is wrong", "USA", results.getString(3));
		assertEquals("The GOLD is wrong", 36, results.getShort(4));
		assertTrue(results.next());
		assertEquals("The YEAR is wrong", 2004, results.getShort(1));
		assertEquals("The COUNTRY is wrong", "China", results.getString(2));
		assertEquals("The CODE is wrong", "CHN", results.getString(3));
		assertEquals("The GOLD is wrong", 32, results.getShort(4));
	}
	
	/**
	 * @throws SQLException
	 */
	public void testListTables() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_1);

		ResultSet results = conn.getMetaData().getTables(null, null, "*", null);
		assertTrue(results.next());
		assertEquals("The TABLE_NAME is wrong", "medals2004", results.getString("TABLE_NAME"));
		assertTrue(results.next());
		assertEquals("The TABLE_NAME is wrong", "medals2008", results.getString("TABLE_NAME"));
		assertFalse(results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testBadTableNameFails() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_1);

		Statement stmt = conn.createStatement();

		try {
			stmt.executeQuery("SELECT * FROM abc");
			fail("Query should fail");
		} catch (SQLException e) {
			String message = "" + e;
			assertTrue(message.startsWith("java.sql.SQLException: Table not found:"));
		}
	}

	/**
	 * @throws SQLException
	 */
	public void testBadZipFileFails() throws SQLException {
		try {
			DriverManager.getConnection("jdbc:relique:csv:zip:"
				+ filePath + File.separator + "abc" + TEST_ZIP_FILENAME_1);
			fail("Connection should fail");
		} catch (SQLException e) {
			String message = "" + e;
			assertTrue(message.startsWith("java.sql.SQLException: Failed opening ZIP file:"));
		}
	}
	
	public void testCharsetISO8859_1() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "String");
		props.put("charset", "ISO-8859-1");
		props.put("fileExtension", ".txt");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_2, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM iso8859-1");
		assertTrue(results.next());
		assertEquals("ISO8859-1 encoding is wrong", "K\u00D8BENHAVN", results.getString(1));
		assertTrue(results.next());
		assertEquals("ISO8859-1 encoding is wrong", "100\u00B0", results.getString(1));
		assertTrue(results.next());
		assertEquals("ISO8859-1 encoding is wrong", "\u00A9 Copyright", results.getString(1));
	}
	
	public void testCharsetUTF_8() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "String");
		props.put("charset", "UTF-8");
		props.put("fileExtension", ".txt");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_2, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM utf-8");
		assertTrue(results.next());
		assertEquals("UTF-8 encoding is wrong", "K\u00D8BENHAVN", results.getString(1));
		assertTrue(results.next());
		assertEquals("UTF-8 encoding is wrong", "100\u00B0", results.getString(1));
		assertTrue(results.next());
		assertEquals("UTF-8 encoding is wrong", "\u00A9 Copyright", results.getString(1));
	}

	public void testCharsetUTF_16() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "String");
		props.put("charset", "UTF-16");
		props.put("fileExtension", ".txt");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:"
			+ filePath + File.separator + TEST_ZIP_FILENAME_2, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM utf-16");
		assertTrue(results.next());
		assertEquals("UTF-16 encoding is wrong", "K\u00D8BENHAVN", results.getString(1));
		assertTrue(results.next());
		assertEquals("UTF-16 encoding is wrong", "100\u00B0", results.getString(1));
		assertTrue(results.next());
		assertEquals("UTF-16 encoding is wrong", "\u00A9 Copyright", results.getString(1));
	}
}