package org.relique.jdbc.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

/**
 * Tests reading of database tables from classpath resources.
 */
public class TestClasspathResources
{
	@Test
	public void testConnectionName() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata"))
		{
			String url = conn.getMetaData().getURL();
			assertEquals("jdbc:relique:csv:classpath:testdata", url);
		}
	}

	@Test
	public void testSelect() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Short,String,String,Short,Short,Short");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata/olympic-medals",
				props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM medals2004"))
		{
			assertTrue(results.next());
			assertEquals(2004, results.getShort(1), "The YEAR is wrong");
			assertEquals("United States", results.getString(2), "The COUNTRY is wrong");
			assertEquals("USA", results.getString(3), "The CODE is wrong");
			assertEquals(36, results.getShort(4), "The GOLD is wrong");
			assertTrue(results.next());
			assertEquals(2004, results.getShort(1), "The YEAR is wrong");
			assertEquals("China", results.getString(2), "The COUNTRY is wrong");
			assertEquals("CHN", results.getString(3), "The CODE is wrong");
			assertEquals(32, results.getShort(4), "The GOLD is wrong");
		}
	}

	@Test
	public void testListTables() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata/olympic-medals");
			ResultSet results = conn.getMetaData().getTables(null, null, "%", null))
		{
			assertTrue(results.next());
			assertEquals("medals2004", results.getString("TABLE_NAME"), "The TABLE_NAME is wrong");
			assertTrue(results.next());
			assertEquals("medals2008", results.getString("TABLE_NAME"), "The TABLE_NAME is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testListTablesOfEmptyResourcePath() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata");
				ResultSet results = conn.getMetaData().getTables(null, null, "%", null))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testBadTableNameFails() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata/olympic-medals");
			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM abc"))
			{
				fail("Query should fail");
			}
			catch (SQLException e)
			{
				String message = "" + e;
				assertTrue(message.startsWith("java.sql.SQLException: " + CsvResources.getString("tableNotFound") + ":"));
			}
		}
	}

	@Test
	public void testCharsetISO8859_1() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String");
		props.put("charset", "ISO-8859-1");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata/encodings", props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM iso8859-1"))
		{
			assertTrue(results.next());
			assertEquals("K\u00D8BENHAVN", results.getString(1), "ISO8859-1 encoding is wrong");
			assertTrue(results.next());
			assertEquals("100\u00B0", results.getString(1), "ISO8859-1 encoding is wrong");
			assertTrue(results.next());
			assertEquals("\u00A9 Copyright", results.getString(1), "ISO8859-1 encoding is wrong");
		}
	}

	@Test
	public void testCharsetUTF_8() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String");
		props.put("charset", "UTF-8");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata/encodings", props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM utf-8"))
		{
			assertTrue(results.next());
			assertEquals("K\u00D8BENHAVN", results.getString(1), "UTF-8 encoding is wrong");
			assertTrue(results.next());
			assertEquals("100\u00B0", results.getString(1), "UTF-8 encoding is wrong");
			assertTrue(results.next());
			assertEquals("\u00A9 Copyright", results.getString(1), "UTF-8 encoding is wrong");
		}
	}

	@Test
	public void testCharsetUTF_16() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String");
		props.put("charset", "UTF-16");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:testdata/encodings", props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM utf-16"))
		{
			assertTrue(results.next());
			assertEquals("K\u00D8BENHAVN", results.getString(1), "UTF-16 encoding is wrong");
			assertTrue(results.next());
			assertEquals("100\u00B0", results.getString(1), "UTF-16 encoding is wrong");
			assertTrue(results.next());
			assertEquals("\u00A9 Copyright", results.getString(1), "UTF-16 encoding is wrong");
		}
	}
}
