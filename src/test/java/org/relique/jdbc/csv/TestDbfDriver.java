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
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.relique.jdbc.dbf.DbfClassNotFoundException;

/**
 * This class is used to test the CsvJdbc driver.
 *
 * @author Mario Frasca
 */
public class TestDbfDriver
{
	private static String filePath;

	@BeforeAll
	public static void setUp()
	{
		filePath = ".." + File.separator + "src" + File.separator + "testdata";
		if (!new File(filePath).isDirectory())
			filePath = "src" + File.separator + "testdata";
		assertTrue(new File(filePath).isDirectory(), "Sample files directory not found: " + filePath);

		// load CSV driver
		try
		{
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		}
		catch (ClassNotFoundException e)
		{
			fail("Driver is not in the CLASSPATH -> " + e);
		}
	}

	/**
	 * This creates several sentences with where and tests they work
	 *
	 * @throws SQLException
	 */
	@Test
	public void testGetAll() throws SQLException
	{
		try
		{
			Properties props = new Properties();
			props.put("fileExtension", ".dbf");

			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement();

				ResultSet results = stmt
					.executeQuery("SELECT * FROM sample"))
			{
				assertTrue(results.next());
				assertEquals("Gianni", results.getString("Name"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Reinout", results.getString("Name"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Alex", results.getString("Name"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Gianni", results.getString("Name"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Mario", results.getString("Name"), "The name is wrong");
				assertFalse(results.next());
			}
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testWhereOp() throws SQLException
	{
		try
		{
			Properties props = new Properties();
			props.put("fileExtension", ".dbf");

			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement();

				ResultSet results = stmt
					.executeQuery("SELECT * FROM sample WHERE key = 'op'"))
			{
				assertTrue(results.next());
				assertEquals("Gianni", results.getString("Name"), "The name is wrong");
				assertEquals("debian", results.getString("value"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Reinout", results.getString("Name"), "The name is wrong");
				assertEquals("ubuntu", results.getString("value"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Alex", results.getString("Name"), "The name is wrong");
				assertEquals("windows", results.getString("value"), "The name is wrong");
				assertFalse(results.next());
			}
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testWhereTodo() throws SQLException
	{
		try
		{
			Properties props = new Properties();
			props.put("fileExtension", ".dbf");

			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement();

				ResultSet results = stmt
					.executeQuery("SELECT * FROM sample WHERE key = 'todo'"))
			{
				assertTrue(results.next());
				assertEquals("Gianni", results.getString("Name"), "The name is wrong");
				assertEquals("none", results.getString("value"), "The name is wrong");
				assertTrue(results.next());
				assertEquals("Mario", results.getString("Name"), "The name is wrong");
				assertEquals("sleep", results.getString("value"), "The name is wrong");
				assertFalse(results.next());
			}
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testWhereWithIsNull() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM fox_samp WHERE COWNNAME IS NULL"))
		{
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testTypedColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
					.executeQuery("SELECT * FROM fox_samp"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("fox_samp", metadata.getTableName(0), "Incorrect Table Name");
			assertEquals(57, metadata.getColumnCount(), "Incorrect Column Count");
			assertEquals(Types.VARCHAR, metadata.getColumnType(1), "Incorrect Column Type");
			assertEquals(Types.BOOLEAN, metadata.getColumnType(2), "Incorrect Column Type");
			assertEquals(Types.DOUBLE, metadata.getColumnType(3), "Incorrect Column Type");
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testMemoColumn() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT NOTE FROM xbase"))
		{
			assertTrue(results.next());
			assertEquals("This is a memo fore record no one", results.getString(1), "The NOTE is wrong");
			assertTrue(results.next());
			assertEquals("This is memo for record 2", results.getString(1), "The NOTE is wrong");
			assertTrue(results.next());
			assertEquals("This is memo 3", results.getString(1), "The NOTE is wrong");
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testFloatColumn() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM d"))
		{
			assertTrue(results.next());
			long l = Math.round(7.63 * 1000);
			assertEquals(l, Math.round(results.getDouble(1) * 1000), "The floatfield is wrong");
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testColumnDisplaySizes() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
					.executeQuery("SELECT * FROM fox_samp"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals(11, metadata.getColumnDisplaySize(1), "Incorrect Column Size");
			assertEquals(1, metadata.getColumnDisplaySize(2), "Incorrect Column Size");
			assertEquals(4, metadata.getColumnDisplaySize(3), "Incorrect Column Size");
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testDatabaseMetadataTables() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);
			ResultSet results = conn.getMetaData().getTables(null, null, "%", null))
		{
			Set<String> target = new HashSet<>();
			target.add("sample");
			target.add("fox_samp");
			target.add("hotel");
			target.add("xbase");
			target.add("d");

			Set<String> current = new HashSet<>();
			assertTrue(results.next());
			current.add(results.getString("TABLE_NAME"));
			assertTrue(results.next());
			current.add(results.getString("TABLE_NAME"));
			assertTrue(results.next());
			current.add(results.getString("TABLE_NAME"));
			assertTrue(results.next());
			current.add(results.getString("TABLE_NAME"));
			assertTrue(results.next());
			current.add(results.getString("TABLE_NAME"));
			assertFalse(results.next());

			assertEquals(target, current, "Incorrect table names");
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testDatabaseMetadataTablesPattern() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);
			// Get tables matching only this pattern
			ResultSet results = conn.getMetaData().getTables(null, null, "x%", new String[]{"TABLE"}))
		{
			assertTrue(results.next());
			assertEquals("xbase", results.getString("TABLE_NAME"), "Incorrect table name");
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testDatabaseMetadataColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);
			ResultSet results = conn.getMetaData().getColumns(null, null, "sample", "%"))
		{
			assertTrue(results.next());
			assertEquals("sample", results.getString("TABLE_NAME"), "Incorrect table name");
			assertEquals("NAME", results.getString("COLUMN_NAME"), "Incorrect column name");
			assertEquals(Types.VARCHAR, results.getInt("DATA_TYPE"), "Incorrect column type");
			assertEquals("String", results.getString("TYPE_NAME"), "Incorrect column type");
			assertEquals(1, results.getInt("ORDINAL_POSITION"), "Incorrect ordinal position");
			assertTrue(results.next());
			assertEquals("KEY", results.getString(4), "Incorrect column name");
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testDatabaseMetadataColumnsPattern() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			ResultSet results = conn.getMetaData().getColumns(null, null, "x%", "M%"))
		{
			assertTrue(results.next());
			assertEquals("xbase", results.getString("TABLE_NAME"), "Incorrect table name");
			assertEquals("MSG", results.getString("COLUMN_NAME"), "Incorrect column name");
			assertEquals(Types.VARCHAR, results.getInt("DATA_TYPE"), "Incorrect column type");
			assertEquals("String", results.getString("TYPE_NAME"), "Incorrect column type");
			assertEquals(2, results.getInt("ORDINAL_POSITION"), "Incorrect ordinal position");
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testGetNumeric() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM fox_samp"))
		{
			assertTrue(results.next());
			assertEquals(33, results.getByte("NCOUNTYCOD"), "The NCOUNTYCOD is wrong");
			assertEquals(33, results.getShort("NCOUNTYCOD"), "The NCOUNTYCOD is wrong");
			assertEquals(2011, results.getInt("NTAXYEAR"), "The NTAXYEAR is wrong");
			assertEquals(0, results.getLong("NNOTFCV"), "The NNOTFCV is wrong");
			assertEquals(7250, Math.round(results.getFloat("NASSASSRAT") * 1000), "The NASSASSRAT is wrong");
			assertEquals(7250, Math.round(results.getDouble("NASSASSRAT") * 1000), "The NASSASSRAT is wrong");
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testGetDate() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT DASSDATE FROM fox_samp"))
		{
			assertTrue(results.next());
			assertEquals(Date.valueOf("2012-12-25"), results.getDate(1), "The DASSDATE is wrong");
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testGetTimestamp() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT DASSDATE FROM fox_samp"))
		{
			assertTrue(results.next());
			assertEquals(Timestamp.valueOf("2012-12-25 00:00:00"),
					results.getTimestamp(1), "The DASSDATE is wrong");
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}

	@Test
	public void testCharset() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".dbf");
		props.put("charset", "ISO-8859-1");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT HOTELNAME FROM hotel"))
		{
			assertTrue(results.next());
			assertEquals("M\u00DCNCHEN HOTEL", results.getString(1), "The HOTELNAME is wrong");
			assertTrue(results.next());
			assertEquals("MALM\u00D6 INN", results.getString(1), "The HOTELNAME is wrong");
			assertTrue(results.next());
			assertEquals("K\u00D8BENHAVN HOTEL", results.getString(1), "The HOTELNAME is wrong");
			assertTrue(results.next());
			assertEquals("C\u00F3rdoba Hotel", results.getString(1), "The HOTELNAME is wrong");
			assertFalse(results.next());
		}
		catch (DbfClassNotFoundException e)
		{
			/*
			 * Skip test if classes for reading DBF files are not available.
			 */
		}
	}
}
