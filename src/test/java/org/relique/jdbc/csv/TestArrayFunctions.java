/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2023  Peter Fokkinga

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;
import java.util.Properties;

public class TestArrayFunctions
{
	public static String filePath;

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

	@Test
	public void testToArrayColumnType() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			ResultSetMetaData meta = results.getMetaData();
			int type = meta.getColumnType(2);
			assertEquals(Types.ARRAY, type);
		}
	}

	@Test
	public void testToArrayWithStringData() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			assertEquals("alice", results.getString(1), "Incorrect row");
			Array array = results.getArray(2);
			assertEquals("String", array.getBaseTypeName(), "Not an array of strings");
			assertEquals(Types.VARCHAR, array.getBaseType(), "Not an array of varchar");
			Object[] data = (Object[]) array.getArray();
			assertEquals(2, data.length);
			assertEquals("teacher", data[0]);
			assertEquals("grader", data[1]);

			assertTrue(results.next());
			assertEquals("bob", results.getString(1), "Incorrect row");
			array = results.getArray(2);
			assertEquals("String", array.getBaseTypeName(), "Not an array of strings");
			data = (Object []) array.getArray();
			assertEquals(2, data.length);
			assertEquals("", data[0]);
			assertEquals("admin", data[1]);
		}
	}

	@Test
	public void testToArrayWithIntegerData() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,String,String,Int,Int");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name,TO_ARRAY(role_id_1, role_id_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			assertEquals("alice", results.getString(1), "Incorrect row");
			Array array = results.getArray(2);
			assertEquals("Int", array.getBaseTypeName(), "Not an array of integers");
			assertEquals(Types.INTEGER, array.getBaseType(), "Not an array of integer");
			Object[] data = (Object[]) array.getArray();
			assertEquals(2, data.length);
			assertEquals(31, data[0]);
			assertEquals(76, data[1]);

			assertTrue(results.next());
			assertEquals("bob", results.getString(1), "Incorrect row");
			array = results.getArray(2);
			assertEquals("Int", array.getBaseTypeName(), "Not an array of integers");
			data = (Object []) array.getArray();
			assertEquals(2, data.length);
			assertNull(data[0]);
			assertEquals(1, data[1]);
		}
	}

	@Test
	public void testToArrayWithDateData() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(PurchaseDate, PurchaseDate + 28) AS date_range FROM Purchase"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			assertEquals("Date", array.getBaseTypeName(), "Not an array of dates");
			assertEquals(Types.DATE, array.getBaseType(), "Not an array of dates");
			ResultSet arrayResults = array.getResultSet();
			assertTrue(arrayResults.next());
			assertEquals(Date.valueOf("2013-01-09"), arrayResults.getDate(2));
			assertTrue(arrayResults.next());
			assertEquals(Date.valueOf("2013-02-06"), arrayResults.getDate(2));
			array.free();
			assertTrue(results.next());
		}
	}

	@Test
	public void testToArrayWithDistinct() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(DISTINCT role_1, role_2) AS roles FROM arrays_sample")) {
			assertTrue(results.next());
			assertEquals("alice", results.getString(1), "Incorrect row");
			Object[] data = (Object[]) results.getArray(2).getArray();
			assertEquals(2, data.length);
			assertEquals("teacher", data[0]);
			assertEquals("grader", data[1]);

			assertTrue(results.next());
			assertTrue(results.next());
			assertEquals("eve", results.getString(1), "Incorrect row");
			data = (Object[]) results.getArray(2).getArray();
			assertEquals(1, data.length);
			assertEquals("teacher", data[0]);
		}
	}

	@Test
	public void testFreedArray() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(DISTINCT role_1, role_2) AS roles FROM arrays_sample")) {
			assertTrue(results.next());
			Array array = results.getArray(1);
			Object[] data = (Object[]) array.getArray();
			assertEquals(2, data.length);
			assertEquals("teacher", data[0]);
			assertEquals("grader", data[1]);
			array.free();

			try
			{
				array.getArray();
				fail("Using freed Array should throw SQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("freedArray"), "" + e);
			}
		}
	}

	@Test
	public void testToArrayWithSubList() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(name, role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			Object[] data = (Object[]) array.getArray(2, 2);
			assertEquals(2, data.length);
			assertEquals("teacher", data[0]);
			assertEquals("grader", data[1]);
		}
	}

	@Test
	public void testToArrayWithSubListOutOfBounds() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(name, role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			Object[] data = (Object[]) array.getArray(1, 0);
			assertEquals(0, data.length);

			try
			{
				array.getArray(5, 2);
				fail("Array index out of bounds should throw SQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.toString().startsWith("java.sql.SQLException: " +
					CsvResources.getString("arraySubListOutOfBounds")));
			}

			try
			{
				array.getArray(-1, 2);
				fail("Array index out of bounds should throw SQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.toString().startsWith("java.sql.SQLException: " +
					CsvResources.getString("arraySubListOutOfBounds")));
			}
		}
	}

	@Test
	public void testToArrayWithResultSet() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			ResultSet arrayResults = array.getResultSet();
			assertEquals(2, arrayResults.getMetaData().getColumnCount(), "Incorrect column count");
			assertTrue(arrayResults.next());
			assertEquals(1, arrayResults.getInt(1));
			assertEquals("teacher", arrayResults.getString(2));
			assertTrue(arrayResults.next());
			assertEquals(2, arrayResults.getInt(1));
			assertEquals("grader", arrayResults.getString(2));
			assertFalse(arrayResults.next());
			arrayResults.close();

			assertTrue(results.next());
			array = results.getArray(1);
			arrayResults = array.getResultSet();
			assertEquals(2, arrayResults.getMetaData().getColumnCount(), "Incorrect column count");
			assertTrue(arrayResults.next());
			assertEquals(1, arrayResults.getInt(1));
			assertEquals("", arrayResults.getString(2));
			assertTrue(arrayResults.next());
			assertEquals(2, arrayResults.getInt(1));
			assertEquals("admin", arrayResults.getString(2));
			assertFalse(arrayResults.next());
			arrayResults.close();
		}
	}

	@Test
	public void testToArrayWithResultSetWithSubList() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(name, role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			ResultSet arrayResults = array.getResultSet(2, 2);
			assertTrue(arrayResults.next());
			assertEquals("teacher", arrayResults.getString(2));
			assertTrue(arrayResults.next());
			assertEquals("grader", arrayResults.getString(2));
			assertFalse(arrayResults.next());
			arrayResults.close();
		}
	}

	@Test
	public void testToArrayWithResultSetWithSubListOutOfBounds() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(name, role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			ResultSet arrayResults = array.getResultSet(1, 0);
			assertFalse(arrayResults.next());

			try
			{
				array.getResultSet(5, 2);
				fail("Array index out of bounds should throw SQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.toString().startsWith("java.sql.SQLException: " +
					CsvResources.getString("arraySubListOutOfBounds")));
			}

			try
			{
				array.getResultSet(-1, 2);
				fail("Array index out of bounds should throw SQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.toString().startsWith("java.sql.SQLException: " +
					CsvResources.getString("arraySubListOutOfBounds")));
			}
		}
	}

	@Test
	public void testToArrayWithWhere() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample " +
			 "WHERE roles = TO_ARRAY('', 'admin')"))
		{
			assertTrue(results.next());
			assertEquals("bob", results.getString(1), "Incorrect row");
			assertFalse(results.next());
		}
	}

	@Test
	public void testToArrayNumericWithWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,Integer,Integer,Long,Float,Double,BigDecimal");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_ARRAY(C1, C2, C3) AS c FROM numeric " +
			 "WHERE c = TO_ARRAY(-22, 15, 2147483647)"))
		{
			assertTrue(results.next());
			Array array = results.getArray(1);
			Object[] data = (Object[]) array.getArray();
			assertEquals(3, data.length);
			assertEquals(Integer.valueOf(-22), data[0]);
			assertEquals(Integer.valueOf(15), data[1]);
			assertEquals(Integer.valueOf(2147483647), data[2]);
			array.free();
			assertFalse(results.next());
		}
	}

	@Test
	public void testToArrayWithWhereWrongColumnType() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample " +
			 "WHERE roles = 66"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testToArrayWithOrderBy() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample " +
			 "ORDER BY roles"))
		{
			assertTrue(results.next());
			assertEquals("bob", results.getString(1), "Incorrect row");
			assertTrue(results.next());
			assertEquals("eve", results.getString(1), "Incorrect row");

			Array array = results.getArray(2);
			ResultSet arrayResults = array.getResultSet();
			assertTrue(arrayResults.next());
			assertEquals("", arrayResults.getString(2));
			assertTrue(arrayResults.next());
			assertEquals("teacher", arrayResults.getString(2));
			assertFalse(arrayResults.next());

			assertTrue(results.next());
			assertEquals("alice", results.getString(1), "Incorrect row");
			assertTrue(results.next());
			assertEquals("eve", results.getString(1), "Incorrect row");

			array = results.getArray(2);
			arrayResults = array.getResultSet();
			assertTrue(arrayResults.next());
			assertEquals("teacher", arrayResults.getString(2));
			assertTrue(arrayResults.next());
			assertEquals("teacher", arrayResults.getString(2));
			assertFalse(arrayResults.next());

			assertFalse(results.next());
		}
	}
}
