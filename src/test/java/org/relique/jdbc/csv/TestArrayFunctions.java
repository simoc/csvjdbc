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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.sql.*;

import static org.junit.Assert.*;

public class TestArrayFunctions
{
	public static String filePath;

	@BeforeClass
	public static void setUp()
	{
		filePath = ".." + File.separator + "src" + File.separator + "testdata";
		if (!new File(filePath).isDirectory())
			filePath = "src" + File.separator + "testdata";
		assertTrue("Sample files directory not found: " + filePath, new File(filePath).isDirectory());

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
	public void testToArrayWithStringData() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(role_1, role_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			assertEquals("Incorrect row", "alice", results.getString(1));
			Array array = results.getArray(2);
			assertEquals("Not an array of strings", "String", array.getBaseTypeName());
			assertEquals("Not an array of varchar", Types.VARCHAR, array.getBaseType());
			Object[] data = (Object[]) array.getArray();
			assertEquals(2, data.length);
			assertEquals("teacher", data[0]);
			assertEquals("grader", data[1]);

			assertTrue(results.next());
			assertEquals("Incorrect row", "bob", results.getString(1));
			array = results.getArray(2);
			assertEquals("Not an array of strings", "String", array.getBaseTypeName());
			data = (Object []) array.getArray();
			assertEquals(2, data.length);
			assertEquals("", data[0]);
			assertEquals("admin", data[1]);
		}
	}

	@Test
	public void testToArrayWithIntegerData() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name,TO_ARRAY(role_id_1, role_id_2) AS roles FROM arrays_sample"))
		{
			assertTrue(results.next());
			assertEquals("Incorrect row", "alice", results.getString(1));
			Array array = results.getArray(2);
			assertEquals("Not an array of integers", "Int", array.getBaseTypeName());
			assertEquals("Not an array of integer", Types.INTEGER, array.getBaseType());
			Object[] data = (Object[]) array.getArray();
			assertEquals(2, data.length);
			assertEquals(31, data[0]);
			assertEquals(76, data[1]);

			assertTrue(results.next());
			assertEquals("Incorrect row", "bob", results.getString(1));
			array = results.getArray(2);
			assertEquals("Not an array of integers", "Int", array.getBaseTypeName());
			data = (Object []) array.getArray();
			assertEquals(2, data.length);
			assertNull(data[0]);
			assertEquals(1, data[1]);
		}
	}

	@Test
	public void testToArrayWithDistinct() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT name, TO_ARRAY(DISTINCT role_1, role_2) AS roles FROM arrays_sample")) {
			assertTrue(results.next());
			assertEquals("Incorrect row", "alice", results.getString(1));
			Object[] data = (Object[]) results.getArray(2).getArray();
			assertEquals(2, data.length);
			assertEquals("teacher", data[0]);
			assertEquals("grader", data[1]);

			assertTrue(results.next());
			assertTrue(results.next());
			assertEquals("Incorrect row", "eve", results.getString(1));
			data = (Object[]) results.getArray(2).getArray();
			assertEquals(1, data.length);
			assertEquals("teacher", data[0]);
		}
	}
}
