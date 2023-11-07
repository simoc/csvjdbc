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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestDoubleQuoting
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

	@Test
	public void testQuotedTableName() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT * FROM \"C D\""))
		{
			assertTrue(rs1.next());
		}
	}

	@Test
	public void testQuotedColumnNames() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,Integer,Integer,Integer,Integer");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT \"A\", \"B\" + 100, \"[A]\", \"A-B\", \"A B\" FROM \"C D\""))
		{
			assertEquals("A", rs1.getMetaData().getColumnName(1), "Column name A is wrong");

			assertEquals("[A]", rs1.getMetaData().getColumnName(3), "Column name [A] is wrong");
			assertEquals("A-B", rs1.getMetaData().getColumnName(4), "Column name A-B is wrong");
			assertEquals("A B", rs1.getMetaData().getColumnName(5), "Column name A B is wrong");
			assertTrue(rs1.next());
			assertEquals(1, rs1.getInt("A"), "The A is wrong");
			assertEquals(3, rs1.getInt("[A]"), "The [A] is wrong");
			assertEquals(4, rs1.getInt("A-B"), "The A-B is wrong");
			assertEquals(5, rs1.getInt("A B"), "The A-B is wrong");
			assertTrue(rs1.next());
			assertEquals(6, rs1.getInt(1), "The A is wrong");
			assertEquals(7 + 100, rs1.getInt(2), "The B + 100 is wrong");
			assertEquals(8, rs1.getInt(3), "The [A] is wrong");
			assertEquals(9, rs1.getInt(4), "The A-B is wrong");
			assertEquals(10, rs1.getInt(5), "The A-B is wrong");
		}
	}

	@Test
	public void testQuotedTableAlias() throws SQLException
	{
		Properties props = new Properties();
		String alias = "\"http://www.google.com\"";

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();


			ResultSet rs1 = stmt.executeQuery("SELECT " + alias + ".ID, " + alias + ".\"EXTRA_FIELD\" " +
				"FROM sample AS " + alias))
		{
			assertTrue(rs1.next());
			assertEquals("Q123", rs1.getString(1), "The ID is wrong");
			assertEquals("F", rs1.getString(2), "The EXTRA_FIELD is wrong");
		}
	}
}
