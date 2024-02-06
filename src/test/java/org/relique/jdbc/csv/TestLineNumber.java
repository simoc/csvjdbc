/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2023  Simon Chenery

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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests SQL LIMIT OFFSET keywords in the CsvJdbc driver.
 */
public class TestLineNumber
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
	public void testLineNumbersSimple() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT LINE_NUMBER(), NAME FROM sample"))
		{
			for (int i = 1; i <= 6; i++)
			{
				assertTrue(results.next());
				assertEquals(i, results.getInt(1));
			}
			assertFalse(results.next());
		}
	}

	@Test
	public void testLineNumbersWhere() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT LINE_NUMBER(), NAME FROM sample WHERE ID = 'C456'"))
		{
			assertTrue(results.next());
			assertEquals(4, results.getInt(1));

			assertFalse(results.next());
		}
	}

	@Test
	public void testLineNumbersOrderBy() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT LINE_NUMBER(), ID FROM sample ORDER BY ID"))
		{
			assertTrue(results.next());
			assertEquals(2, results.getInt(1));
			assertEquals("A123", results.getString(2));

			assertTrue(results.next());
			assertEquals(3, results.getInt(1));
			assertEquals("B234", results.getString(2));

			assertTrue(results.next());
			assertEquals(4, results.getInt(1));
			assertEquals("C456", results.getString(2));

			assertTrue(results.next());
			assertEquals(5, results.getInt(1));
			assertEquals("D789", results.getString(2));

			assertTrue(results.next());
			assertEquals(1, results.getInt(1));
			assertEquals("Q123", results.getString(2));

			assertTrue(results.next());
			assertEquals(6, results.getInt(1));
			assertEquals("X234", results.getString(2));

			assertFalse(results.next());
		}
	}

	@Test
	public void testLineNumbersOffset() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT LINE_NUMBER(), ID FROM sample LIMIT 9 OFFSET 4"))
		{
			assertTrue(results.next());
			assertEquals(5, results.getInt(1));

			assertTrue(results.next());
			assertEquals(6, results.getInt(1));

			assertFalse(results.next());
		}
	}

	@Test
	public void testLineNumbersAggregateFunction() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT MAX(ID), LINE_NUMBER() FROM sample"))
		{
			assertTrue(results.next());
			assertEquals(0, results.getInt(2));

			assertFalse(results.next());
		}
	}

	@Test
	public void testLineNumbersScrollable() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			ResultSet results = stmt
				.executeQuery("SELECT ID, LINE_NUMBER() FROM sample"))
		{
			assertTrue(results.last());
			assertEquals("X234", results.getString(1));
			assertEquals(6, results.getInt(2));

			assertTrue(results.previous());
			assertEquals("D789", results.getString(1));
			assertEquals(5, results.getInt(2));

			assertTrue(results.first());
			assertEquals("Q123", results.getString(1));
			assertEquals(1, results.getInt(2));

			assertTrue(results.next());
			assertEquals("A123", results.getString(1));
			assertEquals(2, results.getInt(2));
		}
	}
}
