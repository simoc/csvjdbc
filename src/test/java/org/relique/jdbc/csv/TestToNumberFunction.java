/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2024 Simon Chenery

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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;
import java.util.Locale;
import java.util.Properties;

public class TestToNumberFunction
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

	/**
	 * Compare two values for near equality, allowing for floating point round-off.
	 */
	private boolean fuzzyEquals(double a, double b)
	{
		return (a == b || Math.round(a * 1000) == Math.round(b * 1000));
	}

	@Test
	public void testToNumberFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("locale", Locale.GERMANY.toString());
		props.put("separator", "|");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_NUMBER(N, '000,000.00;(000,000.00)') FROM numbers_de"))
		{
			assertTrue(results.next());
			double n1 = results.getDouble(1);
			assertTrue(fuzzyEquals(n1, 3));

			assertTrue(results.next());
			double n2 = results.getDouble(1);
			assertTrue(fuzzyEquals(n2, 1234.56));

			Object obj2 = results.getObject(1);
			assertTrue(obj2 instanceof Number);

			assertTrue(results.next());
			double n3 = results.getDouble(1);
			assertTrue(fuzzyEquals(n3, -45678.90));

			assertTrue(results.next());
			double n4 = results.getDouble(1);
			assertTrue(fuzzyEquals(n4, 45.678));
		}
	}

	@Test
	public void testToNumberFunctionNotNumeric() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_NUMBER(ID, '000.00') FROM sample"))
		{
			assertTrue(results.next());
			double n1 = results.getDouble(1);
			assertTrue(fuzzyEquals(n1, 0));
			// Expect null because ID cannot be parsed to a string
			assertTrue(results.wasNull());
		}
	}

	@Test
	public void testToNumberFunctionPartiallyNumeric() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_NUMBER(T, '000.00') FROM yogesh"))
		{
			assertTrue(results.next());
			double n1 = results.getDouble(1);
			assertTrue(fuzzyEquals(n1, 0));
			// Expect null because timestamp T cannot be fully parsed to a number
			assertTrue(results.wasNull());
		}
	}

	@Test
	public void testToNumberFunctionEmptyString() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT TO_NUMBER('', '000.00') FROM sample"))
		{
			assertTrue(results.next());
			double n1 = results.getDouble(1);
			assertTrue(fuzzyEquals(n1, 0));
			// Expect null because empty string cannot be parsed
			assertTrue(results.wasNull());
		}
	}
}
