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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class TestRandomFunction
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
	public void testRandomFunction() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT C1, RANDOM() FROM numeric"))
		{
			assertTrue(results.next());
			double random1 = results.getDouble(2);
			assertTrue(random1 >= 0.0 && random1 < 1.0);
			assertTrue(results.next());
			double random2 = results.getDouble(2);
			assertTrue(random2 >= 0.0 && random2 < 1.0);

			// Ensure that a second access returns the same value, not a new random value.
			double random3 = results.getDouble(2);
			assertEquals(random2, random3);
		}
	}

	@Test
	public void testRandomFunctionWithSeed() throws SQLException
	{
		long seed = 12345;
		Properties props = new Properties();
		props.put("randomSeed", Long.toString(seed));

		ArrayList<Double> random1 = new ArrayList<>();
		ArrayList<Double> random2 = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT C1, RANDOM() FROM numeric"))
		{
			while (results.next())
			{
				random1.add(Double.valueOf(results.getDouble(2)));
			}
			assertEquals(random1.size(), 2);
		}

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT C1, RANDOM() FROM numeric"))
		{
			while (results.next())
			{
				random2.add(Double.valueOf(results.getDouble(2)));
			}
			assertEquals(random2.size(), 2);
		}
		
		// Expect same sequence of random numbers when using same random seed.
		assertEquals(random1, random2);
	}

	@Test
	public void testOrderByRandomFunction() throws SQLException
	{
		HashSet<String> ids = new HashSet<>();
		ArrayList<Double> randoms = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT ID, RANDOM() R FROM sample ORDER BY R"))
		{
			while (results.next())
			{
				ids.add(String.valueOf(results.getString(1)));
				randoms.add(Double.valueOf(results.getDouble(2)));
			}

			// Expect each unique ID once.
			assertEquals(ids.size(), 6);

			// Expect random values in increasing order.
			assertEquals(randoms.size(), 6);
			for (int i = 1; i < randoms.size(); i++)
			{
				assertTrue(randoms.get(i) >= randoms.get(i - 1));
			}
		}
	}
}
