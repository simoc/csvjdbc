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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests SQL LIMIT OFFSET keywords in the CsvJdbc driver.
 */
public class TestLimitOffset
{
	private static String filePath;
	private static DateFormat toUTC;

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
		toUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		toUTC.setTimeZone(TimeZone.getTimeZone("UTC"));  
	}

	@Test
	public void testLimitRows() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 limit 4");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 1, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testLimitHigherThanRowCount() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample4 limit 999");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 1, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 4, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testLimitWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 where id > 6 limit 3");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 7, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testLimitOrderBy() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id, name from sample5 order by id desc limit 2");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 9, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testOffsetRows() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 limit 9 offset 8");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 9, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testOffsetNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 limit 99 offset 99");
		assertFalse(results.next());
	}

	@Test
	public void testOffsetWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 where id > 2 limit 3 offset 4");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 6, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 7, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testOffsetOrderBy() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select * from sample5 order by id limit 99 offset 5");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 6, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 7, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 8, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 9, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 41, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testBadOffset() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		try
		{
			stmt.executeQuery("select * from sample5 limit 5 offset -1");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertTrue(e.toString().startsWith("java.sql.SQLException: " + CsvResources.getString("syntaxError")));
		}
	}
}
