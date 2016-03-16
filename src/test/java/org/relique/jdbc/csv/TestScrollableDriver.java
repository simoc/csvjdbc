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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class is used to test the CsvJdbc Scrollable driver.
 * 
 * @author Chetan Gupta
 */
public class TestScrollableDriver
{
	private static String filePath;

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
	public void testScroll() throws SQLException
	{
		// create a connection. The first command line parameter is assumed to
		// be the directory in which the .csv files are held
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		// create a Statement object to execute the query with
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			ResultSet.CONCUR_READ_ONLY);

		// Select the ID and NAME columns from sample.csv
		ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample");

		// dump out the results
		results.next();
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));
		assertEquals("incorrect row #", 1, results.getRow());

		results.next();
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Jonathan Ackerman", results
				.getString("NAME"));
		assertEquals("incorrect row #", 2, results.getRow());

		results.previous();
		assertEquals("incorrect row #", 1, results.getRow());
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));

		results.first();
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));
		assertEquals("incorrect row #", 1, results.getRow());

		assertFalse(results.previous());
		assertEquals("incorrect row #", 0, results.getRow());
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		results.next();
		assertEquals("incorrect row #", 1, results.getRow());
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));

		results.next();
		assertEquals("incorrect row #", 2, results.getRow());
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Jonathan Ackerman", results
				.getString("NAME"));

		results.absolute(1);
		assertEquals("incorrect row #", 1, results.getRow());
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));

		results.absolute(2);
		assertEquals("incorrect row #", 2, results.getRow());
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Jonathan Ackerman", results
				.getString("NAME"));

		results.relative(2);
		assertEquals("incorrect row #", 4, results.getRow());
		assertEquals("Incorrect ID Value", "C456", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Susan, Peter and Dave", results
				.getString("NAME"));

		results.relative(-3);
		assertEquals("incorrect row #", 1, results.getRow());
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));

		results.relative(0);
		assertEquals("incorrect row #", 1, results.getRow());
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", results.getString("NAME"),
				"\"S,\"");

		results.last();
		assertEquals("incorrect row #", 6, results.getRow());
		assertEquals("Incorrect ID Value", "X234", results.getString("ID"));
		assertEquals("Incorrect NAME Value",
				"Peter \"peg leg\", Jimmy & Samantha \"Sam\"", results
						.getString("NAME"));

		results.absolute(-1);
		assertEquals("incorrect row #", 6, results.getRow());
		assertEquals("Incorrect ID Value", "X234", results.getString("ID"));
		assertEquals("Incorrect NAME Value",
				"Peter \"peg leg\", Jimmy & Samantha \"Sam\"", results
						.getString("NAME"));

		results.absolute(-2);
		assertEquals("incorrect row #", 5, results.getRow());
		assertEquals("Incorrect ID Value", "D789", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Amelia \"meals\" Maurice",
				results.getString("NAME"));

		results.last();
		results.next();
		assertNull(results.getString("ID"));
		assertNull(results.getString("NAME"));

		results.previous();
		assertEquals("Incorrect ID Value", "X234", results.getString("ID"));
		assertEquals("Incorrect NAME Value",
				"Peter \"peg leg\", Jimmy & Samantha \"Sam\"", results
						.getString("NAME"));

		assertFalse(results.relative(100));
		assertEquals("incorrect row #", 7, results.getRow());
		assertNull(results.getString("ID"));
		assertNull(results.getString("NAME"));

		assertFalse(results.relative(-100));
		assertEquals("incorrect row #", 0, results.getRow());
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		results.relative(2);
		assertEquals("incorrect row #", 2, results.getRow());
		assertEquals("Incorrect ID Value", results.getString("ID"), "A123");
		assertEquals("Incorrect NAME Value", results.getString("NAME"),
				"Jonathan Ackerman");

		results.absolute(7);
		assertEquals("incorrect row #", 7, results.getRow());
		assertNull(results.getString("ID"));
		assertNull(results.getString("NAME"));

		assertFalse(results.absolute(-10));
		assertEquals("incorrect row #", 0, results.getRow());
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		results.absolute(2);
		assertEquals("Incorrect ID Value", results.getString("ID"), "A123");
		assertEquals("Incorrect NAME Value", results.getString("NAME"),
				"Jonathan Ackerman");

		assertFalse(results.absolute(0));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		assertFalse(results.relative(0));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		// clean up
		results.close();
		stmt.close();
		conn.close();

	}

	@Test
	public void testIsFirstIsLast() throws SQLException
	{
		// create a connection. The first command line parameter is assumed to
		// be the directory in which the .csv files are held
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		// create a Statement object to execute the query with
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			ResultSet.CONCUR_READ_ONLY);

		// Select the ID and NAME columns from sample.csv
		ResultSet results = stmt.executeQuery("SELECT ID, NAME FROM sample");

		// dump out the results
		results.next();
		assertEquals("incorrect row #", 1, results.getRow());
		assertTrue(results.isFirst());
		assertFalse(results.isLast());
		assertFalse(results.isBeforeFirst());
		assertFalse(results.isAfterLast());

		results.next();
		assertEquals("incorrect row #", 2, results.getRow());
		assertFalse(results.isFirst());
		assertFalse(results.isLast());

		results.previous();
		assertEquals("incorrect row #", 1, results.getRow());
		assertTrue(results.isFirst());
		assertFalse(results.isLast());

		results.first();
		assertEquals("incorrect row #", 1, results.getRow());
		assertTrue(results.isFirst());
		assertFalse(results.isLast());

		results.previous();
		assertEquals("incorrect row #", 0, results.getRow());
		assertTrue(results.isBeforeFirst());
		assertFalse(results.isAfterLast());
		assertFalse(results.isFirst());
		assertFalse(results.isLast());

		results.next();
		assertEquals("incorrect row #", 1, results.getRow());
		assertTrue(results.isFirst());
		assertFalse(results.isLast());

		results.last();
		assertEquals("incorrect row #", 6, results.getRow());
		assertFalse(results.isFirst());
		assertTrue(results.isLast());

		results.absolute(-1);
		assertEquals("incorrect row #", 6, results.getRow());
		assertFalse(results.isBeforeFirst());
		assertFalse(results.isAfterLast());
		assertFalse(results.isFirst());
		assertTrue(results.isLast());

		results.afterLast();
		assertEquals("incorrect row #", 7, results.getRow());
		assertFalse(results.isBeforeFirst());
		assertTrue(results.isAfterLast());
		assertFalse("is seen as first", results.isFirst());
		assertFalse("is seen as last", results.isLast());

		// clean up
		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testScrollWithMultiLineText() throws ClassNotFoundException,
			SQLException
	{
		// load the driver into memory
		Class.forName("org.relique.jdbc.csv.CsvDriver");

		// create a connection. The first command line parameter is assumed to
		// be the directory in which the .csv files are held
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		// create a Statement object to execute the query with
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			ResultSet.CONCUR_READ_ONLY);

		// Select the ID and NAME columns from sample.csv
		ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample2");

		// dump out the results
		results.next();
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Aman", results.getString("NAME"));

		results.next();
		assertEquals("Incorrect ID Value", "B223", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Binoy", results.getString("NAME"));

		results.first();
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Aman", results.getString("NAME"));

		assertFalse(results.previous());
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		results.next();
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Aman", results.getString("NAME"));

		results.next();
		assertEquals("Incorrect ID Value", "B223", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Binoy", results.getString("NAME"));

		results.relative(2);
		assertEquals("Incorrect ID Value", "D456", results.getString("ID"));
		assertEquals("Incorrect NAME Value",
				"Dilip \"meals\" Maurice\n ~In New  LIne~ \nDone", results
						.getString("NAME"));

		results.relative(-3);
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Aman", results.getString("NAME"));

		results.relative(0);
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Aman", results.getString("NAME"));

		results.absolute(2);
		assertEquals("Incorrect ID Value", "B223", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Binoy", results.getString("NAME"));

		results.absolute(-2);
		assertEquals("Incorrect ID Value", "E589", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"Elephant\"", results
				.getString("NAME"));

		results.last();
		assertEquals("Incorrect ID Value", "F634", results.getString("ID"));
		assertEquals(
				"Incorrect NAME Value",
				"Fandu \"\"peg leg\"\", Jimmy & Samantha \n~In Another New  LIne~ \n\"\"Sam\"\"",
				results.getString("NAME"));

		results.next();
		assertNull(results.getString("ID"));
		assertNull(results.getString("NAME"));

		results.previous();
		assertEquals("Incorrect ID Value", "F634", results.getString("ID"));
		assertEquals(
				"Incorrect NAME Value",
				"Fandu \"\"peg leg\"\", Jimmy & Samantha \n~In Another New  LIne~ \n\"\"Sam\"\"",
				results.getString("NAME"));

		results.relative(100);
		assertNull(results.getString("ID"));
		assertNull(results.getString("NAME"));

		assertFalse(results.relative(-100));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		results.relative(2);
		assertEquals("Incorrect ID Value", "B223", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Binoy", results.getString("NAME"));

		results.absolute(7);
		assertNull(results.getString("ID"));
		assertNull(results.getString("NAME"));

		assertFalse(results.absolute(-10));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		results.absolute(2);
		assertEquals("Incorrect ID Value", "B223", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Binoy", results.getString("NAME"));

		assertFalse(results.absolute(0));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		assertFalse(results.relative(0));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}

		// clean up
		results.close();
		stmt.close();
		conn.close();
	}

	/**
	 * This checks for the scenario when due to where clause no rows are
	 * returned.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testWhereNoResults() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		// create a Statement object to execute the query with
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			ResultSet.CONCUR_READ_ONLY);

		ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM sample4 WHERE ID='05'");
		assertFalse("There are some junk records found", results.next());
		assertFalse(results.last());
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		assertTrue("Is not last", results.isLast());
		assertFalse(results.absolute(0));
		try
		{
			results.getString("ID");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		try
		{
			results.getString("NAME");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
		}
		assertTrue("Is not last", results.isLast());
		assertFalse(results.absolute(0));
		assertTrue("Is not before first", results.isBeforeFirst());
		results.previous();
		assertTrue("Is not before first", results.isBeforeFirst());
		assertTrue("Is not last", results.isLast());
		// Following throws exception
		// assertTrue("Is not before first", results.isAfterLast());

	}

	/**
	 * This checks for the scenario when we have single record
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testWhereSingleRecord() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		// create a Statement object to execute the query with
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			ResultSet.CONCUR_READ_ONLY);

		ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM singlerecord");
		assertTrue(results.last());
		assertEquals("Invalid Id", "A123", results.getString("ID"));
		assertEquals("Invalid Name", "Jonathan Ackerman", results
				.getString("NAME"));
		results.absolute(1);
		assertEquals("Invalid Id", "A123", results.getString("ID"));
		assertEquals("Invalid Name", "Jonathan Ackerman", results
				.getString("NAME"));
		assertTrue("Is not last", results.isLast());
		assertTrue("Is not first", results.isFirst());
		results.absolute(0);
		assertTrue("Is not before first", results.isBeforeFirst());
		results.previous();
		assertTrue("Is not before first", results.isBeforeFirst());
		assertTrue(results.next());
		assertEquals("Invalid Id", "A123", results.getString("ID"));
		assertEquals("Invalid Name", "Jonathan Ackerman", results
				.getString("NAME"));
		results.relative(1);
		assertTrue("Is not after last", results.isAfterLast());
		results.previous();
		assertEquals("Invalid Id", "A123", results.getString("ID"));
		assertEquals("Invalid Name", "Jonathan Ackerman", results
				.getString("NAME"));
	}

	/**
	 * This tests for the scenario with where clause.
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testWhereMultipleResult() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			ResultSet.CONCUR_READ_ONLY);

		ResultSet results = stmt
				.executeQuery("SELECT ID, Name, Job FROM sample4 WHERE Job = 'Project Manager'");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertTrue(results.first());
		assertEquals("The ID is wrong when using first", "01", results
				.getString("ID"));

		assertTrue(results.absolute(3));
		assertEquals("The ID is wrong", "04", results.getString("ID"));

		assertTrue(results.last());
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertFalse("It has records after last", results.next());
		assertTrue("Is not after Last", results.isAfterLast());
		assertTrue(results.previous());
		assertTrue(results.previous());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.relative(0));
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.relative(1));
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertTrue("Is not last", results.isLast());
		assertTrue(results.relative(-2));
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertTrue("Is not first", results.isFirst());
		results.previous();
		assertTrue("Is not before first", results.isBeforeFirst());

	}

	@Test
	public void testMaxRows() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		stmt.setMaxRows(4);
		assertEquals("getMaxRows() incorrect", 4, stmt.getMaxRows());

		ResultSet results = stmt.executeQuery("SELECT * FROM sample5");

		assertTrue("Moving to last record failed", results.last());
		assertEquals("Last record wrong", 4, results.getRow());
		assertEquals("The ID is wrong", "03", results.getString("ID"));
		assertTrue("Moving to first record failed", results.first());
		assertEquals("The ID is wrong", "41", results.getString("ID"));
		assertTrue("Reading row 2 failed", results.next());
		assertTrue("Reading row 3 failed", results.next());
		assertTrue("Reading row 4 failed", results.next());
		assertFalse("Stopping after row 4 failed", results.next());
	}

	@Test
	public void testResultSetFirstClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.first();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetLastClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.last();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetPreviousClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.previous();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetAbsoluteClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.absolute(2);
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetRelativeClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.relative(-1);
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetAfterLastClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.afterLast();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetBeforeFirstClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.beforeFirst();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetIsAfterLastClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.isAfterLast();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetIsBeforeFirstClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.isBeforeFirst();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetIsFirstClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.isFirst();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}

	@Test
	public void testResultSetIsLastClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		results.next();
		results.next();
		results.close();
		try
		{
			results.isLast();
			fail("Closed result set should throw SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
		}

		// clean up
		stmt.close();
		conn.close();
	}
}
