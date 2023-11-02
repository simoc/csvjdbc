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
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * This class is used to test the CsvJdbc Scrollable driver.
 *
 * @author Chetan Gupta
 */
public class TestScrollableDriver
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
	public void testScroll() throws SQLException
	{
		// create a connection. The first command line parameter is assumed to
		// be the directory in which the .csv files are held
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			// create a Statement object to execute the query with
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			// Select the ID and NAME columns from sample.csv
			ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample"))
		{
			// dump out the results
			results.next();
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"),
				"Incorrect NAME Value");
			assertEquals(1, results.getRow(), "incorrect row #");

			results.next();
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Incorrect NAME Value");
			assertEquals(2, results.getRow(), "incorrect row #");

			results.previous();
			assertEquals(1, results.getRow(), "incorrect row #");
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"),
				"Incorrect NAME Value");

			results.first();
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"),
				"Incorrect NAME Value");
			assertEquals(1, results.getRow(), "incorrect row #");

			assertFalse(results.previous());
			assertEquals(0, results.getRow(), "incorrect row #");
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
			assertEquals(1, results.getRow(), "incorrect row #");
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"),
				"Incorrect NAME Value");

			results.next();
			assertEquals(2, results.getRow(), "incorrect row #");
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Incorrect NAME Value");

			results.absolute(1);
			assertEquals(1, results.getRow(), "incorrect row #");
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"),
				"Incorrect NAME Value");

			results.absolute(2);
			assertEquals(2, results.getRow(), "incorrect row #");
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Incorrect NAME Value");

			results.relative(2);
			assertEquals(4, results.getRow(), "incorrect row #");
			assertEquals("C456", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Susan, Peter and Dave", results.getString("NAME"),
				"Incorrect NAME Value");

			results.relative(-3);
			assertEquals(1, results.getRow(), "incorrect row #");
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"),
				"Incorrect NAME Value");

			results.relative(0);
			assertEquals(1, results.getRow(), "incorrect row #");
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals(results.getString("NAME"),	"\"S,\"",
				"Incorrect NAME Value");

			results.last();
			assertEquals(6, results.getRow(), "incorrect row #");
			assertEquals("X234", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Peter \"peg leg\", Jimmy & Samantha \"Sam\"",
				results.getString("NAME"), "Incorrect NAME Value");

			results.absolute(-1);
			assertEquals(6, results.getRow(), "incorrect row #");
			assertEquals("X234", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Peter \"peg leg\", Jimmy & Samantha \"Sam\"",
				results.getString("NAME"), "Incorrect NAME Value");

			results.absolute(-2);
			assertEquals(5, results.getRow(), "incorrect row #");
			assertEquals("D789", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Amelia \"meals\" Maurice",
					results.getString("NAME"), "Incorrect NAME Value");

			results.last();
			results.next();
			assertNull(results.getString("ID"));
			assertNull(results.getString("NAME"));

			results.previous();
			assertEquals("X234", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Peter \"peg leg\", Jimmy & Samantha \"Sam\"",
				results.getString("NAME"), "Incorrect NAME Value");

			assertFalse(results.relative(100));
			assertEquals(7, results.getRow(), "incorrect row #");
			assertNull(results.getString("ID"));
			assertNull(results.getString("NAME"));

			assertFalse(results.relative(-100));
			assertEquals(0, results.getRow(), "incorrect row #");
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
			assertEquals(2, results.getRow(), "incorrect row #");
			assertEquals(results.getString("ID"), "A123", "Incorrect ID Value");
			assertEquals(results.getString("NAME"),
					"Jonathan Ackerman", "Incorrect NAME Value");

			results.absolute(7);
			assertEquals(7, results.getRow(), "incorrect row #");
			assertNull(results.getString("ID"));
			assertNull(results.getString("NAME"));

			assertFalse(results.absolute(-10));
			assertEquals(0, results.getRow(), "incorrect row #");
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
			assertEquals(results.getString("ID"), "A123", "Incorrect ID Value");
			assertEquals(results.getString("NAME"),
					"Jonathan Ackerman", "Incorrect NAME Value");

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
		}
	}

	@Test
	public void testIsFirstIsLast() throws SQLException
	{
		// create a connection. The first command line parameter is assumed to
		// be the directory in which the .csv files are held
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			// create a Statement object to execute the query with
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			// Select the ID and NAME columns from sample.csv
			ResultSet results = stmt.executeQuery("SELECT ID, NAME FROM sample"))
		{
			// dump out the results
			results.next();
			assertEquals(1, results.getRow(), "incorrect row #");
			assertTrue(results.isFirst());
			assertFalse(results.isLast());
			assertFalse(results.isBeforeFirst());
			assertFalse(results.isAfterLast());

			results.next();
			assertEquals(2, results.getRow(), "incorrect row #");
			assertFalse(results.isFirst());
			assertFalse(results.isLast());

			results.previous();
			assertEquals(1, results.getRow(), "incorrect row #");
			assertTrue(results.isFirst());
			assertFalse(results.isLast());

			results.first();
			assertEquals(1, results.getRow(), "incorrect row #");
			assertTrue(results.isFirst());
			assertFalse(results.isLast());

			results.previous();
			assertEquals(0, results.getRow(), "incorrect row #");
			assertTrue(results.isBeforeFirst());
			assertFalse(results.isAfterLast());
			assertFalse(results.isFirst());
			assertFalse(results.isLast());

			results.next();
			assertEquals(1, results.getRow(), "incorrect row #");
			assertTrue(results.isFirst());
			assertFalse(results.isLast());

			results.last();
			assertEquals(6, results.getRow(), "incorrect row #");
			assertFalse(results.isFirst());
			assertTrue(results.isLast());

			results.absolute(-1);
			assertEquals(6, results.getRow(), "incorrect row #");
			assertFalse(results.isBeforeFirst());
			assertFalse(results.isAfterLast());
			assertFalse(results.isFirst());
			assertTrue(results.isLast());

			results.afterLast();
			assertEquals(7, results.getRow(), "incorrect row #");
			assertFalse(results.isBeforeFirst());
			assertTrue(results.isAfterLast());
			assertFalse(results.isFirst(), "is seen as first");
			assertFalse(results.isLast(), "is seen as last");
		}
	}

	@Test
	public void testScrollWithMultiLineText() throws ClassNotFoundException,
			SQLException
	{
		// load the driver into memory
		Class.forName("org.relique.jdbc.csv.CsvDriver");

		// create a connection. The first command line parameter is assumed to
		// be the directory in which the .csv files are held
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			// create a Statement object to execute the query with
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			// Select the ID and NAME columns from sample.csv
			ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample2"))
		{
			// dump out the results
			results.next();
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Aman", results.getString("NAME"), "Incorrect NAME Value");

			results.next();
			assertEquals("B223", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Binoy", results.getString("NAME"), "Incorrect NAME Value");

			results.first();
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Aman", results.getString("NAME"), "Incorrect NAME Value");

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
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Aman", results.getString("NAME"), "Incorrect NAME Value");

			results.next();
			assertEquals("B223", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Binoy", results.getString("NAME"), "Incorrect NAME Value");

			results.relative(2);
			assertEquals("D456", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Dilip \"meals\" Maurice\n ~In New  LIne~ \nDone",
				results.getString("NAME"), "Incorrect NAME Value");

			results.relative(-3);
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Aman", results.getString("NAME"), "Incorrect NAME Value");

			results.relative(0);
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Aman", results.getString("NAME"), "Incorrect NAME Value");

			results.absolute(2);
			assertEquals("B223", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Binoy", results.getString("NAME"), "Incorrect NAME Value");

			results.absolute(-2);
			assertEquals("E589", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"Elephant\"", results.getString("NAME"),
				"Incorrect NAME Value");

			results.last();
			assertEquals("F634", results.getString("ID"), "Incorrect ID Value");
			assertEquals(
					"Fandu \"\"peg leg\"\", Jimmy & Samantha \n~In Another New  LIne~ \n\"\"Sam\"\"",
					results.getString("NAME"),
					"Incorrect NAME Value");

			results.next();
			assertNull(results.getString("ID"));
			assertNull(results.getString("NAME"));

			results.previous();
			assertEquals("F634", results.getString("ID"), "Incorrect ID Value");
			assertEquals(
					"Fandu \"\"peg leg\"\", Jimmy & Samantha \n~In Another New  LIne~ \n\"\"Sam\"\"",
					results.getString("NAME"),
					"Incorrect NAME Value");

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
			assertEquals("B223", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Binoy", results.getString("NAME"), "Incorrect NAME Value");

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
			assertEquals("B223", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Binoy", results.getString("NAME"), "Incorrect NAME Value");

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
		}
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
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			// create a Statement object to execute the query with
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM sample4 WHERE ID='05'"))
		{
			assertFalse(results.next(), "There are some junk records found");
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
			assertTrue(results.isLast(), "Is not last");
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
			assertTrue(results.isLast(), "Is not last");
			assertFalse(results.absolute(0));
			assertTrue(results.isBeforeFirst(), "Is not before first");
			results.previous();
			assertTrue(results.isBeforeFirst(), "Is not before first");
			assertTrue(results.isLast(), "Is not last");
			// Following throws exception
			// assertTrue("Is not before first", results.isAfterLast());
		}
	}

	/**
	 * This checks for the scenario when we have single record
	 *
	 * @throws SQLException
	 */
	@Test
	public void testWhereSingleRecord() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			// create a Statement object to execute the query with
			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM singlerecord"))
		{
			assertTrue(results.last());
			assertEquals("A123", results.getString("ID"), "Invalid Id");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Invalid Name");
			results.absolute(1);
			assertEquals("A123", results.getString("ID"), "Invalid Id");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Invalid Name");
			assertTrue(results.isLast(), "Is not last");
			assertTrue(results.isFirst(), "Is not first");
			results.absolute(0);
			assertTrue(results.isBeforeFirst(), "Is not before first");
			results.previous();
			assertTrue(results.isBeforeFirst(), "Is not before first");
			assertTrue(results.next());
			assertEquals("A123", results.getString("ID"), "Invalid Id");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Invalid Name");
			results.relative(1);
			assertTrue(results.isAfterLast(), "Is not after last");
			results.previous();
			assertEquals("A123", results.getString("ID"), "Invalid Id");
			assertEquals("Jonathan Ackerman", results.getString("NAME"),
				"Invalid Name");
		}
	}

	/**
	 * This tests for the scenario with where clause.
	 *
	 * @throws SQLException
	 */
	@Test
	public void testWhereMultipleResult() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

			ResultSet results = stmt
				.executeQuery("SELECT ID, Name, Job FROM sample4 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals("01", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("04", results.getString("ID"), "The ID is wrong");
			assertTrue(results.first());
			assertEquals("01", results.getString("ID"), "The ID is wrong when using first");

			assertTrue(results.absolute(3));
			assertEquals("04", results.getString("ID"), "The ID is wrong");

			assertTrue(results.last());
			assertEquals("04", results.getString("ID"), "The ID is wrong");
			assertFalse(results.next(), "It has records after last");
			assertTrue(results.isAfterLast(), "Is not after Last");
			assertTrue(results.previous());
			assertTrue(results.previous());
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(results.relative(0));
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(results.relative(1));
			assertEquals("04", results.getString("ID"), "The ID is wrong");
			assertTrue(results.isLast(), "Is not last");
			assertTrue(results.relative(-2));
			assertEquals("01", results.getString("ID"), "The ID is wrong");
			assertTrue(results.isFirst(), "Is not first");
			results.previous();
			assertTrue(results.isBeforeFirst(), "Is not before first");
		}
	}

	@Test
	public void testMaxRows() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY))
		{
			stmt.setMaxRows(4);
			assertEquals(4, stmt.getMaxRows(), "getMaxRows() incorrect");

			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample5"))
			{
				assertTrue(results.last(), "Moving to last record failed");
				assertEquals(4, results.getRow(), "Last record wrong");
				assertEquals("03", results.getString("ID"), "The ID is wrong");
				assertTrue(results.first(), "Moving to first record failed");
				assertEquals("41", results.getString("ID"), "The ID is wrong");
				assertTrue(results.next(), "Reading row 2 failed");
				assertTrue(results.next(), "Reading row 3 failed");
				assertTrue(results.next(), "Reading row 4 failed");
				assertFalse(results.next(), "Stopping after row 4 failed");
			}
		}
	}

	@Test
	public void testResultSetFirstClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetLastClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetPreviousClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetAbsoluteClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetRelativeClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetAfterLastClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetBeforeFirstClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetIsAfterLastClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetIsBeforeFirstClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetIsFirstClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}

	@Test
	public void testResultSetIsLastClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
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
		}
	}
}
