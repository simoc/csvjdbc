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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests use of SQL ORDER BY clause.
 */
public class TestOrderBy
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
	public void testOrderBySimple() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,String");
		props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM banks ORDER BY BANK_NAME"))
		{
			assertTrue(results.next());
			assertEquals(10010424, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Aareal Bank (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10020200, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("BHF-BANK (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Bank f\u00FCr Sozialwirtschaft (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10020000, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Berliner Bank -alt- (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10000000, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Bundesbank (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10020400, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Citadele Bank Zndl Deutschland (M\u00FCnchen)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10019610, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Dexia Kommunalbank Deutschland (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("Postbank (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10010111, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("SEB (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertTrue(results.next());
			assertEquals(10010222, results.getInt("BLZ"), "The BLZ is wrong");
			assertEquals("The Royal Bank of Scotland, Niederlassung Deutschland (Berlin)", results.getString("BANK_NAME"), "The BLZ_NAME is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testOrderByColumnNumber() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 ORDER BY 1"))
		{
			assertTrue(results.next());
			assertEquals(1, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(2, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(4, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(5, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(6, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(7, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(8, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(9, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(41, results.getInt("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testOrderByColumnNumberExpression() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT ID+10 AS ID10 FROM sample5 ORDER BY 1"))
		{
			assertTrue(results.next());
			assertEquals(11, results.getInt("ID10"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(12, results.getInt("ID10"), "The ID is wrong");
			assertTrue(results.next());
		}
	}

	@Test
	public void testOrderByDesc() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,String");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT BLZ FROM banks ORDER BY BLZ DESC"))
		{
			assertTrue(results.next());
			assertEquals(10020500, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10020400, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10020200, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10020000, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10019610, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10010424, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10010222, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10010111, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt(1), "The BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10000000, results.getInt(1), "The BLZ is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testOrderByTwoColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select * from transactions order by from_blz, from_acct"))
		{
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(3670345, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(3670345, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(3670345, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(3670345, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(58340576, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10020200, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(2340529, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(97540210, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(97540210, results.getInt("from_acct"), "The from_acct is wrong");
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("from_blz"), "The from_blz is wrong");
			assertEquals(97540210, results.getInt("from_acct"), "The from_acct is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testOrderByWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT FROM_ACCT, FROM_BLZ from transactions where AMOUNT < 50 ORDER BY AMOUNT"))
		{
			assertTrue(results.next());
			assertEquals(97540210, results.getInt("FROM_ACCT"), "The FROM_ACCT is wrong");
			assertEquals(10020500, results.getInt("FROM_BLZ"), "The FROM_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(3670345, results.getInt("FROM_ACCT"), "The FROM_ACCT is wrong");
			assertEquals(10010010, results.getInt("FROM_BLZ"), "The FROM_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(3670345, results.getInt("FROM_ACCT"), "The FROM_ACCT is wrong");
			assertEquals(10010010, results.getInt("FROM_BLZ"), "The FROM_BLZ is wrong");
			assertFalse(results.next());
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
	public void testOrderByColumnAlias() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT FROM_ACCT, AMOUNT * 0.01 as FEE from transactions ORDER BY FEE"))
		{
			assertTrue(results.next());
			assertEquals(97540210, results.getInt(1), "The FROM_ACCT is wrong");
			double fee = results.getDouble(2);
			assertTrue(fuzzyEquals(7.23 * 0.01, fee), "The FEE is wrong");
			assertTrue(results.next());
			assertEquals(3670345, results.getInt(1), "The FROM_ACCT is wrong");
			fee = results.getDouble(2);
			assertTrue(fuzzyEquals(21.23 * 0.01, fee), "The FEE is wrong");
		}
	}

	@Test
	public void testOrderByNumericExpression() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");
		props.put("columnTypes", "Date,Time,String,Float,Float,String,String");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT AI007.000, AI007.001 FROM test ORDER BY AI007.000 + AI007.001 DESC"))
		{
			assertTrue(results.next());
			double a0 = results.getFloat("AI007.000");
			double a1 = results.getFloat("AI007.001");
			assertTrue(fuzzyEquals(26.54, a0 + a1), "The sort order is wrong");
			assertTrue(results.next());
			a0 = results.getFloat("AI007.000");
			a1 = results.getFloat("AI007.001");
			assertTrue(fuzzyEquals(26.54, a0 + a1), "The sort order is wrong");
		}
	}

	@Test
	public void testOrderByDateExpression() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select * from sample5 order by start+timeoffset asc"))
		{
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(41, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(2, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(7, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(8, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(4, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(5, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(6, results.getInt(1), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(9, results.getInt(1), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testOrderByWithBadColumnName()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT Id FROM sample order by XXXX"))
			{
				fail("Should raise a java.sqlSQLException");
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testOrderByWithBadColumnNumber()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample ORDER BY 99"))
			{
				fail("Should raise a java.sqlSQLException");
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidOrderBy") + ": 99", "" + e);
		}
	}

	@Test
	public void testOrderByWithFloatColumnNumber()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample ORDER BY 3.14"))
			{
				fail("Should raise a java.sqlSQLException");
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidOrderBy") + ": 3.14", "" + e);
		}
	}

	@Test
	public void testOrderByWithBadValue()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample ORDER BY 'X'"))
			{
				fail("Should raise a java.sqlSQLException");
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidOrderBy") + ": 'X'", "" + e);
		}
	}

	@Test
	public void testOrderByNoResults() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT Name, Job FROM sample4 WHERE ID='05' order by Name"))
		{
			assertFalse(results.next());
		}
	}
}
