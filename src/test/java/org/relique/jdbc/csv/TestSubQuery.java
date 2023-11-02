/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2014 Simon Chenery

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
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSubQuery
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

	/**
     * Compare two values for near equality, allowing for floating point round-off.
     */
	private boolean fuzzyEquals(double a, double b)
    {
            return (a == b || Math.round(a * 1000) == Math.round(b * 1000));
    }

	@Test
	public void testSubQuery() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes.banks", "Integer,String");
		props.put("columnTypes.transactions", "Date,Integer,Integer,Integer,Integer,Double");
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT (SELECT BANK_NAME FROM banks WHERE BLZ=FROM_BLZ), AMOUNT FROM transactions"))
		{
			assertTrue(rs1.next());
			assertEquals("Postbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertTrue(fuzzyEquals(250.0, rs1.getDouble(2)), "The AMOUNT is wrong");
			assertTrue(rs1.next());
			assertEquals("Postbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertTrue(rs1.next());
			assertEquals("Postbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertTrue(rs1.next());
			assertEquals("Bank f\u00FCr Sozialwirtschaft (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertTrue(rs1.next());
			assertEquals("Postbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
		}
	}

	@Test
	public void testInvalidTable() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try
			{
				ResultSet rs1 = stmt.executeQuery("SELECT (SELECT BANK_NAME FROM XXXX), AMOUNT FROM transactions");
				if (rs1.next())
					rs1.getString(1);
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().contains(CsvResources.getString("fileNotFound")));
				assertTrue(e.getMessage().contains("XXXX"));
			}
		}
	}

	@Test
	public void testInvalidColumn() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try
			{
				ResultSet rs1 = stmt.executeQuery("SELECT (SELECT XXXX FROM banks WHERE BLZ=FROM_BLZ), AMOUNT FROM transactions");
				if (rs1.next())
					rs1.getString(1);
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().contains(CsvResources.getString("invalidColumnName")));
				assertTrue(e.getMessage().contains("XXXX"));
			}
		}
	}

	@Test
	public void testMoreThanOneColumn() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try
			{
				ResultSet rs1 = stmt.executeQuery("SELECT (SELECT BANK_NAME, BANK_NAME B2 FROM banks WHERE BLZ=FROM_BLZ), AMOUNT FROM transactions");
				if (rs1.next())
					rs1.getString(1);
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("subqueryOneColumn"), "" + e);
			}
		}
	}

	@Test
	public void testMoreThanOneRow() throws SQLException
	{
		Properties props = new Properties();
		props.put("", "");
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try
			{
				ResultSet rs1 = stmt.executeQuery("SELECT (SELECT BANK_NAME FROM banks), AMOUNT FROM transactions");
				if (rs1.next())
					rs1.getString(1);
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("subqueryOneRow"), "" + e);
			}
		}
	}

	@Test
	public void testNoMatch() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT ID, NAME, (SELECT NAME FROM sample2 s2 WHERE s2.ID=s1.ID) FROM sample s1"))
		{
			assertTrue(rs1.next());
			assertEquals("Q123", rs1.getString(1), "The ID is wrong");
			assertEquals("\"S,\"", rs1.getString(2), "The Name is wrong");
			// No match so subquery returns null for this row.
			assertNull(rs1.getString(3), "The Subquery is wrong");
			assertTrue(rs1.next());
			assertEquals("A123", rs1.getString(1), "The ID is wrong");
			assertEquals("Jonathan Ackerman", rs1.getString(2), "The Name is wrong");
			assertEquals("Aman", rs1.getString(3), "The Subquery is wrong");
			assertTrue(rs1.next());
			assertEquals("B234", rs1.getString(1), "The ID is wrong");
			assertEquals("Grady O'Neil", rs1.getString(2), "The Name is wrong");
			// No match so subquery returns null for this row.
			assertNull(rs1.getString(3), "The Subquery is wrong");
		}
	}

	@Test
	public void testAggregateFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT BANK_NAME, (SELECT COUNT(*) FROM transactions WHERE BLZ=FROM_BLZ) COUNT_ FROM banks"))
		{
			assertTrue(rs1.next());
			assertEquals("Bundesbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertEquals(0, rs1.getInt(2), "The Subquery is wrong");
			assertTrue(rs1.next());
			assertEquals("Postbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertEquals(5, rs1.getInt(2), "The Subquery is wrong");
			assertTrue(rs1.next());
			assertEquals("SEB (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertEquals(0, rs1.getInt(2), "The Subquery is wrong");
		}
	}

	@Test
	public void testOrderBy() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline.banks", "BLZ,BANK_NAME");
        props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
        props.put("suppressHeaders", "true");
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT BANK_NAME, (SELECT COUNT(*) FROM transactions WHERE BLZ=FROM_BLZ) COUNT_ FROM banks ORDER BY COUNT_ DESC"))
		{
			assertTrue(rs1.next());
			assertEquals("Postbank (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertEquals(5, rs1.getInt(2), "The Subquery is wrong");
			assertTrue(rs1.next());
			assertEquals("Bank f\u00FCr Sozialwirtschaft (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertEquals(3, rs1.getInt(2), "The Subquery is wrong");
			assertTrue(rs1.next());
			assertEquals("BHF-BANK (Berlin)", rs1.getString(1), "The BANK_NAME is wrong");
			assertEquals(1, rs1.getInt(2), "The Subquery is wrong");
		}
	}

	@Test
	public void testSubQueryFromSameTable() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,Integer,Integer,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("select AccountNo from Purchase where CampaignNo=(select max(CampaignNo) from Purchase)"))
		{
			assertTrue(rs1.next());
			assertEquals(22021, rs1.getInt(1), "The AccountNo is wrong");
			assertFalse(rs1.next());
		}
	}

	@Test
	public void testInSubQuery() throws SQLException
	{
		Properties props = new Properties();
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("select NAME from sample5 where ID in (select ID from sample4)"))
		{
			assertTrue(rs1.next());
			assertEquals("Juan Pablo Morales", rs1.getString(1), "The NAME is wrong");
			assertTrue(rs1.next());
			assertEquals("Mauricio Hernandez", rs1.getString(1), "The NAME is wrong");
			assertTrue(rs1.next());
			assertEquals("Maria Cristina Lucero", rs1.getString(1), "The NAME is wrong");
			assertTrue(rs1.next());
			assertEquals("Felipe Grajales", rs1.getString(1), "The NAME is wrong");
			assertFalse(rs1.next());
		}
	}

	@Test
	public void testInSubQueryNoMatch() throws SQLException
	{
		Properties props = new Properties();
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("select NAME from sample5 where ID in (select AccountNo from Purchase where CampaignNo='XXXX')"))
		{
			assertFalse(rs1.next());
		}
	}

	@Test
	public void testExistsSubQuery() throws SQLException
	{
		Properties props = new Properties();
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("select EXTRA_FIELD from sample where exists (select 1 from sample2 where sample2.id=sample.id)"))
		{
			assertTrue(rs1.next());
			assertEquals("A", rs1.getString(1), "The EXTRA_FIELD is wrong");
			assertFalse(rs1.next());
		}
	}

	@Test
	public void testNotExistsSubQuery() throws SQLException
	{
		Properties props = new Properties();
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("select EXTRA_FIELD from sample where not exists (select 1 from sample2 where sample2.id=sample.id)"))
		{
			assertTrue(rs1.next());
			assertEquals("F", rs1.getString(1), "The EXTRA_FIELD is wrong");
			assertTrue(rs1.next());
			assertEquals("B", rs1.getString(1), "The EXTRA_FIELD is wrong");
			assertTrue(rs1.next());
			assertEquals("C", rs1.getString(1), "The EXTRA_FIELD is wrong");
			assertTrue(rs1.next());
			assertEquals("E", rs1.getString(1), "The EXTRA_FIELD is wrong");
			assertTrue(rs1.next());
			assertEquals("G", rs1.getString(1), "The EXTRA_FIELD is wrong");
			assertFalse(rs1.next());
		}
	}

	@Test
	public void testDerivedTable() throws SQLException
	{
		Properties props = new Properties();
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try
			{
				ResultSet rs1 = stmt.executeQuery("SELECT ID FROM (SELECT ID FROM sample) AS X");
				if (rs1.next())
					rs1.getString(1);
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().contains(CsvResources.getString("derivedTableNotSupported")));
			}
		}
	}
}
