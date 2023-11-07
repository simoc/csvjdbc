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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.sql.*;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests use of SQL GROUP BY clause.
 */
public class TestGroupBy
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
	public void testGroupBySimple() throws SQLException
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
				.executeQuery("select TO_BLZ from transactions group by TO_BLZ"))
		{
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10010424, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10020400, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByTwoColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		props.put("dateFormat", "dd-mm-yyyy");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select TRANS_DATE, TO_BLZ from transactions group by TRANS_DATE, TO_BLZ"))
		{
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-19"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-21"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-21"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10010424, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-24"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-27"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-28"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10020400, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(Date.valueOf("2011-10-31"), results.getDate("TRANS_DATE"), "The TRANS_DATE is wrong");
			assertEquals(10010010, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select ID from empty-2 GROUP BY ID"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByAllDifferent() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select ID from sample4 GROUP BY ID"))
		{
			assertTrue(results.next());
			assertEquals("01", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("03", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("04", results.getString("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job from sample5 WHERE ID >= 5 GROUP BY Job"))
		{
			assertTrue(results.next());
			assertEquals("Piloto", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Office Manager", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Office Employee", results.getString("Job"), "The Job is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByOrderBy() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job from sample4 GROUP BY Job ORDER BY Job"))
		{
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString("Job"), "The Job is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByCount() throws SQLException
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
				.executeQuery("select TO_BLZ, COUNT(TO_BLZ) AS N from transactions group by TO_BLZ"))
		{
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertEquals(5, results.getInt("N"), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals(10010424, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertEquals(2, results.getInt("N"), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals(10020400, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertEquals(1, results.getInt("N"), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertEquals(1, results.getInt("N"), "The COUNT is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByCountStar() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Integer");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select NAME, count(*) from scores group by NAME"))
		{
			assertTrue(results.next());
			assertEquals("Daniel", results.getString(1), "The NAME is wrong");
			assertEquals(3, results.getInt(2), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals("Mark", results.getString(1), "The NAME is wrong");
			assertEquals(3, results.getInt(2), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals("Maria", results.getString(1), "The NAME is wrong");
			assertEquals(3, results.getInt(2), "The COUNT is wrong");
		}
	}

	@Test
	public void testGroupByCountNull() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select NAME, COUNT(NULLIF(SCORE, 'NA')) FROM scores GROUP BY NAME"))
		{
			assertTrue(results.next());
			assertEquals("Daniel", results.getString(1), "The NAME is wrong");
			assertEquals(2, results.getInt(2), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals("Mark", results.getString(1), "The NAME is wrong");
			assertEquals(3, results.getInt(2), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals("Maria", results.getString(1), "The NAME is wrong");
			assertEquals(1, results.getInt(2), "The COUNT is wrong");
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
	public void testGroupByMinMax() throws SQLException
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
				.executeQuery("select TO_BLZ, MIN(AMOUNT) AS MIN_AMOUNT, MAX(AMOUNT) AS MAX_AMOUNT from transactions group by TO_BLZ"))
		{
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(fuzzyEquals(21.23, results.getDouble("MIN_AMOUNT")), "The MIN_AMOUNT is wrong");
			assertTrue(fuzzyEquals(250.00, results.getDouble("MAX_AMOUNT")), "The MAX_AMOUNT is wrong");
			assertTrue(results.next());
			assertEquals(10010424, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(fuzzyEquals(460.00, results.getDouble("MIN_AMOUNT")), "The MIN_AMOUNT is wrong");
			assertTrue(fuzzyEquals(999.00, results.getDouble("MAX_AMOUNT")), "The MAX_AMOUNT is wrong");
			assertTrue(results.next());
			assertEquals(10020400, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(fuzzyEquals(1012.74, results.getDouble("MIN_AMOUNT")), "The MIN_AMOUNT is wrong");
			assertTrue(fuzzyEquals(1012.74, results.getDouble("MAX_AMOUNT")), "The MAX_AMOUNT is wrong");
			assertTrue(results.next());
			assertEquals(10010010, results.getInt("TO_BLZ"), "The TO_BLZ is wrong");
			assertTrue(fuzzyEquals(7.23, results.getDouble("MIN_AMOUNT")), "The MIN_AMOUNT is wrong");
			assertTrue(fuzzyEquals(7.23, results.getDouble("MAX_AMOUNT")), "The MAX_AMOUNT is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByStringAgg() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job, string_agg(Name, ';') from sample4 GROUP BY Job"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString(1), "The Job is wrong");
			assertEquals("Juan Pablo Morales;Mauricio Hernandez;Felipe Grajales", results.getString(2), "The string_agg is wrong");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString(1), "The Job is wrong");
			assertEquals("Maria Cristina Lucero", results.getString(2), "The string_agg is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByArrayAgg() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("select Job, ARRAY_AGG(Name) from sample4 GROUP BY Job"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString(1), "The Job is wrong");
			Array array = results.getArray(2);
			assertNotNull(array);
			Object[] data = (Object[]) array.getArray();
			assertEquals(3, data.length);
			assertArrayEquals(new String[]{ "Juan Pablo Morales", "Mauricio Hernandez","Felipe Grajales"},
				data, "The array is wrong");
		}
	}



	@Test
	public void testGroupByOrderByCount() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job, COUNT(Job) C from sample4 GROUP BY Job ORDER BY C DESC"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString("Job"), "The Job is wrong");
			assertEquals(3, results.getInt("C"), "The COUNT is wrong");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString("Job"), "The Job is wrong");
			assertEquals(1, results.getInt("C"), "The COUNT is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByColumnNumber() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job from sample4 GROUP BY 1"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString("Job"), "The Job is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByTableAlias() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job from sample4 T GROUP BY T.Job"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString("Job"), "The Job is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByExpression() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		props.put("dateFormat", "dd-mm-yyyy");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select FROM_ACCT + '/' + FROM_BLZ as KEY from transactions group by KEY"))
		{
			assertTrue(results.next());
			assertEquals("3670345/10010010", results.getString("KEY"), "The KEY is wrong");
			assertTrue(results.next());
			assertEquals("97540210/10020500", results.getString("KEY"), "The KEY is wrong");
			assertTrue(results.next());
			assertEquals("58340576/10010010", results.getString("KEY"), "The KEY is wrong");
			assertTrue(results.next());
			assertEquals("2340529/10020200", results.getString("KEY"), "The KEY is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByWithLiteral() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Job + '/' C1, 7 C2 from sample4 GROUP BY Job"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager/", results.getString("C1"), "The C1 is wrong");
			assertEquals("7", results.getString("C2"), "The C2 is wrong");
			assertTrue(results.next());
			assertEquals("Finance Manager/", results.getString("C1"), "The C1 is wrong");
			assertEquals("7", results.getString("C2"), "The C2 is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupByWithBadColumnName()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT Id FROM sample group by XXXX"))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testSelectUngroupedColumn()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT Id FROM sample4 group by Job"))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("columnNotInGroupBy") + ": ID", "" + e);
		}
	}

	@Test
	public void testOrderByUngroupedColumn()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT Job FROM sample4 group by Job order by Id"))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("orderByNotInGroupBy") + ": ID", "" + e);
		}
	}

	@Test
	public void testHavingSimple() throws SQLException
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
				.executeQuery("select FROM_BLZ from transactions group by FROM_BLZ having FROM_BLZ > 10020000"))
		{
			assertTrue(results.next());
			assertEquals(10020500, results.getInt("FROM_BLZ"), "The FROM_BLZ is wrong");
			assertTrue(results.next());
			assertEquals(10020200, results.getInt("FROM_BLZ"), "The FROM_BLZ is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testHavingCount() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select Job from sample5 group by Job having COUNT(Job) = 1"))
		{
			assertTrue(results.next());
			assertEquals("Piloto", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString("Job"), "The Job is wrong");
			assertTrue(results.next());
			assertEquals("Office Manager", results.getString("Job"), "The Job is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testSelectAndHavingCount() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select Job, COUNT(Job) from sample5 group by Job having COUNT(Job) > 1"))
		{
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString("Job"), "The Job is wrong");
			assertEquals(3, results.getInt(2), "The COUNT(Job) is wrong");
			assertTrue(results.next());
			assertEquals("Office Employee", results.getString("Job"), "The Job is wrong");
			assertEquals(4, results.getInt(2), "The COUNT(Job) is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testHavingWithBadColumnName()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT Id FROM sample group by Id HAVING XXXX = 1"))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testHavingUngroupedColumn()
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT Id FROM sample group by Id HAVING Name = 'foo'"))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidHaving") + ": NAME", "" + e);
		}
	}

	@Test
	public void testGroupByCountDistinct() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select FROM_BLZ, count(distinct FROM_ACCT) from transactions group by FROM_BLZ order by FROM_BLZ"))
		{
			assertTrue(results.next());
			assertEquals("10010010", results.getString(1), "Incorrect FROM_BLZ");
			assertEquals(2, results.getInt(2), "Incorrect count FROM_ACCT");
			assertTrue(results.next());
			assertEquals("10020200", results.getString(1), "Incorrect FROM_BLZ");
			assertEquals(1, results.getInt(2), "Incorrect count FROM_ACCT");
			assertTrue(results.next());
			assertEquals("10020500", results.getString(1), "Incorrect FROM_BLZ");
			assertEquals(1, results.getInt(2), "Incorrect count FROM_ACCT");
			assertFalse(results.next());
		}
	}

	@Test
	public void testGroupBySumAvgDistinct() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select CampaignNo, sum(distinct PurchaseCt), round(avg(distinct PurchaseCt)*10) from Purchase group by CampaignNo order by CampaignNo"))
		{
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "Incorrect CampaignNo");
			assertEquals(4 + 1 + 11, results.getInt(2), "Incorrect sum PurchaseCt");
			assertEquals(Math.round((4 + 1 + 11) / 3.0 * 10), results.getInt(3), "Incorrect avg PurchaseCt");
			assertTrue(results.next());
			assertEquals(21, results.getInt(1), "Incorrect CampaignNo");
			assertEquals(1 + 3, results.getInt(2), "Incorrect sum PurchaseCt");
			assertEquals(Math.round((1 + 3) / 2.0 * 10), results.getInt(3), "Incorrect avg PurchaseCt");
			assertTrue(results.next());
			assertEquals(61, results.getInt(1), "Incorrect CampaignNo");
			assertEquals(4, results.getInt(2), "Incorrect sum PurchaseCt");
			assertEquals(4 * 10, results.getInt(3), "Incorrect avg PurchaseCt");
			assertFalse(results.next());
		}
	}
}
