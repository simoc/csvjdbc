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
import java.sql.Date;
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
 * Tests use of SQL GROUP BY clause.
 */
public class TestGroupBy
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
	public void testGroupBySimple() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TO_BLZ from transactions group by TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertFalse(results.next());
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
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TRANS_DATE, TO_BLZ from transactions group by TRANS_DATE, TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-19"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-21"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-21"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-24"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-27"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-28"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-31"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select ID from empty-2 GROUP BY ID");
		assertFalse(results.next());
	}

	@Test
	public void testGroupByAllDifferent() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select ID from sample4 GROUP BY ID");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "03", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample5 WHERE ID >= 5 GROUP BY Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Piloto", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Employee", results.getString("Job"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByOrderBy() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample4 GROUP BY Job ORDER BY Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertFalse(results.next());
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
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TO_BLZ, COUNT(TO_BLZ) AS N from transactions group by TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 5, results.getInt("N"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 2, results.getInt("N"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 1, results.getInt("N"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 1, results.getInt("N"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByCountStar() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Integer");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select NAME, count(*) from scores group by NAME");
		assertTrue(results.next());
		assertEquals("The NAME is wrong", "Daniel", results.getString(1));
		assertEquals("The COUNT is wrong", 3, results.getInt(2));
		assertTrue(results.next());
		assertEquals("The NAME is wrong", "Mark", results.getString(1));
		assertEquals("The COUNT is wrong", 3, results.getInt(2));
		assertTrue(results.next());
		assertEquals("The NAME is wrong", "Maria", results.getString(1));
		assertEquals("The COUNT is wrong", 3, results.getInt(2));
	}

	@Test
	public void testGroupByCountNull() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select NAME, COUNT(NULLIF(SCORE, 'NA')) FROM scores GROUP BY NAME");
		assertTrue(results.next());
		assertEquals("The NAME is wrong", "Daniel", results.getString(1));
		assertEquals("The COUNT is wrong", 2, results.getInt(2));
		assertTrue(results.next());
		assertEquals("The NAME is wrong", "Mark", results.getString(1));
		assertEquals("The COUNT is wrong", 3, results.getInt(2));
		assertTrue(results.next());
		assertEquals("The NAME is wrong", "Maria", results.getString(1));
		assertEquals("The COUNT is wrong", 1, results.getInt(2));
		assertFalse(results.next());
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
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TO_BLZ, MIN(AMOUNT) AS MIN_AMOUNT, MAX(AMOUNT) AS MAX_AMOUNT from transactions group by TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(21.23, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(250.00, results.getDouble("MAX_AMOUNT")));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(460.00, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(999.00, results.getDouble("MAX_AMOUNT")));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(1012.74, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(1012.74, results.getDouble("MAX_AMOUNT")));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(7.23, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(7.23, results.getDouble("MAX_AMOUNT")));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByStringAgg() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job, string_agg(Name, ';') from sample4 GROUP BY Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString(1));
		assertEquals("The string_agg is wrong", "Juan Pablo Morales;Mauricio Hernandez;Felipe Grajales", results.getString(2));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString(1));
		assertEquals("The string_agg is wrong", "Maria Cristina Lucero", results.getString(2));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByOrderByCount() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job, COUNT(Job) C from sample4 GROUP BY Job ORDER BY C DESC");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertEquals("The COUNT is wrong", 3, results.getInt("C"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertEquals("The COUNT is wrong", 1, results.getInt("C"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByColumnNumber() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample4 GROUP BY 1");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByTableAlias() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample4 T GROUP BY T.Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
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
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select FROM_ACCT + '/' + FROM_BLZ as KEY from transactions group by KEY");
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "3670345/10010010", results.getString("KEY"));
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "97540210/10020500", results.getString("KEY"));
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "58340576/10010010", results.getString("KEY"));
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "2340529/10020200", results.getString("KEY"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByWithLiteral() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job + '/' C1, 7 C2 from sample4 GROUP BY Job");
		assertTrue(results.next());
		assertEquals("The C1 is wrong", "Project Manager/", results.getString("C1"));
		assertEquals("The C2 is wrong", "7", results.getString("C2"));
		assertTrue(results.next());
		assertEquals("The C1 is wrong", "Finance Manager/", results.getString("C1"));
		assertEquals("The C2 is wrong", "7", results.getString("C2"));
		assertFalse(results.next());
	}

	@Test
	public void testGroupByWithBadColumnName() throws SQLException
	{		
		try
		{
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample group by XXXX");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testSelectUngroupedColumn() throws SQLException
	{		
		try
		{
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample4 group by Job");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("columnNotInGroupBy") + ": ID", "" + e);
		}
	}

	@Test
	public void testOrderByUngroupedColumn() throws SQLException
	{		
		try
		{
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Job FROM sample4 group by Job order by Id");
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
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select FROM_BLZ from transactions group by FROM_BLZ having FROM_BLZ > 10020000");
		assertTrue(results.next());
		assertEquals("The FROM_BLZ is wrong", 10020500, results.getInt("FROM_BLZ"));
		assertTrue(results.next());
		assertEquals("The FROM_BLZ is wrong", 10020200, results.getInt("FROM_BLZ"));
		assertFalse(results.next());
	}

	@Test
	public void testHavingCount() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select Job from sample5 group by Job having COUNT(Job) = 1");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Piloto", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Manager", results.getString("Job"));
		assertFalse(results.next());
	}

	@Test
	public void testSelectAndHavingCount() throws SQLException
	{
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select Job, COUNT(Job) from sample5 group by Job having COUNT(Job) > 1");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertEquals("The COUNT(Job) is wrong", 3, results.getInt(2));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Employee", results.getString("Job"));
		assertEquals("The COUNT(Job) is wrong", 4, results.getInt(2));
		assertFalse(results.next());
	}

	@Test
	public void testHavingWithBadColumnName() throws SQLException
	{		
		try
		{
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample group by Id HAVING XXXX = 1");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testHavingUngroupedColumn() throws SQLException
	{		
		try
		{
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample group by Id HAVING Name = 'foo'");
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
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select FROM_BLZ, count(distinct FROM_ACCT) from transactions group by FROM_BLZ order by FROM_BLZ");
		assertTrue(results.next());
		assertEquals("Incorrect FROM_BLZ", "10010010", results.getString(1));
		assertEquals("Incorrect count FROM_ACCT", 2, results.getInt(2));
		assertTrue(results.next());
		assertEquals("Incorrect FROM_BLZ", "10020200", results.getString(1));
		assertEquals("Incorrect count FROM_ACCT", 1, results.getInt(2));
		assertTrue(results.next());
		assertEquals("Incorrect FROM_BLZ", "10020500", results.getString(1));
		assertEquals("Incorrect count FROM_ACCT", 1, results.getInt(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	@Test
	public void testGroupBySumAvgDistinct() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select CampaignNo, sum(distinct PurchaseCt), round(avg(distinct PurchaseCt)*10) from Purchase group by CampaignNo order by CampaignNo");
		assertTrue(results.next());
		assertEquals("Incorrect CampaignNo", 1, results.getInt(1));
		assertEquals("Incorrect sum PurchaseCt", 4 + 1 + 11, results.getInt(2));
		assertEquals("Incorrect avg PurchaseCt", Math.round((4 + 1 + 11) / 3.0 * 10), results.getInt(3));
		assertTrue(results.next());
		assertEquals("Incorrect CampaignNo", 21, results.getInt(1));
		assertEquals("Incorrect sum PurchaseCt", 1 + 3, results.getInt(2));
		assertEquals("Incorrect avg PurchaseCt", Math.round((1 + 3) / 2.0 * 10), results.getInt(3));
		assertTrue(results.next());
		assertEquals("Incorrect CampaignNo", 61, results.getInt(1));
		assertEquals("Incorrect sum PurchaseCt", 4, results.getInt(2));
		assertEquals("Incorrect avg PurchaseCt", 4 * 10, results.getInt(3));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
}
