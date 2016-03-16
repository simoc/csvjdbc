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
 * This class tests SQL aggregate functions in the CsvJdbc driver.
 */
public class TestAggregateFunctions
{
	public static String filePath;
	public static DateFormat toUTC;

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
	public void testCountStar() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT COUNT(*) FROM sample");
		assertTrue(results.next());
		assertEquals("Incorrect count", 6, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testCountColumn() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT count(ID) FROM sample");
		assertTrue(results.next());
		assertEquals("Incorrect count", "6", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testCountInvalidColumn() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try
		{
			stmt.executeQuery("SELECT count(XXXX) FROM sample");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testCountPlusColumn() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try
		{
			stmt.executeQuery("SELECT ID, count(ID) FROM sample");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("columnsWithAggregateFunctions"), "" + e);
		}
	}

	@Test
	public void testCountWhere() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT count(*) C FROM sample where ID like '%234'");
		assertTrue(results.next());
		assertEquals("Incorrect count", 2, results.getInt("C"));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testCountNoResults() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT count(*) FROM sample where ID like 'ZZZ%'");
		assertTrue(results.next());
		assertEquals("Incorrect count", 0, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testCountInWhereClause() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try
		{
			stmt.executeQuery("SELECT * FROM sample where count(*)=1");
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("noAggregateFunctions"), "" + e);
		}
	}

	@Test
	public void testMax() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MAX(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect max", 6, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMaxWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select max(name) from sample8 where id < 3");
		assertTrue(results.next());
		assertEquals("Incorrect max", "Mauricio Hernandez", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMaxNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MAX(Name) from sample8 where ID = 999");
		assertTrue(results.next());
		assertEquals("Incorrect max", null, results.getObject(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMaxNoResultsExpression() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT MAX(ID)+1 FROM sample8 WHERE ID > 999");
		assertTrue(results.next());
		assertEquals("Incorrect max", null, results.getObject(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMaxRound() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Double,Double,Double,Double,Double,Double,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT ROUND(MAX(C5)) FROM numeric");
		assertTrue(results.next());
		assertEquals("Incorrect max", 3, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMaxDate() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select max(d) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect max", "2010-03-28", results.getObject(1).toString());
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMin() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MIN(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect min", 1, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMinWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select min(upper(name)) from sample8 where id < 3");
		assertTrue(results.next());
		assertEquals("Incorrect min", "JUAN PABLO MORALES", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testMinMax() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select MIN(ID), MAX(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect min", 1, results.getInt(1));
		assertEquals("Incorrect max", 6, results.getInt(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testSum() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select SUM(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect sum", 21, results.getInt(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testSumTwoColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Long,Double,Double,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select sum(c4), sum(c5) from numeric");
		assertTrue(results.next());
		assertEquals("Incorrect sum", 989999995600L, results.getLong(1));
		assertEquals("Incorrect sum", "3.14", results.getString(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testSumNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select SUM(ID) from sample8 where id > 999");
		assertTrue(results.next());
		assertEquals("Incorrect sum", null, results.getObject(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testAvg() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select AVG(ID) from sample8");
		assertTrue(results.next());
		assertEquals("Incorrect avg", "3.5", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	@Test
	public void testAvgTwoColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Long,Double,Double,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select avg(c2), avg(c5) from numeric");
		assertTrue(results.next());
		assertEquals("Incorrect avg", "-497.5", results.getString(1));
		assertEquals("Incorrect avg", "1.57", results.getString(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testAvgNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select AVG(ID) from sample8 where id > 999");
		assertTrue(results.next());
		assertEquals("Incorrect avg", null, results.getObject(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	@Test
	public void testCountDistinct() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select count(distinct FROM_ACCT), count(distinct FROM_BLZ) from transactions");
		assertTrue(results.next());
		assertEquals("Incorrect count FROM_ACCT", 4, results.getInt(1));
		assertEquals("Incorrect count FROM_BLZ", 3, results.getInt(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
	
	@Test
	public void testSumAvgDistinct() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select sum(distinct PurchaseCt), round(avg(distinct PurchaseCt)) from Purchase");
		assertTrue(results.next());
		assertEquals("Incorrect sum", 1 + 3 + 4 + 11, results.getInt(1));
		assertEquals("Incorrect avg", Math.round((1 + 3 + 4 + 11) / 4.0), results.getInt(2));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}

	@Test
	public void testStringAgg() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select STRING_AGG(ID, ';') from sample");
		assertTrue(results.next());
		assertEquals("Incorrect STRING_AGG", "Q123;A123;B234;C456;D789;X234", results.getString(1));
		assertFalse(results.next());

		results.close();
		stmt.close();
		conn.close();
	}
}
