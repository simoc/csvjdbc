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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Mario Frasca
 */
public class TestPrepareStatement
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
	public void testCanPrepareStatement() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		try
		{
			conn.prepareStatement(queryString);
			conn.prepareStatement(queryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			conn.prepareStatement(queryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
				ResultSet.HOLD_CURSORS_OVER_COMMIT);
		}
		catch (UnsupportedOperationException e)
		{
			fail("cannot prepareStatement!");
		}
	}

	@Test
	public void testCanUsePreparedStatement() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 1);
		prepstmt.setInt(2, 3);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(2), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(3), results.getObject("id"));
		assertFalse(results.next());
	}

	@Test
	public void testPreparedStatementIsClosed() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
		String queryString = "SELECT * FROM sample WHERE id = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setString(1, "A123");
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column EXTRA_FIELD is wrong", "A", results.getString("EXTRA_FIELD"));
		conn.close();
		assertTrue(prepstmt.isClosed());
		
	}

	@Test
	public void testShortParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Short,String,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample4 WHERE id = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setShort(1, (short)3);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
	}

	@Test
	public void testLongParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Long,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		String queryString = "SELECT * FROM banks WHERE BLZ = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		long blz = 10020200;
		prepstmt.setLong(1, blz);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column BANK_NAME is wrong", "BHF-BANK (Berlin)", results.getString("BANK_NAME"));
		assertFalse(results.next());
	}

	@Test
	public void testFloatParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM numeric WHERE C5 < ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setFloat(1, (float)3);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column C5 is wrong", "0.0", results.getString("C5"));
		assertFalse(results.next());
	}

	@Test
	public void testDoubleParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM numeric WHERE C6 < ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setDouble(1, 1000.0);
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column C6 is wrong", "-0.0", results.getString("C6"));
		assertFalse(results.next());
	}

	@Test
	public void testLike() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE Name LIKE ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setString(1, "%Lucero%");
		ResultSet results = prepstmt.executeQuery();

		assertTrue(results.next());
		assertEquals("Column ID is wrong", 3, results.getInt("ID"));
		assertFalse(results.next());
	}

	@Test
	public void testCanReuseAPreparedStatement() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 1);
		prepstmt.setInt(2, 3);
		ResultSet results1 = prepstmt.executeQuery();

		assertTrue(results1.next());
		assertTrue(results1.next());
		assertTrue(results1.next());
		assertFalse(results1.next());

		prepstmt.setInt(1, 30);
		prepstmt.setInt(2, 50);
		ResultSet results2 = prepstmt.executeQuery();

		assertTrue(results1.isClosed());
		assertTrue(results2.next());
		assertEquals("Integer column ID is wrong", new Integer(41), results2.getObject("id"));
		assertFalse(results2.next());
	}

	@Test
	public void testCanUsePreparedStatementOnStrings() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample5 WHERE job = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setString(1, "Project Manager");
		ResultSet results = prepstmt.executeQuery();
		
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(3), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(4), results.getObject("id"));
		assertFalse(results.next());
		
		prepstmt.setString(1, "Office Employee");
		results = prepstmt.executeQuery();
		
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(6), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(7), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(8), results.getObject("id"));
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(9), results.getObject("id"));
		assertFalse(results.next());
	}

	@Test
	public void testNoWhereClause() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		String queryString = "SELECT * FROM sample5";
		try
		{
			conn.prepareStatement(queryString);
		}
		catch (UnsupportedOperationException e)
		{
			fail("can't prepareStatement!");
		}
	}

	@Test
	public void testTableReader() throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM airport where code=?");
		stmt.setString(1, "CDG");
		ResultSet results = stmt.executeQuery();
		assertTrue(results.next());
		assertEquals("NAME wrong", "Paris Charles De Gaulle", results.getString("NAME"));
		assertFalse(results.next());
	}

	@Test
	public void testPreparedStatementWithOrderBy() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample5 where id > ? order by id";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 7);
		ResultSet results = prepstmt.executeQuery();
		assertTrue(results.next());
		assertEquals("column ID is wrong", 8, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("column ID is wrong", 9, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("column ID is wrong", 41, results.getInt("ID"));
		assertFalse(results.next());
		results.close();
	}

	@Test
	public void testExecute() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
		String queryString = "SELECT * FROM sample5 where id = ?";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);

		prepstmt.setInt(1, 2);
		assertTrue(prepstmt.execute());
		ResultSet results = prepstmt.getResultSet();
		assertNotNull("ResultSet is null", results);
		assertTrue(results.next());
		assertEquals("column Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
		assertFalse(prepstmt.getMoreResults());
		results.close();
	}
}
