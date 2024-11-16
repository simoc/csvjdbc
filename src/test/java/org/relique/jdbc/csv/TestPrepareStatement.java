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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the CsvJdbc driver.
 *
 * @author Mario Frasca
 */
public class TestPrepareStatement
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
	public void testCanPrepareStatement() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props))
		{
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
	}

	@Test
	public void testCanUsePreparedStatement() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 1);
			prepstmt.setInt(2, 3);
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals(Integer.valueOf(1), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(2), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(3), results.getObject("id"), "Integer column ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testPreparedStatementIsClosed() throws SQLException
	{
		String queryString = "SELECT * FROM sample WHERE id = ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setString(1, "A123");
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals("A", results.getString("EXTRA_FIELD"), "Column EXTRA_FIELD is wrong");
				conn.close();
				assertTrue(prepstmt.isClosed());
			}
		}
	}

	@Test
	public void testShortParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Short,String,String");
		String queryString = "SELECT * FROM sample4 WHERE id = ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setShort(1, (short)3);
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals("Finance Manager", results.getString("Job"), "Column Job is wrong");
				assertFalse(results.next());
			}
		}
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
		String queryString = "SELECT * FROM banks WHERE BLZ = ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			long blz = 10020200;
			prepstmt.setLong(1, blz);
			ResultSet results = prepstmt.executeQuery();

			assertTrue(results.next());
			assertEquals("BHF-BANK (Berlin)", results.getString("BANK_NAME"), "Column BANK_NAME is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testFloatParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		String queryString = "SELECT * FROM numeric WHERE C5 < ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setFloat(1, 3);
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals("0.0", results.getString("C5"), "Column C5 is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testDoubleParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		String queryString = "SELECT * FROM numeric WHERE C6 < ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setDouble(1, 1000.0);
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals("-0.0", results.getString("C6"), "Column C6 is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testTooManyParameters() throws SQLException
	{
		String queryString = "SELECT * FROM sample WHERE id = ? OR id = ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			try
			{
				prepstmt.setString(1, "A123");
				prepstmt.setString(2, "B234");
				prepstmt.setString(3, "C456");
				fail("Setting too many parameters should fail");
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().contains(CsvResources.getString("parameterIndex")));
			}
		}
	}

	@Test
	public void testLike() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		String queryString = "SELECT * FROM sample5 WHERE Name LIKE ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setString(1, "%Lucero%");
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals(3, results.getInt("ID"), "Column ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testCanReuseAPreparedStatement() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		String queryString = "SELECT * FROM sample5 WHERE id BETWEEN ? AND ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 1);
			prepstmt.setInt(2, 3);
			try (ResultSet results1 = prepstmt.executeQuery())
			{
				assertTrue(results1.next());
				assertTrue(results1.next());
				assertTrue(results1.next());
				assertFalse(results1.next());

				prepstmt.setInt(1, 30);
				prepstmt.setInt(2, 50);
				try (ResultSet results2 = prepstmt.executeQuery())
				{
					assertTrue(results1.isClosed());
					assertTrue(results2.next());
					assertEquals(Integer.valueOf(41), results2.getObject("id"), "Integer column ID is wrong");
					assertFalse(results2.next());
				}
			}
		}
	}

	@Test
	public void testCanUsePreparedStatementOnStrings() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		String queryString = "SELECT * FROM sample5 WHERE job = ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setString(1, "Project Manager");
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals(Integer.valueOf(1), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(3), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(4), results.getObject("id"), "Integer column ID is wrong");
				assertFalse(results.next());
			}

			prepstmt.setString(1, "Office Employee");
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals(Integer.valueOf(6), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(7), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(8), results.getObject("id"), "Integer column ID is wrong");
				assertTrue(results.next());
				assertEquals(Integer.valueOf(9), results.getObject("id"), "Integer column ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testNoWhereClause() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props))
		{
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
	}

	@Test
	public void testTableReader() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
			PreparedStatement stmt = conn.prepareStatement("SELECT * FROM airport where code=?"))
		{
			stmt.setString(1, "CDG");
			try (ResultSet results = stmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals("Paris Charles De Gaulle", results.getString("NAME"), "NAME wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testPreparedStatementWithOrderBy() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		String queryString = "SELECT * FROM sample5 where id > ? order by id";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 7);
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertEquals(8, results.getInt("ID"), "column ID is wrong");
				assertTrue(results.next());
				assertEquals(9, results.getInt("ID"), "column ID is wrong");
				assertTrue(results.next());
				assertEquals(41, results.getInt("ID"), "column ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testExecute() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String,Timestamp,String");
		String queryString = "SELECT * FROM sample5 where id = ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 2);
			assertTrue(prepstmt.execute());
			try (ResultSet results = prepstmt.getResultSet())
			{
				assertNotNull(results, "ResultSet is null");
				assertTrue(results.next());
				assertEquals("Finance Manager", results.getString("Job"), "column Job is wrong");
				assertFalse(results.next());
				assertFalse(prepstmt.getMoreResults());
			}
		}
	}

	@Test
	public void testLimitParameter() throws SQLException
	{
		Properties props = new Properties();
		props.put("extension", ".csv");
		props.put("columnTypes", "Int,String,String");
		String queryString = "SELECT * FROM sample4 LIMIT ?";
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 2);
			try (ResultSet results = prepstmt.executeQuery())
			{
				assertTrue(results.next());
				assertTrue(results.next());
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testOffsetParameter() throws SQLException{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		String queryString = "select id from sample5 where id > 2 limit 3 offset ?";

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 4);

			try (ResultSet results = prepstmt.executeQuery()) {
				assertTrue(results.next());
				assertEquals(6, results.getInt("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(7, results.getInt("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(8, results.getInt("ID"), "The ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testLimitOffsetParameter() throws SQLException{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		String queryString = "select id from sample5 where id > 2 limit ? offset ?";

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 3);
			prepstmt.setInt(2, 4);

			try (ResultSet results = prepstmt.executeQuery()) {
				assertTrue(results.next());
				assertEquals(6, results.getInt("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(7, results.getInt("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(8, results.getInt("ID"), "The ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testWhereAndLimitParameter() throws SQLException{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		String queryString = "select id from sample5 where id > ? limit ? offset ?";

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);
			 PreparedStatement prepstmt = conn.prepareStatement(queryString))
		{
			prepstmt.setInt(1, 2);
			prepstmt.setInt(2, 3);
			prepstmt.setInt(3, 4);

			try (ResultSet results = prepstmt.executeQuery()) {
				assertTrue(results.next());
				assertEquals(6, results.getInt("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(7, results.getInt("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(8, results.getInt("ID"), "The ID is wrong");
				assertFalse(results.next());
			}
		}
	}


}
