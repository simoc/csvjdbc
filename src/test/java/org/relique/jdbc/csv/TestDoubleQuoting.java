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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestDoubleQuoting
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
	public void testQuotedTableName() throws SQLException
	{
		Properties props = new Properties();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet rs1 = stmt.executeQuery("SELECT * FROM \"C D\"");
		assertTrue(rs1.next());

		rs1.close();
		stmt.close();
	}
	
	@Test
	public void testQuotedColumnNames() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,Integer,Integer,Integer,Integer");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet rs1 = stmt.executeQuery("SELECT \"A\", \"B\" + 100, \"[A]\", \"A-B\", \"A B\" FROM \"C D\"");
		assertEquals("Column name A is wrong", "A", rs1.getMetaData().getColumnName(1));

		assertEquals("Column name [A] is wrong", "[A]", rs1.getMetaData().getColumnName(3));
		assertEquals("Column name A-B is wrong", "A-B", rs1.getMetaData().getColumnName(4));
		assertEquals("Column name A B is wrong", "A B", rs1.getMetaData().getColumnName(5));
		assertTrue(rs1.next());
		assertEquals("The A is wrong", 1, rs1.getInt("A"));
		assertEquals("The [A] is wrong", 3, rs1.getInt("[A]"));
		assertEquals("The A-B is wrong", 4, rs1.getInt("A-B"));
		assertEquals("The A-B is wrong", 5, rs1.getInt("A B"));
		assertTrue(rs1.next());
		assertEquals("The A is wrong", 6, rs1.getInt(1));
		assertEquals("The B + 100 is wrong", 7 + 100, rs1.getInt(2));
		assertEquals("The [A] is wrong", 8, rs1.getInt(3));
		assertEquals("The A-B is wrong", 9, rs1.getInt(4));
		assertEquals("The A-B is wrong", 10, rs1.getInt(5));
		rs1.close();
		stmt.close();
	}
	
	@Test
	public void testQuotedTableAlias() throws SQLException
	{
		Properties props = new Properties();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		String alias = "\"http://www.google.com\"";
		ResultSet rs1 = stmt.executeQuery("SELECT " + alias + ".ID, " + alias + ".\"EXTRA_FIELD\" " +
				"FROM sample AS " + alias);
		assertTrue(rs1.next());
		assertEquals("The ID is wrong", "Q123", rs1.getString(1));
		assertEquals("The EXTRA_FIELD is wrong", "F", rs1.getString(2));

		rs1.close();
		stmt.close();
	}
}
