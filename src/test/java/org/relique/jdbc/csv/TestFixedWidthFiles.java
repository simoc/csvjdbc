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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestFixedWidthFiles
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
	public void testFixedWidth() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fixedWidths", "1-16,17-24,25-27,35-42,43-50,51-58");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			// GERMANY         Euro    EUR       0.7616450.7632460.002102
			ResultSet rs1 = stmt.executeQuery("SELECT * FROM currency-exchange-rates-fixed"))
		{
			assertTrue(rs1.next());
			assertEquals("GERMANY", rs1.getString("Country"), "Country rate is wrong");
			assertEquals("Euro", rs1.getString("Currency"), "Currency rate is wrong");
			assertEquals("EUR", rs1.getString("ISO"), "ISO rate is wrong");
			assertEquals("0.761645", rs1.getString("YESTERDY"), "YESTERDY rate is wrong");
			assertEquals("0.763246", rs1.getString("TODAY_"), "TODAY_ rate is wrong");
			assertEquals("0.002102", rs1.getString("% Change"), "% Change rate is wrong");

			assertTrue(rs1.next());
			assertEquals("GREECE", rs1.getString(1), "Country rate is wrong");
			assertEquals("Euro", rs1.getString(2), "Currency rate is wrong");
			assertEquals("EUR", rs1.getString(3), "ISO rate is wrong");
			assertEquals("0.761645", rs1.getString(4), "YESTERDY rate is wrong");
			assertEquals("0.763246", rs1.getString(5), "TODAY_ rate is wrong");
			assertEquals("0.002102", rs1.getString(6), "% Change rate is wrong");

			assertTrue(rs1.next());
		}
	}

	@Test
	public void testHeaderline() throws SQLException
	{
		/*
		 * Test providing our own header with more readable names.
		 */
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("headerline", "Country         CurrencyISO       PREV_DAYTODAY   PTChange");
		props.put("suppressHeaders", "true");
		props.put("skipLeadingLines", "1");
		props.put("columnTypes", "String,String,String,Double,Double,Double");
		props.put("fixedWidths", "1-16,17-24,25-27,35-42,43-50,51-58");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			// GERMANY         Euro    EUR       0.7616450.7632460.002102
			try (ResultSet rs1 = stmt
				.executeQuery("SELECT * FROM currency-exchange-rates-fixed c WHERE c.Country = 'GERMANY'"))
			{
				assertTrue(rs1.next());
				assertEquals("EUR", rs1.getString("ISO"), "ISO rate is wrong");
				assertEquals("0.761645", rs1.getString("PREV_DAY"), "YESTERDY rate is wrong");
				assertEquals("0.763246", rs1.getString("TODAY"), "TODAY_ rate is wrong");
				assertEquals("0.002102", rs1.getString("PTChange"), "% Change rate is wrong");
			}

			// HUNGARY         Forint  HUF       226.1222226.67130.002429
			try (ResultSet rs2 = stmt
				.executeQuery("SELECT * FROM currency-exchange-rates-fixed c WHERE c.Country = 'HUNGARY'"))
			{
				assertTrue(rs2.next());
				assertEquals("HUF", rs2.getString("ISO"), "ISO rate is wrong");
				assertEquals("226.1222", rs2.getString("PREV_DAY"), "YESTERDY rate is wrong");
				assertEquals("226.6713", rs2.getString("TODAY"), "TODAY_ rate is wrong");
				assertEquals("0.002429", rs2.getString("PTChange"), "% Change rate is wrong");
			}

			// PERU            Sol     PEN       2.6618362.661836       0
			try (ResultSet rs3 = stmt
				.executeQuery("SELECT * FROM currency-exchange-rates-fixed c WHERE c.Country = 'PERU'"))
			{
				assertTrue(rs3.next());
				assertEquals("PEN", rs3.getString("ISO"), "ISO rate is wrong");
				assertEquals("2.661836", rs3.getString("PREV_DAY"), "YESTERDY rate is wrong");
				assertEquals("2.661836", rs3.getString("TODAY"), "TODAY_ rate is wrong");
				assertEquals("0.0", rs3.getString("PTChange"), "% Change rate is wrong");
			}

			//SAUDI ARABIA    Riyal   SAR       3.7504133.750361-1.4E-05
			try (ResultSet rs4 = stmt
				.executeQuery("SELECT * FROM currency-exchange-rates-fixed c WHERE c.Country = 'SAUDI ARABIA'"))
			{
				assertTrue(rs4.next());
				assertEquals("SAR", rs4.getString("ISO"), "ISO rate is wrong");
				assertEquals("3.750413", rs4.getString("PREV_DAY"), "YESTERDY rate is wrong");
				assertEquals("3.750361", rs4.getString("TODAY"), "TODAY_ rate is wrong");
				assertEquals("-1.4E-5", rs4.getString("PTChange"), "% Change rate is wrong");
			}
		}
	}

	@Test
	public void testNumericColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("columnTypes", "String,String,String,Double,Double,Double");
		props.put("fixedWidths", "1-16,17-24,25-27,35-42,43-50,51-58");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			//SWEDEN          Krona   SEK       6.76557 6.752711 -0.0019
			//SWITZERLAND     Franc   CHF       0.9153470.9178320.002715
			ResultSet rs1 = stmt.executeQuery("SELECT * FROM currency-exchange-rates-fixed " +
				"WHERE Country='SWEDEN' OR Country='SWITZERLAND'"))
		{
			int multiplier = 1000;
			assertTrue(rs1.next());
			assertEquals(Math.round(6.76557 * multiplier), Math.round(rs1.getDouble("YESTERDY") * multiplier), "TODAY_ is wrong");
			assertEquals(Math.round(6.752711 * multiplier), Math.round(rs1.getDouble("TODAY_") * multiplier), "YESTERDY is wrong");
			assertEquals(Math.round(-0.0019 * multiplier), Math.round(rs1.getDouble("% Change") * multiplier), "% Change is wrong");

			assertTrue(rs1.next());
			assertEquals(Math.round(0.915347 * multiplier), Math.round(rs1.getDouble("YESTERDY") * multiplier), "TODAY_ is wrong");
			assertEquals(Math.round(0.917832 * multiplier), Math.round(rs1.getDouble("TODAY_") * multiplier), "YESTERDY is wrong");
			assertEquals(Math.round(0.002715 * multiplier), Math.round(rs1.getDouble("% Change") * multiplier), "% Change is wrong");
		}
	}

	@Test
	public void testColumnSizes() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("columnTypes", "String,String,String,Double,Double,Double");
		props.put("fixedWidths", "1-16,17-24,25-27,35-42,43-50,51-58");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT Country,YESTERDY,ISO FROM currency-exchange-rates-fixed"))
		{
			ResultSetMetaData meta = rs1.getMetaData();
			assertEquals(16, meta.getColumnDisplaySize(1), "Incorrect Column Size");
			assertEquals(8, meta.getColumnDisplaySize(2), "Incorrect Column Size");
			assertEquals(3, meta.getColumnDisplaySize(3), "Incorrect Column Size");
		}
	}

	@Test
	public void testWidthOrder() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("columnTypes", "String,Integer,String");
		props.put("fixedWidths", "30-32,29,25-28");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet rs1 = stmt.executeQuery("SELECT * FROM flights"))
		{
			assertTrue(rs1.next());
			assertEquals("A18", rs1.getString(1), "Column 1 is wrong");
			assertEquals(1, rs1.getInt(2), "Column 2 is wrong");
			assertEquals("", rs1.getString(3), "Column 3 is wrong");

			assertTrue(rs1.next());
			assertEquals("B2", rs1.getString(1), "Column 1 is wrong");
			assertEquals(1, rs1.getInt(2), "Column 2 is wrong");
			assertEquals("", rs1.getString(3), "Column 3 is wrong");

			assertTrue(rs1.next());
			assertEquals("D4", rs1.getString(1), "Column 1 is wrong");
			assertEquals(2, rs1.getInt(2), "Column 2 is wrong");
			assertEquals("", rs1.getString(3), "Column 3 is wrong");

			assertTrue(rs1.next());
			assertEquals("A22", rs1.getString(1), "Column 1 is wrong");
			assertEquals(1, rs1.getInt(2), "Column 2 is wrong");
			assertEquals("1320", rs1.getString(3), "Column 3 is wrong");
		}
	}

	@Test
	public void testFixedWidthHeaderLineNotFixedWidth() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("isHeaderFixedWidth", "false");
		props.put("fixedWidths", "1-2,3-3,4-6,7-7");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			// GERMANY		   Euro	   EUR		 0.7616450.7632460.002102
			ResultSet rs1 = stmt.executeQuery("SELECT * FROM single-char-cols-fixed"))
		{
			assertTrue(rs1.next());
			assertEquals("BB", rs1.getString("TwoChars"), "Column 1 value is wrong");
			assertEquals("A", rs1.getString("OneChar"), "Column 2 value is wrong");
			assertEquals("CCC",rs1.getString("ThreeChars"), "Column 3 value is wrong");
			assertEquals("D", rs1.getString("YetAnotherOneChar"), "Column 4 value is wrong");

			assertTrue(rs1.next());
			assertEquals("22", rs1.getString(1), "Column 1 value is wrong");
			assertEquals("1", rs1.getString(2), "Column 2 value is wrong");
			assertEquals("333", rs1.getString(3), "Column 3 value is wrong");
			assertEquals("4", rs1.getString(4), "Column 4 value is wrong");

			assertTrue(rs1.next());
		}
	}

	@Test
	public void testFixedWidthHeaderLineNotFixedWidthSupressHeaders() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("skipLeadingLines", "1");
		props.put("suppressHeaders", "true");
		props.put("headerline", "Column1,Column2,Column3,Column4");
		props.put("isHeaderFixedWidth", "false");
		props.put("fixedWidths", "1-2,3-3,4-6,7-7");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			// GERMANY		   Euro	   EUR		 0.7616450.7632460.002102
			ResultSet rs1 = stmt.executeQuery("SELECT * FROM single-char-cols-fixed"))
		{
			assertTrue(rs1.next());
			assertEquals("BB", rs1.getString("Column1"), "Column 1 value is wrong");
			assertEquals("A", rs1.getString("Column2"), "Column 2 value is wrong");
			assertEquals("CCC",rs1.getString("Column3"), "Column 3 value is wrong");
			assertEquals("D", rs1.getString("Column4"), "Column 4 value is wrong");

			assertTrue(rs1.next());
			assertEquals("22", rs1.getString(1), "Column 1 value is wrong");
			assertEquals("1", rs1.getString(2), "Column 2 value is wrong");
			assertEquals("333", rs1.getString(3), "Column 3 value is wrong");
			assertEquals("4", rs1.getString(4), "Column 4 value is wrong");

			assertTrue(rs1.next());
		}
	}
}
