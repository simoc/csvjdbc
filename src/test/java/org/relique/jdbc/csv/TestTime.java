/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2024  Simon Chenery

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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.time.LocalTime;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests columns with type TIME and arithmetic using these columns.
 */
public class TestTime
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
	public void testTimePlusMinusNumber() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Date,Time,Date,Time,String,String,String");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT Start_time, Start_time + 3600000, Start_time - 1000 FROM calendar"))
		{
			assertTrue(results.next());
			Time expectedTime1 = Time.valueOf(LocalTime.of(9, 0, 0));
			assertEquals(expectedTime1, results.getTime(1));
			Time expectedTime2 = Time.valueOf(LocalTime.of(9 + 1, 0, 0));
			assertEquals(expectedTime2, results.getTime(2));
			Time expectedTime3 = Time.valueOf(LocalTime.of(8, 59, 59));
			assertEquals(expectedTime3, results.getTime(3));
		}
	}
	
	@Test
	public void testTimeMinusTime() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Date,Time,Date,Time,String,String,String");
		props.put("useDateTimeFormatter", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT End_time - Start_time FROM calendar"))
		{
			assertTrue(results.next());
			long expectedDifference = 3 * 3600 * 1000;
			assertEquals(expectedDifference, results.getLong(1));
		}
	}
}
