/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2024 Simon Chenery

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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

public class TestVersionFunction
{
	public static String filePath;

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
	public void testVersionFunction() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			 Statement stmt = conn.createStatement();
			 ResultSet results = stmt.executeQuery("SELECT VERSION()"))
		{
			assertTrue(results.next());
			String version = results.getString(1);
			assertEquals(version, conn.getMetaData().getDriverVersion());

			Pattern semverPattern = Pattern.compile("^(\\d+\\.\\d+|\\d+\\.\\d+\\.\\d+)(?:-SNAPSHOT)?$");
			assertTrue(semverPattern.asMatchPredicate().test(version), "Version is not valid semver version: "+version);
		}
	}

}
