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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Chetan Gupta
 * @author Mario Frasca
 */
public class TestCsvDriver
{
	private static String filePath;
	private static DateTimeFormatter toUTCDateTimeFormatter;

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

		toUTCDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));
	}

	private DateFormat getUTCDateFormat()
	{
		// java.text.DateFormat is not thread-safe, so create new object every time we need one.
		DateFormat toUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		toUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
		return toUTC;
	}

	@Test
	public void testWithDefaultValues() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample"))
		{
			results.next();
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("\"S,\"", results.getString("NAME"), "Incorrect NAME Value");
			assertEquals("F", results.getString("EXTRA_FIELD"), "Incorrect EXTRA_FIELD Value");

			assertEquals("\"S,\"", results.getString(1), "Incorrect Column 1 Value");
			assertEquals("Q123", results.getString(2), "Incorrect Column 2 Value");
			assertEquals("F", results.getString(3), "Incorrect Column 3 Value");

			results.next();
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Jonathan Ackerman", results.getString("NAME"), "Incorrect NAME Value");
			assertEquals("A", results.getString("EXTRA_FIELD"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertEquals("B234", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Grady O'Neil", results.getString("NAME"), "Incorrect NAME Value");
			assertEquals("B", results.getString("EXTRA_FIELD"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertEquals("C456", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Susan, Peter and Dave", results.getString("NAME"), "Incorrect NAME Value");
			assertEquals("C", results.getString("EXTRA_FIELD"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertEquals("D789", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Amelia \"meals\" Maurice", results.getString("NAME"),
				"Incorrect NAME Value");
			assertEquals("E", results.getString("EXTRA_FIELD"),
				"Incorrect EXTRA_FIELD Value");

			results.next();
			assertEquals("X234", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Peter \"peg leg\", Jimmy & Samantha \"Sam\"", results.getString("NAME"),
				"Incorrect NAME Value");
			assertEquals("G", results.getString("EXTRA_FIELD"), "Incorrect EXTRA_FIELD Value");
		}
	}

	/**
	 * This creates several sentences with where and tests they work
	 *
	 * @throws SQLException
	 */
	@Test
	public void testWhereSimple() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM sample4 WHERE ID='03'"))
		{
			assertTrue(results.next());
			assertEquals("Maria Cristina Lucero", results.getString("Name"),
				"The name is wrong");
			try
			{
				results.getString("Job");
				fail("Should not find the column 'Job'");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": Job", "" + e);
			}
			assertTrue(!results.next());
		}
	}

	/**
	 * fields in different order than in source file.
	 *
	 * @throws SQLException
	 */
	@Test
	public void testWhereShuffled() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT Job,ID,Name FROM sample4 WHERE ID='02'"))
		{
			assertTrue(results.next(), "no results found - should be one");
			assertEquals("Mauricio Hernandez", results.getString("Name"),
				"The name is wrong");
			assertEquals("Project Manager", results.getString("Job"),
				"The job is wrong");
			assertTrue(!results.next(), "more than one matching records");
		}
	}

	@Test
	public void testWithProperties() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", "");
		props.put("separator", ";");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample"))
		{
			results.next();
			assertTrue(results.getString("ID").equals("Q123"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("\"S;\""), "Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("F"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertTrue(results.getString("ID").equals("A123"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("Jonathan Ackerman"), "Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("A"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertTrue(results.getString("ID").equals("B234"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("Grady O'Neil"), "Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("B"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertTrue(results.getString("ID").equals("C456"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("Susan; Peter and Dave"), "Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("C"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertTrue(results.getString("ID").equals("D789"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("Amelia \"meals\" Maurice"), "Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("E"), "Incorrect EXTRA_FIELD Value");

			results.next();
			assertTrue(results.getString("ID").equals("X234"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("Peter \"peg leg\"; Jimmy & Samantha \"Sam\""),
				"Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("G"),
				"Incorrect EXTRA_FIELD Value");
		}
	}

	@Test
	public void testFindColumn() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			assertEquals(1, results.findColumn("ID"), "Incorrect Column");
			assertEquals(2, results.findColumn("Name"), "Incorrect Column");
			assertEquals(3, results.findColumn("EXTRA_FIELD"), "Incorrect Column");

			try
			{
				results.findColumn("foo");
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": foo", "" + e);
			}
		}
	}

	@Test
	public void testMetadata() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM sample3"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("sample3", metadata.getTableName(0), "Incorrect Table Name");

			assertEquals("column 1", metadata.getColumnName(1), "Incorrect Column Name 1");
			assertEquals("column \"2\" two", metadata.getColumnName(2), "Incorrect Column Name 2");
			assertEquals("Column 3", metadata.getColumnName(3), "Incorrect Column Name 3");
			assertEquals("CoLuMn4", metadata.getColumnName(4), "Incorrect Column Name 4");
			assertEquals("COLumn5", metadata.getColumnName(5), "Incorrect Column Name 5");
		}
	}

	@Test
	public void testMetadataWithSupressedHeaders() throws SQLException
	{
		Properties props = new Properties();
		props.put("suppressHeaders", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertTrue(metadata.getTableName(0).equals("sample"), "Incorrect Table Name");

			assertTrue(metadata.getColumnName(1).equals("COLUMN1"), "Incorrect Column Name 1");
			assertTrue(metadata.getColumnName(2).equals("COLUMN2"), "Incorrect Column Name 2");
			assertTrue(metadata.getColumnName(3).equals("COLUMN3"), "Incorrect Column Name 3");
		}
	}

	@Test
	public void testMetadataWithColumnType() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id, name, job, start FROM sample5"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals(Types.INTEGER, metadata.getColumnType(1),
				"type of column 1 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(2),
				"type of column 2 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(3),
				"type of column 3 is incorrect");
			assertEquals(Types.TIMESTAMP, metadata.getColumnType(4),
				"type of column 4 is incorrect");

			assertEquals("Int", metadata.getColumnTypeName(1),
				"type of column 1 is incorrect");
			assertEquals("String", metadata.getColumnTypeName(2),
				"type of column 2 is incorrect");
			assertEquals("String", metadata.getColumnTypeName(3),
				"type of column 3 is incorrect");
			assertEquals("Timestamp", metadata.getColumnTypeName(4),
				"type of column 4 is incorrect");
		}
	}

	@Test
	public void testMetadataWithColumnSize() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id+1 AS ID1, name, job, start FROM sample5"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals(20, metadata.getColumnDisplaySize(1),
				"size of column 1 is incorrect");
			assertEquals(20, metadata.getColumnDisplaySize(2),
				"size of column 2 is incorrect");
			assertEquals(20, metadata.getColumnDisplaySize(3),
				"size of column 3 is incorrect");
			assertEquals(20, metadata.getColumnDisplaySize(4),
				"size of column 4 is incorrect");
		}
	}

	@Test
	public void testMetadataWithColumnTypeShuffled() throws SQLException
	{
		// TODO: this test fails!
		Properties props = new Properties();
		// header in file: ID,Name,Job,Start,timeoffset
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT start, id, name, job FROM sample5"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			// TODO - this fails
			assertEquals(Types.TIMESTAMP, metadata.getColumnType(1),
				"type of column 1 is incorrect");
			assertEquals(Types.INTEGER, metadata.getColumnType(2),
				"type of column 2 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(3),
				"type of column 3 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(4),
				"type of column 4 is incorrect");
		}
	}

	@Test
	public void testMetadataWithOperations() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id, start+timeoffset AS ts, 999 as C3, id - 4 as C4, " +
						"ID * 1.1 as C5, Name+JOB AS c6, '.com' as C7, 'Mr '+Name as C8 FROM sample5"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals(Types.INTEGER, metadata.getColumnType(1),
				"type of column 1 is incorrect");
			assertEquals(Types.TIMESTAMP, metadata.getColumnType(2),
				"type of column 2 is incorrect");
			assertEquals(Types.INTEGER, metadata.getColumnType(3),
				"type of column 3 is incorrect");
			assertEquals(Types.INTEGER, metadata.getColumnType(4),
				"type of column 4 is incorrect");
			assertEquals(Types.DOUBLE, metadata.getColumnType(5),
				"type of column 5 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(6),
				"type of column 6 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(7),
				"type of column 7 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(8),
				"type of column 8 is incorrect");
		}
	}

	@Test
	public void testMetadataWithTableAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select sx.id, sx.name as name from sample as sx"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("sample", metadata.getTableName(0), "Incorrect Table Name");

			assertEquals("ID", metadata.getColumnName(1), "Incorrect Column Name 1");
			assertEquals("NAME", metadata.getColumnName(2), "Incorrect Column Name 2");
		}
	}

	@Test
	public void testMetadataColumnLabels() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id * 10 as XID, name, 1000 as dept FROM sample5"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("XID", metadata.getColumnName(1), "name of column 1 is incorrect");
			assertEquals("XID", metadata.getColumnLabel(1), "label of column 1 is incorrect");
			assertEquals("Name", metadata.getColumnName(2), "name of column 2 is incorrect");
			assertEquals("Name", metadata.getColumnLabel(2), "label of column 2 is incorrect");
			assertEquals("DEPT", metadata.getColumnName(3), "name of column 3 is incorrect");
			assertEquals("DEPT", metadata.getColumnLabel(3), "label of column 3 is incorrect");
		}
	}

	@Test
	public void testDatabaseMetadataTableTypes() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath))
		{
			DatabaseMetaData metadata = conn.getMetaData();
			ResultSet results = metadata.getTableTypes();
			assertTrue(results.next());
			assertEquals("TABLE", results.getString(1), "Wrong table type");
			assertFalse(results.next());
		}
	}

	@Test
	public void testDatabaseMetadataSchemas() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath))
		{
			DatabaseMetaData metadata = conn.getMetaData();
			ResultSet results = metadata.getSchemas();
			assertFalse(results.next());
		}
	}

	@Test
	public void testDatabaseMetadataColumns() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			ResultSet results = conn.getMetaData().getColumns(null, null, "C D", null))
		{
			assertTrue(results.next());
			assertEquals("C D", results.getString("TABLE_NAME"), "Wrong table name");
			assertEquals("A", results.getString("COLUMN_NAME"), "Wrong column name");
			assertTrue(results.next());
			assertEquals("C D", results.getString("TABLE_NAME"), "Wrong table name");
			assertEquals("B", results.getString("COLUMN_NAME"), "Wrong column name");
		}
	}

	@Test
	public void testDatabaseMetadataColumnsWithIndexedFiles() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			ResultSet results = conn.getMetaData().getColumns(null, null, "test", null))
		{
			assertTrue(results.next());
			assertEquals("test", results.getString("TABLE_NAME"), "Wrong table name");
			assertEquals("Datum", results.getString("COLUMN_NAME"), "Wrong column name");
			assertTrue(results.next());
			assertEquals("test", results.getString("TABLE_NAME"), "Wrong table name");
			assertEquals("Tijd", results.getString("COLUMN_NAME"), "Wrong column name");
		}
	}

	@Test
	public void testDatabaseMetadataProcedures() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			ResultSet results = conn.getMetaData().getProcedures(null, null, "*"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testDatabaseMetadataUDTs() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			ResultSet results = conn.getMetaData().getUDTs(null, null, "test", null))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testDatabaseMetadataPrimaryKeys() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			ResultSet results = conn.getMetaData().getPrimaryKeys(null, null, "sample"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testDatabaseMetadataCatalogs() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			ResultSet results = conn.getMetaData().getCatalogs())
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testDatabaseMetadataTypeInfo() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			ResultSet results = conn.getMetaData().getTypeInfo())
		{
			assertTrue(results.next());
			assertEquals("String", results.getString("TYPE_NAME"), "TYPE_NAME is wrong");
			assertEquals(Types.VARCHAR, results.getInt("DATA_TYPE"), "DATA_TYPE is wrong");
			assertEquals(DatabaseMetaData.typeNullable, results.getShort("NULLABLE"), "NULLABLE is wrong");
		}
	}

	@Test
	public void testColumnTypesUserSpecified() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals(Integer.valueOf(1), results.getObject("id"),
				"Integer column ID is wrong");
			assertEquals(Integer.valueOf(1), results.getObject(1),
				"Integer column 1 is wrong");
			java.sql.Date shouldBe = java.sql.Date.valueOf("2001-01-02");
			assertEquals(shouldBe, results.getObject("start"),
				"Date column Start is wrong");
			assertEquals(shouldBe, results.getObject(4),
				"Date column 4 is wrong");
			assertEquals("Juan Pablo Morales", results.getObject("name"),
				"The Name is wrong");
		}
	}

	@Test
	public void testColumnTypesUserSpecifiedShuffled() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT ID, Start, Name, Job "
				+ "FROM sample5 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals(Integer.valueOf(1), results.getObject("id"),
				"Integer column ID is wrong");
			assertEquals(Integer.valueOf(1), results.getObject(1),
				"Integer column 1 is wrong");
			java.sql.Date shouldBe = java.sql.Date.valueOf("2001-01-02");
			assertEquals(shouldBe, results.getObject("start"),
				"Date column Start is wrong");
			assertEquals(shouldBe, results.getObject(2), "Date column 4 is wrong");
			assertEquals("Juan Pablo Morales", results.getObject("name"),
				"The Name is wrong");
		}
	}

	@Test
	public void testColumnTypesUserSpecifiedTS() throws SQLException,
			ParseException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			DateFormat dfp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			assertEquals(Integer.valueOf(1), results.getObject("id"),
				"Integer column ID is wrong");
			assertEquals(Integer.valueOf(1), results.getObject(1),
				"Integer column 1 is wrong");
			assertEquals(dfp.parse(results.getString("start")), results.getObject("start"),
				"Date column Start is wrong");
			assertEquals(dfp.parse(results.getString("start")), results.getObject(4),
				"Date column 4 is wrong");
			assertEquals("Juan Pablo Morales", results.getObject("name"),
				"The Name is wrong");
		}
	}

	/**
	 * @throws SQLException
	 * @throws ParseException
	 */
	@Test
	public void testColumnTypesInferFromData() throws SQLException,
			ParseException
	{
		Properties props = new Properties();
		props.put("columnTypes", "");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals(Types.INTEGER, metadata.getColumnType(1),
				"type of column 1 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(2),
				"type of column 2 is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(3),
				"type of column 3 is incorrect");
			assertEquals(Types.TIMESTAMP, metadata.getColumnType(4),
				"type of column 4 is incorrect");
		}
	}

	@Test
	public void testColumnTypesInferDateTimeFromData() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "");
		props.put("dateFormat", "dd-MMM-yy");
		props.put("timeFormat", "hh:mm:ss.SSS aa");
		if (!Locale.getDefault().equals(Locale.US))
		{
			/*
			 * Ensure that test passes when running on non-English language computers.
			 */
			props.put("locale", Locale.US.toString());
		}

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT ID, D, T "
				+ "FROM sunil_date_time"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals(Types.INTEGER, metadata.getColumnType(1),
				"type of column 1 is incorrect");
			assertEquals(Types.DATE, metadata.getColumnType(2),
				"type of column 2 is incorrect");
			assertEquals(Types.TIME, metadata.getColumnType(3),
				"type of column 3 is incorrect");
		}
	}

	@Test
	public void testColumnTypesInferBeforeNext() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM sample5"))
		{
			try
			{
				results.getMetaData();
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("cannotInferColumns"), "" + e);
			}
		}
	}

	/**
	 * @throws SQLException
	 * @throws ParseException
	 */
	@Test
	public void testColumnTypesNumeric() throws SQLException,
			ParseException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT C1, C2, C3, C4, C5, C6, C7 "
				+ "FROM numeric"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals(Types.TINYINT, metadata.getColumnType(1),
				"type of column 1 is incorrect");
			assertEquals(Types.SMALLINT, metadata.getColumnType(2),
				"type of column 2 is incorrect");
			assertEquals(Types.INTEGER, metadata.getColumnType(3),
				"type of column 3 is incorrect");
			assertEquals(Types.BIGINT, metadata.getColumnType(4),
				"type of column 4 is incorrect");
			assertEquals(Types.FLOAT, metadata.getColumnType(5),
				"type of column 5 is incorrect");
			assertEquals(Types.DOUBLE, metadata.getColumnType(6),
				"type of column 6 is incorrect");
			assertEquals(Types.DECIMAL, metadata.getColumnType(7),
				"type of column 7 is incorrect");
		}
	}

	@Test
	public void testColumnTypesDefaultBehaviour() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals("2001-01-02 12:30:00", results.getObject("start"),
				"the start time is wrong");
			assertEquals("01", results.getObject("id"), "The ID is wrong");
			assertEquals("Juan Pablo Morales", results.getObject("name"),
				"The Name is wrong");
		}
	}

	@Test
	public void testBadColumnTypesFails()
	{
		try
		{
			Properties props = new Properties();
			props.put("columnTypes", "Varchar,Varchar");
			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement())
			{
				try (ResultSet results = stmt.executeQuery("SELECT Id, Name FROM sample"))
				{
					fail("Should raise a java.sqlSQLException");
				}
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnType") + ": Varchar", "" + e);
		}
	}

	@Test
	public void testBadColumnNameFails()
	{
		try
		{
			Properties props = new Properties();
			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement())
			{
				try (ResultSet results = stmt.executeQuery("SELECT Id, XXXX FROM sample"))
				{
					fail("Should raise a java.sqlSQLException");
				}
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testEmptyColumnTypesFails()
	{
		try
		{
			Properties props = new Properties();
			props.put("columnTypes", ",");

			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement())
			{
				try (ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
				{
					fail("Should raise a java.sqlSQLException");
				}
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnType") + ": ,", "" + e);
		}
	}

	@Test
	public void testColumnTypesWithSelectStar() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * "
				+ "FROM sample5 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			String target = "2001-01-02 12:30:00";
			DateFormat toUTC = getUTCDateFormat();
			assertEquals(target, toUTC.format(results.getObject("start")),
				"the start time is wrong");
			assertEquals(Integer.valueOf(1), results.getObject("id"), "The ID is wrong");
			assertEquals("Juan Pablo Morales", results.getObject("name"),
				"The Name is wrong");
		}
	}

	@Test
	public void testColumnTypesWithMultipleTables() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes.sample5", "Int,String,String,Timestamp");
		props.put("columnTypes.sample", "String");
		// Give empty list so column types are inferred from data.
		props.put("columnTypes.numeric", "");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * from sample5"))
			{
				assertTrue(results.next());
				assertEquals(Integer.valueOf(41), results.getObject("id"), "The sample5 ID is wrong");
				assertEquals("Piloto", results.getObject("job"), "The sample5 Job is wrong");
			}
			try (ResultSet results = stmt.executeQuery("SELECT ID,EXTRA_FIELD from sample"))
			{
				assertTrue(results.next());
				assertEquals("Q123", results.getObject(1), "The sample ID is wrong");
				assertEquals("F", results.getObject(2), "The sample EXTRA_FIELD is wrong");
			}

			// column types are inferred from data.
			try (ResultSet results = stmt.executeQuery("SELECT C2, 'X' as X from numeric"))
			{
				assertTrue(results.next());
				assertEquals(Integer.valueOf(-1010), results.getObject(1), "The numeric C2 is wrong");
				assertEquals("X", results.getObject(2), "The numeric X is wrong");
			}
		}
	}

	@Test
	public void testWithSuppressedHeaders() throws SQLException
	{
		Properties props = new Properties();
		props.put("suppressHeaders", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			// header is now treated as normal data line
			results.next();
			assertTrue(results.getString("COLUMN1").equals("ID"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("NAME"),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("EXTRA_FIELD"),
				"Incorrect COLUMN3 Value");

			results.next();
			assertTrue(results.getString("COLUMN1").equals("Q123"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("\"S,\""),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("F"),
				"Incorrect COLUMN3 Value");

			results.next();
			assertTrue(results.getString("COLUMN1").equals("A123"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("Jonathan Ackerman"),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("A"),
				"Incorrect COLUMN3 Value");

			results.next();
			assertTrue(results.getString("COLUMN1").equals("B234"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("Grady O'Neil"),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("B"),
				"Incorrect COLUMN3 Value");

			results.next();
			assertTrue(results.getString("COLUMN1").equals("C456"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("Susan, Peter and Dave"),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("C"),
				"Incorrect COLUMN3 Value");

			results.next();
			assertTrue(results.getString("COLUMN1").equals("D789"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("Amelia \"meals\" Maurice"),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("E"),
				"Incorrect COLUMN3 Value");

			results.next();
			assertTrue(results.getString("COLUMN1").equals("X234"),
				"Incorrect COLUMN1 Value");
			assertTrue(results.getString("COLUMN2").equals("Peter \"peg leg\", Jimmy & Samantha \"Sam\""),
				"Incorrect COLUMN2 Value");
			assertTrue(results.getString("COLUMN3").equals("G"),
				"Incorrect COLUMN3 Value");
		}
	}

	@Test
	public void testWithSuppressedHeadersMultiline() throws SQLException
	{
		Properties props = new Properties();
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM embassies"))
		{
			// Test selecting from a file with a multi-line value as the first record.
			assertTrue(results.next());
			assertEquals("Germany", results.getString("COLUMN1"), "Incorrect COLUMN1 Value");
			assertEquals("Wallstrasse 76-79,\n10179 Berlin", results.getString("COLUMN2"), "Incorrect COLUMN2 Value");
			assertTrue(results.next());
		}
	}

	@Test
	public void testRelativePath() throws SQLException
	{
		// break up file path to test relative paths
		String parentPath = new File(filePath).getParent();
		String subPath = new File(filePath).getName();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ parentPath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM ."
						+ File.separator + subPath + File.separator + "sample"))
		{
			results.next();
			assertTrue(results.getString("ID").equals("Q123"), "Incorrect ID Value");
			assertTrue(results.getString("NAME").equals("\"S,\""),
				"Incorrect NAME Value");
			assertTrue(results.getString("EXTRA_FIELD").equals("F"),
				"Incorrect EXTRA_FIELD Value");
		}
	}

	@Test
	public void testWhereMultipleResult() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT ID, Name, Job FROM sample4 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals("01", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("04", results.getString("ID"), "The ID is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testFieldAsAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			//TODO using alias j in the WHERE clause is not valid SQL.  Should we really test this?
			ResultSet results = stmt
				.executeQuery("SELECT ID as i, Name as n, Job as j FROM sample4 WHERE j='Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals("01", results.getString("i"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("02", results.getString("i"), "The ID is wrong");
			assertEquals("Mauricio Hernandez", results.getString("N"),
				"The name is wrong");
			assertEquals("Mauricio Hernandez", results.getString(2),
				"The name is wrong");
			assertEquals("Project Manager", results.getString("J"),
				"The job is wrong");
			assertTrue(results.next());
			assertEquals("04", results.getString("i"), "The ID is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testSelectStar() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			CsvResultSet results = (CsvResultSet) stmt
				.executeQuery("SELECT * FROM sample4"))
		{
			assertEquals("ID", results.getMetaData().getColumnName(1).toString());
			assertEquals("ID", results.getMetaData().getColumnLabel(1).toString());

			assertTrue(results.next());
			assertEquals("01", results.getString("id"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("02", results.getString("id"), "The ID is wrong");
			assertEquals("Mauricio Hernandez", results.getString("Name"),
				"The name is wrong");
			assertEquals("Mauricio Hernandez", results.getString(2),
				"The name is wrong");
			assertEquals("Project Manager", results.getString("Job"),
				"The job is wrong");
			assertTrue(results.next());
			assertTrue(results.next());
			assertEquals("04", results.getString("id"), "The ID is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testSelectNull() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT ID, null as ID2 FROM sample4"))
		{
			assertEquals("ID2", results.getMetaData().getColumnName(2));
			assertTrue(results.next());
			assertEquals(null, results.getString("id2"), "The ID2 is wrong");
			assertTrue(results.next());
			assertEquals(null, results.getObject("id2"), "The ID2 is wrong");
		}
	}

	@Test
	public void testLiteralAsAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT Job j,ID i,Name n, 0 c FROM sample4"))
		{
			assertTrue(results.next(), "no results found - should be all");
			assertTrue(results.next(), "no results found - should be all");
			assertEquals("0", results.getString("c"), "The literal c is wrong");
			assertEquals("0", results.getString(4), "The literal c is wrong");
			assertEquals("Mauricio Hernandez", results.getString("N"),
				"The name is wrong");
			assertEquals("Project Manager", results.getString("J"),
				"The job is wrong");
		}
	}

	@Test
	public void testColumnWithoutAlias() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT 44, lower(job), ID*2, 'hello', 44 from sample4"))
		{
			assertTrue(results.next(), "no results found");
			assertEquals(44, results.getInt(1), "Number 44 is wrong");
			assertEquals("project manager", results.getString(2), "lower(job) is wrong");
			assertEquals(2, results.getInt(3), "ID*2 is wrong");
			assertEquals("hello", results.getString(4), "String 'hello' is wrong");
			assertEquals(44, results.getInt(5), "Number 44 is wrong");
		}
	}

	/**
	 * This returns no results with where and tests if this still works
	 *
	 * @throws SQLException
	 */
	@Test
	public void testWhereNoResults() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM sample4 WHERE ID='05'"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testSelectStarWhereMultipleResult() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE Job = 'Project Manager'"))
		{
			assertTrue(results.next());
			assertEquals("01", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("04", results.getString("ID"), "The ID is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testSelectStarWithTableAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT tbl.* FROM sample4 tbl"))
		{
			assertTrue(results.next());
			assertEquals("01", results.getString("ID"), "The ID is wrong");
			assertEquals("Project Manager", results.getString(3), "The Job is wrong");
		}
	}

	@Test
	public void testWhereWithAndOperator() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE Job = 'Project Manager' AND Name = 'Mauricio Hernandez'"))
		{
			assertTrue(results.next());
			assertEquals("02", results.getString("ID"), "The ID is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testWhereWithBetweenOperator() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE id BETWEEN '02' AND '03'"))
		{
			assertTrue(results.next());
			assertEquals(2, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt("ID"), "The ID is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testWhereWithBetweenDates() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM Purchase WHERE PurchaseDate BETWEEN '1/11/2013' AND '1/15/2013'"))
		{
			assertTrue(results.next());
			assertEquals(58375, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertTrue(results.next());
			assertEquals(34625, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertTrue(results.next());
			assertEquals(34771, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testWhereWithBetweenTimes() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM Purchase WHERE PurchaseTime BETWEEN '08:30:00' AND '10:00:00'"))
		{
			assertTrue(results.next());
			assertEquals(51002, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithBetweenTimestamps() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");
		props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE Start BETWEEN '2003-03-01 08:30:00' AND '2003-03-02 17:30:00'"))
		{
			assertTrue(results.next());
			assertEquals(3, results.getInt("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithLikeOperatorPercent() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE Name LIKE 'Ma%'"))
		{
			assertTrue(results.next());
			assertEquals(2, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithLikeOperatorUnderscore() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select id from sample where id like '_234'"))
		{
			assertTrue(results.next());
			assertEquals("B234", results.getString("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals("X234", results.getString("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithLikeOperatorEscape() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt
				.executeQuery("select ID from escape where ID like 'index\\__'"))
			{
				assertTrue(results.next());
				assertEquals("index_1", results.getString("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals("index_2", results.getString("ID"), "The ID is wrong");
				assertTrue(results.next());
				assertEquals("index_3", results.getString("ID"), "The ID is wrong");
				assertFalse(results.next());
			}

			try (ResultSet results2 = stmt
				.executeQuery("select ID from escape where ID like 'index^__' escape '^'"))
			{
				assertTrue(results2.next());
				assertEquals("index_1", results2.getString("ID"), "The ID is wrong");
				assertTrue(results2.next());
				assertEquals("index_2", results2.getString("ID"), "The ID is wrong");
				assertTrue(results2.next());
				assertEquals("index_3", results2.getString("ID"), "The ID is wrong");
				assertFalse(results2.next());
			}

			try (ResultSet results3 = stmt
				.executeQuery("select ID from escape where ID like 'index^%%' escape '^'"))
			{
				assertTrue(results3.next());
				assertEquals("index%%", results3.getString("ID"), "The ID is wrong");
				assertTrue(results3.next());
				assertEquals("index%3", results3.getString("ID"), "The ID is wrong");
				assertFalse(results3.next());
			}
		}
	}

	@Test
	public void testWhereWithLikeMultiLine() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select N from nikesh where Message like '%SSL%'"))
		{
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "N is wrong");
			assertTrue(results.next());
			assertEquals(2, results.getInt(1), "N is wrong");
			assertTrue(results.next());
			assertEquals(4, results.getInt(1), "N is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithUnselectedColumn() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT Name, Job FROM sample4 WHERE id = '04'"))
		{
			assertTrue(results.next());
			try
			{
				results.getString("id");
				fail("Should not find the column 'id'");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": id", "" + e);
			}
			assertEquals("Felipe Grajales", results.getString("name"),
				"The name is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testWhereWithBadColumnName()
	{
		try
		{
			Properties props = new Properties();
			try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

				Statement stmt = conn.createStatement())
			{
				try (ResultSet results = stmt.executeQuery("SELECT Id FROM sample where XXXX='123'"))
				{
					fail("Should raise a java.sqlSQLException");
				}
			}
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": XXXX", "" + e);
		}
	}

	@Test
	public void testWhereWithExponentialNumbers() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT c4 FROM numeric WHERE c4 >= 2.00e+6 and c4 <= 9e15 and c1 < 1.e-1"))
		{
			assertTrue(results.next());

			double d = results.getDouble("c4");
			long l = Math.round(d);
			assertEquals(l, 990000000000l, "The c4 is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithDates() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		props.put("dateFormat", "yyyy-MM-dd");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT ID from sample8 where d = '2010-02-02'"))
			{
				assertTrue(results.next());
				assertEquals(2, results.getInt(1), "The ID is wrong");
				assertFalse(results.next());
			}

			try (ResultSet results = stmt.executeQuery("SELECT ID from sample8 where '2010-03-24' < d"))
			{
				assertTrue(results.next());
				assertEquals(3, results.getInt(1), "The ID is wrong");
				assertTrue(results.next());
				assertEquals(6, results.getInt(1), "The ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testWhereWithTimes() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM Purchase WHERE PurchaseTime >= '12:00:00' and '12:59:59' >= PurchaseTime"))
		{
			assertTrue(results.next());
			assertEquals(34771, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithTimestamps() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");
		props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE Start >= '2002-01-01 00:00:00' and '2003-12-31 23:59:59' >= Start"))
		{
			assertTrue(results.next());
			assertEquals(2, results.getInt("ID"), "The ID is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithIn() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Name from sample5 where id in (3, 4, 5)"))
		{
			assertTrue(results.next());
			assertEquals("Maria Cristina Lucero", results.getString("Name"), "The Name is wrong");
			assertTrue(results.next());
			assertEquals("Felipe Grajales", results.getString("Name"), "The Name is wrong");
			assertTrue(results.next());
			assertEquals("Melquisedec Rojas Castillo", results.getString("Name"), "The Name is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithInNoResults() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Name from sample5 where id in (23, 24, 25)"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithNotIn() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select Id from sample where Id not in ('A123', 'B234', 'X234')"))
		{
			assertTrue(results.next());
			assertEquals("Q123", results.getString("Id"), "The Id is wrong");
			assertTrue(results.next());
			assertEquals("C456", results.getString("Id"), "The Id is wrong");
			assertTrue(results.next());
			assertEquals("D789", results.getString("Id"), "The Id is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithInEmpty() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement())
		{
			try
			{
				try (ResultSet results = stmt.executeQuery("select * from sample where Name in ()"))
				{
					fail("SQL Query should fail");
				}
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().startsWith(CsvResources.getString("syntaxError")));
			}
		}
	}

	@Test
	public void testWhereWithInDates() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM Purchase WHERE PurchaseDate IN ('1/9/2013', '1/16/2013')"))
		{
			assertTrue(results.next());
			assertEquals(19685, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertTrue(results.next());
			assertEquals(51002, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testWhereWithInTimes() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Int,Int,Date,Time");
		props.put("dateFormat", "M/D/YYYY");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM Purchase WHERE PurchaseTime IN ('10:10:06', '11:10:06')"))
		{
			assertTrue(results.next());
			assertEquals(22021, results.getInt("AccountNo"), "The AccountNo is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWhereWithInTimestamps() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");
		props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			// Note that final Timestamp is wrong format.
			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 " +
					"WHERE Start IN ('2002-02-02 12:30:00', '2004-04-02 12:00:00', '2004-04-02')"))
		{
			assertTrue(results.next());
			assertEquals(2, results.getInt("ID"), "The ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void test1073375() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("separator", "\t");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM jo"))
		{
			assertTrue(results.next());
			try
			{
				results.getString("id");
				fail("Should not find the column 'id'");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": id", "" + e);
			}
			assertEquals("3", results.getString("rc"), "The name is wrong");
			// would like to test the full_name_nd, but can't insert the Arabic
			// string in the code
			assertTrue(results.next());
			assertEquals("3", results.getString("rc"), "The name is wrong");
			assertTrue(results.next());
			assertEquals("3", results.getString("rc"), "The name is wrong");
			assertEquals("Tall Dhayl", results.getString("full_name_nd"), "The name is wrong");
		}
	}

	@Test
	public void test0733215() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM witheol"))
		{
			assertTrue(results.next());
			assertEquals("1", results.getString("key"), "The name is wrong");
			// would like to test the full_name_nd, but can't insert the Arabic
			// string in the code
			assertTrue(results.next());
			assertEquals("2", results.getString("key"), "The name is wrong");
			assertTrue(results.next());
			assertEquals("3", results.getString("key"), "The name is wrong");
			assertEquals("123\n456\n789", results.getString("value"),
				"The name is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void test3091923() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".csv");
		props.put("separator", ";");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM badquoted"))
		{
			assertTrue(results.next());
			assertEquals("Rechtsform unbekannt", results.getString("F2"), "The name is wrong");
			assertTrue(results.next());
			assertEquals("Rechtsform \nunbekannt", results.getString("F2"), "The name is wrong");
			assertTrue(results.next());
			assertEquals("Rechtsform unbekannt", results.getString("F2"), "The name is wrong");
			try
			{
				results.next();
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("eofInQuotes") + ": 6", "" + e);
			}
		}
	}

	@Test
	public void testColumnWithDot() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT datum, tijd, station, ai007.000 as value FROM test-001-20081112"))
		{
			assertTrue(results.next());
			assertEquals("20-12-2007", results.getString("datum"), "The name is wrong");
			assertEquals("10:59:00", results.getString("tijd"), "The name is wrong");
			assertEquals("007", results.getString("station"), "The name is wrong");
			assertEquals("0.0", results.getString("value"), "The name is wrong");
		}
	}

	/**
	 * accessing indexed files that do not exist is the same as accessing an
	 * empty table. no "file not found" error
	 *
	 * @throws SQLException
	 */
	@Test
	public void testFromNonExistingIndexedTable() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT location,station,datum,tijd,file_date FROM test57"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testFromIndexedTable() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT location,station,datum,tijd,file_date FROM test"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertTrue(metadata.getTableName(0).equals("test"), "Incorrect Table Name");

			assertEquals(metadata.getColumnName(1),	"location", "Incorrect Column Name 1");
			assertEquals(metadata.getColumnName(2),	"Station", "Incorrect Column Name 2");
			assertEquals(metadata.getColumnName(3),	"Datum", "Incorrect Column Name 3");
			assertEquals(metadata.getColumnName(4),	"Tijd", "Incorrect Column Name 4");

			assertTrue(results.next());
			for (int i = 1; i < 12; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 12; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 12; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 36; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 36; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 36; i++)
			{
				assertTrue(results.next());
			}
			assertFalse(results.next());
		}
	}

	@Test
	public void testFromIndexedTablePrepend() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");
		props.put("fileTailPrepend", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT location,file_date,datum,tijd,station FROM test"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertTrue(metadata.getTableName(0).equals("test"), "Incorrect Table Name");

			assertEquals(metadata.getColumnName(1),	"location", "Incorrect Column Name 1");
			assertEquals(metadata.getColumnName(2),	"file_date", "Incorrect Column Name 1");
			assertEquals(metadata.getColumnName(3),	"Datum", "Incorrect Column Name 1");
			assertEquals(metadata.getColumnName(4),	"Tijd", "Incorrect Column Name 2");
			assertEquals(metadata.getColumnName(5),	"Station", "Incorrect Column Name 3");

			assertTrue(results.next());
			for (int i = 1; i < 12; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 12; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 12; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 36; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 36; i++)
			{
				assertTrue(results.next());
			}
			for (int i = 0; i < 36; i++)
			{
				assertTrue(results.next());
			}
			assertFalse(results.next());
		}
	}

	@Test
	public void testNoPatternGroupFromIndexedTable() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("indexedFiles", "true");

		// No groups in ()'s used in regular expression
		props.put("fileTailPattern", ".*");
		props.put("suppressHeaders", "true");
		props.put("headerline", "BLZ,BANK_NAME");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM banks"))
		{
			assertTrue(results.next());
			assertEquals("10000000", results.getString("BLZ"), "The BLZ is wrong");
		}
	}

	@Test
	public void testAddingFields() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id + ' ' + job as mix FROM sample4"))
		{
			assertTrue(results.next());
			assertEquals("01 Project Manager", results.getString("mix"),
				"The mix is wrong");
			assertTrue(results.next());
			assertEquals("02 Project Manager", results.getString("mix"),
				"The mix is wrong");
			assertTrue(results.next());
			assertEquals("03 Finance Manager", results.getString("mix"),
				"The mix is wrong");
			assertTrue(results.next());
			assertEquals("04 Project Manager", results.getString("mix"),
				"The mix is wrong");
			assertTrue(!results.next());
		}
	}

	@Test
	public void testReadingTime() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id, timeoffset FROM sample5"))
		{
			assertTrue(results.next());
			Object expect = java.sql.Time.valueOf("12:30:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("12:35:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("12:40:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("12:45:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("01:00:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("01:00:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("01:00:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("00:00:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("00:10:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertTrue(results.next());
			expect = java.sql.Time.valueOf("01:23:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");

			assertFalse(results.next());
		}
	}

	@Test
	public void testAddingDatePlusTime() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id, start, timeoffset, start+timeoffset AS ts FROM sample5 WHERE id=41 OR id=4"))
		{
			assertTrue(results.next());
			Object expect = java.sql.Date.valueOf("2001-04-02");
			assertEquals(expect.getClass(), results.getObject("start").getClass(),
				"Date is a Date");
			assertEquals(expect, results.getObject("start"), "Date is a Date");
			expect = java.sql.Time.valueOf("12:30:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");
			expect = java.sql.Timestamp.valueOf("2001-04-02 12:30:00");
			assertEquals(expect.getClass(), results.getObject("ts").getClass(),
				"adding Date to Time");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals(((Timestamp) expect).toString(), toUTC.format(results.getObject("ts")) + ".0",
				"adding Date to Time");

			assertTrue(results.next());
			expect = java.sql.Date.valueOf("2004-04-02");
			assertEquals(expect.getClass(), results.getObject("start").getClass(),
				"Date is a Date");
			assertEquals(expect, results.getObject("start"), "Date is a Date");
			expect = java.sql.Time.valueOf("01:00:00");
			assertEquals(expect.getClass(), results.getObject("timeoffset").getClass(),
				"Time is a Time");
			assertEquals(expect, results.getObject("timeoffset"), "Time is a Time");
			expect = java.sql.Timestamp.valueOf("2004-04-02 01:00:00");
			assertEquals(expect.getClass(), results.getObject("ts").getClass(),
				"adding Date to Time");
			assertEquals(((Timestamp) expect).toString(), toUTC.format(results.getObject("ts")) + ".0",
				"adding Date to Time");

			assertFalse(results.next());
		}
	}

	@Test
	public void testAddingTimestampPlusInteger() throws SQLException
	{
		// misusing Date+Time to get a Timestamp, but here we are just
		// interested in doing Timestamp +/- Integer
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT id, start+timeoffset+61000 AS ts, start+timeoffset-61000 AS ts2 FROM sample5 WHERE id=41 OR id=4"))
		{
			assertTrue(results.next());
			Object expect = java.sql.Timestamp.valueOf("2001-04-02 12:31:01");
			assertEquals(expect.getClass(), results.getObject("ts").getClass(),
				"adding Date + Time + Int");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals(((Timestamp) expect).toString(), toUTC.format(results.getObject("ts")) + ".0",
				"adding Date to Time");
			expect = java.sql.Timestamp.valueOf("2001-04-02 12:28:59");
			assertEquals(expect.getClass(), results.getObject("ts2").getClass(),
				"adding Date + Time - Int");
			assertEquals(((Timestamp) expect).toString(), toUTC.format(results.getObject("ts2")) + ".0",
				"adding Date to Time");

			assertTrue(results.next());
			expect = java.sql.Timestamp.valueOf("2004-04-02 01:01:01");
			assertEquals(expect.getClass(), results.getObject("ts").getClass(),
				"adding Date to Time");
			assertEquals(((Timestamp) expect).toString(), toUTC.format(results.getObject("ts")) + ".0",
				"adding Date to Time");
			expect = java.sql.Timestamp.valueOf("2004-04-02 00:58:59");
			assertEquals(expect.getClass(), results.getObject("ts2").getClass(),
				"adding Date to Time");
			assertEquals(((Timestamp) expect).toString(), toUTC.format(results.getObject("ts2")) + ".0",
				"adding Date to Time");

			assertFalse(results.next());
		}
	}

	@Test
	public void testAddingAndMultiplyingFields() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT 1 + ID * 2 as N1, ID * -3 + 1 as N2, ID+1+2*3+4 as N3 FROM sample5"))
		{
			assertTrue(results.next());
			assertEquals(83, results.getInt("N1"), "N1 is wrong");
			assertEquals(-122, results.getInt("N2"), "N2 is wrong");
			assertEquals(52, results.getInt("N3"), "N3 is wrong");
		}
	}

	@Test
	public void testParentheses() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT ( ID + 1 ) as N1, ((ID + 2) * 3) as N2, (3) as N3 FROM sample5 where ( Job = 'Piloto' )"))
		{
			assertTrue(results.next());
			assertEquals(42, results.getInt("N1"), "N1 is wrong");
			assertEquals(129, results.getInt("N2"), "N2 is wrong");
			assertEquals(3, results.getInt("N3"), "N3 is wrong");
		}
	}

	@Test
	public void testBadParenthesesFails() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement())
		{
			try
			{
				try (ResultSet results = stmt.executeQuery("SELECT ((ID + 1) as N1 FROM sample5"))
				{
					fail("Should raise a java.sqlSQLException");
				}
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().startsWith(CsvResources.getString("syntaxError")));
			}
		}
	}

	@Test
	public void testLowerFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("select lower(job) as ljob, lower('AA') as CAT from sample5"))
			{
				assertTrue(results.next());
				assertEquals("piloto", results.getString(1), "The JOB is wrong");
				assertEquals("aa", results.getString(2), "The CAT is wrong");
				assertTrue(results.next());
				assertEquals("project manager", results.getString(1), "The JOB is wrong");
				assertEquals("aa", results.getString(2), "The CAT is wrong");
			}

			try (ResultSet results = stmt.executeQuery("select ID from sample5 where lower(job) = lower('FINANCE MANAGER')"))
			{
				assertTrue(results.next());
				assertEquals(2, results.getInt(1), "The ID is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testUpperFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,String");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("select UPPER(BANK_NAME) as UC, UPPER('Credit' + 'a') as N2, upper(7) as N3 from banks"))
			{
				assertTrue(results.next());
				assertEquals("BUNDESBANK (BERLIN)", results.getString(1), "The BANK_NAME is wrong");
				assertEquals("CREDITA", results.getString(2), "N2 is wrong");
				assertEquals("7", results.getString(3), "N3 is wrong");
			}

			try (ResultSet results = stmt.executeQuery("select BLZ from banks where UPPER(BANK_NAME) = 'POSTBANK (BERLIN)'"))
			{
				assertTrue(results.next());
				assertEquals(10010010, results.getInt(1), "The BLZ is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testLengthFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		props.put("charset", "UTF-8");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select length(Name) as X, length(Job) as Y, length('') as Z from sample5 where id = 8"))
		{
			assertTrue(results.next());
			assertEquals(27, results.getInt(1), "The Length is wrong");
			assertEquals(15, results.getInt(2), "The Length is wrong");
			assertEquals(0, results.getInt(3), "The Length is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testTrimFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Int,Float,String");
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("select TRIM(comment), TRIM('\tfoo bar\n') from with_comments"))
			{
				assertTrue(results.next());
				assertEquals("some field", results.getString(1), "The comment is wrong");
				assertEquals("foo bar", results.getString(2), "The trimmed value is wrong");
				assertTrue(results.next());
				assertEquals("other parameter", results.getString(1), "The comment is wrong");
				assertTrue(results.next());
				assertEquals("still a field", results.getString(1), "The comment is wrong");
				assertFalse(results.next());
			}

			try (ResultSet results = stmt.executeQuery("select TRIM(name, '#'), TRIM(name, '#h'), TRIM('00000', '0') from with_comments"))
			{
				assertTrue(results.next());
				assertEquals("alpha", results.getString(1), "The trimmed value is wrong");
				assertEquals("alpha", results.getString(2), "The trimmed value is wrong");
				assertEquals("", results.getString(3), "The trimmed value is wrong");
				assertTrue(results.next());
				assertEquals("beta", results.getString(1), "The trimmed value is wrong");
				assertEquals("beta", results.getString(2), "The trimmed value is wrong");
				assertTrue(results.next());
				assertEquals("hash", results.getString(1), "The trimmed value is wrong");
				assertEquals("as", results.getString(2), "The trimmed value is wrong");
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testLTrimFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT LTRIM(TO_ACCT,'0'), LTRIM('0000','0'), LTRIM('','0'), LTRIM('  X  ') FROM transactions"))
		{
			assertTrue(results.next());
			assertEquals("27853256", results.getString(1), "The trimmed value is wrong");
			assertEquals("", results.getString(2), "The trimmed value is wrong");
			assertEquals("", results.getString(3), "The trimmed value is wrong");
			assertEquals("X  ", results.getString(4), "The trimmed value is wrong");
			assertTrue(results.next());
			assertEquals("27234813", results.getString(1), "The trimmed value is wrong");
			assertTrue(results.next());
			assertEquals("81824588", results.getString(1), "The trimmed value is wrong");
		}
	}

	@Test
	public void testRTrimFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT RTRIM(BLZ,'0'), RTRIM(' ZZ ') FROM banks"))
		{
			assertTrue(results.next());
			assertEquals("1", results.getString(1), "The trimmed value is wrong");
			assertEquals(" ZZ", results.getString(2), "The trimmed value is wrong");
			assertTrue(results.next());
			assertEquals("1001001", results.getString(1), "The trimmed value is wrong");
			assertTrue(results.next());
			assertEquals("10010111", results.getString(1), "The trimmed value is wrong");
		}
	}

	@Test
	public void testSubstringFunction() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT substring(name, 2), substring(name, 3, 4), substring(name, 200) FROM sample4"))
		{
			assertTrue(results.next());
			assertEquals("uan Pablo Morales", results.getString(1), "The substring is wrong");
			assertEquals("an P", results.getString(2), "The substring is wrong");
			assertEquals("", results.getString(3), "The substring is wrong");
		}
	}

	@Test
	public void testReplaceFunction() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT replace(name, ' ', '_'), replace(name, 'foo', 'bar'), " +
					"replace(id, '0', ''), replace('abcd0123', id, job) FROM sample4"))
		{
			assertTrue(results.next());
			assertEquals("Juan_Pablo_Morales", results.getString(1), "The replace is wrong");
			assertEquals("Juan Pablo Morales", results.getString(2), "The replace is wrong");
			assertEquals("1", results.getString(3), "The replace is wrong");
			assertEquals("abcdProject Manager23", results.getString(4), "The replace is wrong");
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
	public void testAbsFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select ABS(C2) as R1, ABS(C5) as R2, ABS('-123456') as R3 from numeric"))
		{
			assertTrue(results.next());
			assertEquals(1010, results.getInt(1), "R1 is wrong");
			assertTrue(fuzzyEquals(results.getDouble(2), 3.14), "R2 is wrong");
			assertTrue(fuzzyEquals(results.getDouble(3), 123456), "R3 is wrong");
			assertTrue(results.next());
			assertEquals(15, results.getInt(1), "R1 is wrong");
			assertTrue(fuzzyEquals(results.getDouble(2), 0.0), "R2 is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testRoundFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select"
					+ " ROUND(11.77) as R1,"
					+ " ROUND('11.77') as R2,"
					+ " ROUND(C2) as R3,"
					+ " round(C3/7.0) as R4,"
					+ " ROUND(11.77, 1) as R5,"
					+ " ROUND(11.74, 1) as R6,"
					+ " ROUND(11.75, 1) as R7,"
					+ " ROUND('11.77', '1') as R8,"
					+ " ROUND(C2, 1) as R9,"
					+ " round(C3/7.0, 1) as R10,"
					+ " round(C3/7.0, 2) as R11,"
					+ " round(C3/7.0, '3') as R12,"
					+ " ROUND(11.77, 0) as R13,"
					+ " ROUND(11.77, -1) as R14"
					+ " from numeric"
					+ " where ROUND(C5) = 3"))
		{
			assertTrue(results.next());
			assertEquals(12, results.getInt(1), "R1 is wrong");
			assertEquals(12, results.getInt(2), "R2 is wrong");
			assertEquals(-1010, results.getInt(3), "R3 is wrong");
			assertEquals(42871, results.getInt(4), "R4 is wrong");
			assertEquals(11.8, results.getDouble(5), 0.01, "R5 is wrong");
			assertEquals(11.7, results.getDouble(6), 0.01, "R6 is wrong");
			assertEquals(11.8, results.getDouble(7), 0.01, "R7 is wrong");
			assertEquals(11.8, results.getDouble(8), 0.01, "R8 is wrong");
			assertEquals(-1010.0, results.getDouble(9), 0.01, "R9 is wrong");
			assertEquals(42871.4, results.getDouble(10), 0.01, "R10 is wrong");
			assertEquals(42871.43, results.getDouble(11), 0.001, "R11 is wrong");
			assertEquals(42871.429, results.getDouble(12), 0.0001, "R12 is wrong");
			assertEquals(12, results.getDouble(13), 0.01, "R13 is wrong");
			assertEquals(10, results.getDouble(14), 0.01, "R14 is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testDayOfMonthFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");
		props.put("timeZoneName", TimeZone.getDefault().getID());

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select dayofmonth(d) as dom, " +
				"dayofmonth('2013-10-13') as today from sample8"))
		{
			assertTrue(results.next());
			assertEquals(2, results.getInt(1), "dom is wrong");
			assertEquals(13, results.getInt(2), "today is wrong");
			assertTrue(results.next());
			assertEquals(2, results.getInt(1), "dom is wrong");
			assertTrue(results.next());
			assertEquals(28, results.getInt(1), "dom is wrong");
		}
	}

	@Test
	public void testMonthFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select month(d) as month, " +
				"month('2013-10-13') as today from sample8"))
		{
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "month is wrong");
			assertEquals(10, results.getInt(2), "today is wrong");
			assertTrue(results.next());
			assertEquals(2, results.getInt(1), "dom is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt(1), "dom is wrong");
		}
	}

	@Test
	public void testYearFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");
		props.put("timeZoneName", TimeZone.getDefault().getID());

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select year(d) as year, " +
				"year('2013-10-13') as today from sample8"))
		{
			assertTrue(results.next());
			assertEquals(2010, results.getInt(1), "month is wrong");
			assertEquals(2013, results.getInt(2), "today is wrong");
			assertTrue(results.next());
			assertEquals(2010, results.getInt(1), "dom is wrong");
			assertTrue(results.next());
			assertEquals(2010, results.getInt(1), "dom is wrong");
			assertTrue(results.next());
			assertEquals(2010, results.getInt(1), "dom is wrong");
			assertTrue(results.next());
			assertEquals(2009, results.getInt(1), "dom is wrong");
		}
	}

	@Test
	public void testHourOfDayFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select hourofday(t) as hour, " +
				"hourofday('23:41:17') as h from sample8"))
		{
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "hour is wrong");
			assertEquals(23, results.getInt(2), "h is wrong");
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "hour is wrong");
			assertTrue(results.next());
			assertEquals(1, results.getInt(1), "hour is wrong");
			assertTrue(results.next());
			assertEquals(5, results.getInt(1), "hour is wrong");
		}
	}

	@Test
	public void testMinuteFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select minute(t) as minute, " +
				"minute('23:41:17') as m from sample8"))
		{
			assertTrue(results.next());
			assertEquals(30, results.getInt(1), "minute is wrong");
			assertEquals(41, results.getInt(2), "m is wrong");
		}
	}

	@Test
	public void testDateFunctionsWithTimestamp() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		props.put("timeZoneName", TimeZone.getDefault().getID());

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("select dayofmonth(Start), month(Start), year(Start), " +
				"hourofday(Start), minute(Start), second(Start) from sample5"))
			{
				assertTrue(results.next());
				assertEquals(2, results.getInt(1), "dayofmonth is wrong");
				assertEquals(4, results.getInt(2), "month is wrong");
				assertEquals(2001, results.getInt(3), "year is wrong");
				assertEquals(12, results.getInt(4), "hourofday is wrong");
				assertEquals(30, results.getInt(5), "minute is wrong");
				assertEquals(0, results.getInt(6), "second is wrong");
			}

			String timestamp = "2013-10-13 14:33:55";
			try (ResultSet results = stmt.executeQuery("select dayofmonth('" + timestamp + "')," +
				"month('" + timestamp + "'), year('" + timestamp + "'), " +
				"hourofday('" + timestamp + "'), minute('" + timestamp + "'), " +
				"second('" + timestamp + "') from sample5"))
			{
				assertTrue(results.next());
				assertEquals(13, results.getInt(1), "dayofmonth is wrong");
				assertEquals(10, results.getInt(2), "month is wrong");
				assertEquals(2013, results.getInt(3), "year is wrong");
				assertEquals(14, results.getInt(4), "hourofday is wrong");
				assertEquals(33, results.getInt(5), "minute is wrong");
				assertEquals(55, results.getInt(6), "second is wrong");
			}
		}
	}

	@Test
	public void testLongPlusDate() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Date,Long,String");
		props.put("dateFormat", "yyyy-MM-dd");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

            ResultSet results = stmt.executeQuery("select start_date + duration, duration + start_date from events"))
		{
			assertTrue(results.next());
			java.sql.Date expect = java.sql.Date.valueOf("2024-01-01");
			assertEquals(expect, results.getDate(1), "start_date plus duration is wrong");
			assertEquals(expect, results.getDate(2), "duration plus start_date is wrong");
			assertTrue(results.next());
			expect = java.sql.Date.valueOf("2023-04-01");
			assertEquals(expect, results.getDate(1), "start_date plus duration is wrong");
			assertEquals(expect, results.getDate(2), "duration plus start_date is wrong");
		}
	}

	@Test
	public void testNullIfFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("trimHeaders", "true");
		props.put("trimValues", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select nullif(key, ' - ') as k2 from foodstuffs"))
		{
			assertTrue(results.next());
			assertEquals("orange", results.getString(1), "K2 is wrong");
			assertTrue(results.next());
			assertEquals("apple", results.getString(1), "K2 is wrong");
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next());
			assertEquals(null, results.getString(1), "K2 is wrong");
			assertTrue(results.wasNull());
			assertFalse(results.next());
		}
	}

	@Test
	public void testCoalesceFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT COALESCE(ID, 999) FROM bad_values"))
		{
			assertTrue(results.next());
			assertEquals(999, results.getInt(1), "ID is wrong");
			assertTrue(results.next());
			assertEquals(999, results.getInt(1), "ID is wrong");
			assertTrue(results.next());
			assertEquals(3, results.getInt(1), "ID is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testWithComments() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Int,Float,String");
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM with_comments"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("name", metadata.getColumnName(1));
			assertEquals("id", metadata.getColumnName(2));
			assertEquals("value", metadata.getColumnName(3));
			assertEquals("comment", metadata.getColumnName(4));

			assertTrue(results.next());
			assertEquals(Integer.valueOf(1), results.getObject(2));
			assertTrue(results.next());
			assertEquals(Integer.valueOf(2), results.getObject(2));
			assertTrue(results.next());
			assertEquals(Integer.valueOf(3), results.getObject(2));
			assertFalse(results.next());
		}
	}

	@Test
	public void testWithoutComments() throws SQLException
	{
		Properties props = new Properties();
		props.put("commentChar", "");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM sample5"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("ID", metadata.getColumnName(1));
			assertEquals("Name", metadata.getColumnName(2));
			assertEquals("Job", metadata.getColumnName(3));
			assertEquals("Start", metadata.getColumnName(4));

			assertTrue(results.next());
			assertEquals("41", results.getObject(1));
			assertTrue(results.next());
			assertEquals("01", results.getObject(1));
			assertTrue(results.next());
			assertEquals("02", results.getObject(1));
			assertTrue(results.next());
			assertEquals("03", results.getObject(1));
			assertTrue(results.next());
			assertEquals("04", results.getObject(1));
			assertTrue(results.next());
			assertEquals("05", results.getObject(1));
			assertTrue(results.next());
			assertEquals("06", results.getObject(1));
			assertTrue(results.next());
			assertEquals("07", results.getObject(1));
			assertTrue(results.next());
			assertEquals("08", results.getObject(1));
			assertTrue(results.next());
			assertEquals("09", results.getObject(1));
			assertFalse(results.next());
		}
	}

	@Test
	public void testSkippingUtf8ByteOrderMark() throws SQLException
	{
		Properties props = new Properties();
		props.put("charset", "UTF-8");

		/*
		 * Check that the 3 byte Byte Order Mark at start of file is skipped.
		 */
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM utf8_bom"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("foo", metadata.getColumnName(1), "name of column 1 is incorrect");
			assertEquals("bar", metadata.getColumnName(2), "name of column 2 is incorrect");
			assertEquals("1", results.getString(1), "Incorrect value 1");
			assertEquals("3", results.getString(2), "Incorrect value 2");
		}
	}

	@Test
	public void testSkippingLeadingLines() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,Int,Float,String");
		props.put("skipLeadingLines", "3");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM with_comments"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("name", metadata.getColumnName(1));
			assertEquals("id", metadata.getColumnName(2));
			assertEquals("value", metadata.getColumnName(3));
			assertEquals("comment", metadata.getColumnName(4));

			assertTrue(results.next());
			assertEquals(Integer.valueOf(1), results.getObject(2));
			assertTrue(results.next());
			assertEquals(Integer.valueOf(2), results.getObject(2));
			assertTrue(results.next());
			assertEquals(Integer.valueOf(3), results.getObject(2));
			assertFalse(results.next());
		}
	}

	@Test
	public void testNonParseableWithColumnTypes() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "String,String,String,String,Int,Float");
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");
		props.put("suppressHeaders", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM with_leading_trash"))
		{
			assertTrue(results.next());
			assertEquals("12:20", results.getString(2));
			assertTrue(results.next());
			assertEquals("12:30", results.getString(2));
			assertTrue(results.next());
			assertEquals("12:40", results.getObject(2));
			assertFalse(results.next());
		}
	}

	@Test
	public void testNonParseableWithHeaderline() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "Date;Time;TimeZone;Unit;Quality;Value");
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");
		props.put("suppressHeaders", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM with_leading_trash"))
		{
			assertTrue(results.next());
			assertEquals("12:20", results.getString(2));
			assertTrue(results.next());
			assertEquals("12:30", results.getString(2));
			assertTrue(results.next());
			assertEquals("12:40", results.getObject(2));
			assertFalse(results.next());
		}
	}

	@Test
	public void testNonParseable() throws SQLException
	{
		Properties props = new Properties();
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM with_leading_trash"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("12:20", metadata.getColumnName(2));

			assertTrue(results.next());
			assertEquals("12:30", results.getString(2));
			assertTrue(results.next());
			assertEquals("12:40", results.getObject(2));
			assertFalse(results.next());
		}
	}

	@Test
	public void testNonParseableMultiline() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "COUNTRY,ADDRESS");
		props.put("suppressHeaders", "true");
		props.put("ignoreNonParseableLines", "True");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM embassies"))
		{
			assertTrue(results.next());
			assertEquals("Germany", results.getString(1));
			assertTrue(results.next());
			assertEquals("United Kingdom", results.getString(1));
			assertFalse(results.next());
		}
	}

	@Test
	public void testNonParseableLogging() throws SQLException
	{
		Properties props = new Properties();
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement())
		{
			StringWriter sw = new StringWriter();
			PrintWriter logger = new PrintWriter(sw, true);
			DriverManager.setLogWriter(logger);

			try (ResultSet results = stmt.executeQuery("SELECT * FROM with_leading_trash"))
			{
				while (results.next())
				{

				}
			}
			String logMessages = sw.getBuffer().toString();

			/*
			 * Check that non-parseables lines were logged.
			 */
			assertTrue(logMessages.contains("Databank=MSW"));
			assertTrue(logMessages.contains("Locatie=DENH"));
		}
	}

	@Test
	public void testMissingValue() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ":");
		props.put("fileExtension", ".log");
		props.put("missingValue", "$$");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM recording-2015-06-28"))
		{
			assertTrue(results.next());
			assertEquals("2015-01-01", results.getString(1));
			assertEquals("start", results.getString(2));
			assertEquals("$$", results.getString(3));
			assertEquals("$$", results.getString(4));
			assertTrue(results.next());
			assertEquals("2015-01-02", results.getString(1));
			assertEquals("new", results.getString(2));
			assertEquals("event", results.getString(3));
			assertEquals("$$", results.getString(4));
			assertTrue(results.next());
			assertEquals("2015-01-03", results.getString(1));
			assertEquals("repeat", results.getString(2));
			assertEquals("previous", results.getString(3));
			assertEquals("100", results.getString(4));
			assertFalse(results.next());
		}
	}

	@Test
	public void testBadColumnValues() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select ID, ID + 1, NAME, START_DATE, START_DATE + 1, START_TIME from bad_values"))
		{
			assertTrue(results.next());

			/*
			 * Check that SQL NULL is returned for empty or invalid numeric, date and time fields,
			 * and that zero is returned from methods that return a number.
			 */
			assertEquals(0, results.getInt("ID"), "ID is wrong");
			assertTrue(results.wasNull());
			assertEquals(0, results.getInt(2), "ID + 1 is wrong");
			assertTrue(results.wasNull());
			assertEquals("Simon", results.getString(3), "NAME is wrong");
			assertFalse(results.wasNull());
			assertNull(results.getDate(4), "START_DATE is wrong");
			assertTrue(results.wasNull());
			assertNull(results.getDate(5), "START_DATE + 1 is wrong");
			assertTrue(results.wasNull());
			assertNull(results.getTime(6), "START_TIME is wrong");
			assertTrue(results.wasNull());

			assertTrue(results.next());

			assertNull(results.getObject("ID"), "ID is wrong");
			assertTrue(results.wasNull());
			assertNull(results.getObject(2), "ID + 1 is wrong");
			assertTrue(results.wasNull());
			assertEquals("Wally", results.getString(3), "NAME is wrong");
			assertFalse(results.wasNull());
			assertNull(results.getObject(4), "START_DATE is wrong");
			assertTrue(results.wasNull());
			assertNull(results.getObject(5), "START_DATE + 1 is wrong");
			assertTrue(results.wasNull());
			assertNull(results.getObject(6), "START_TIME is wrong");
			assertTrue(results.wasNull());

			assertTrue(results.next());
		}
	}

	@Test
	public void testVariableColumnCount() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		props.put("fileTailPrepend", "True");
		props.put("columnTypes", "String,Date,Time,String,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM varlen1"))
			{
				ResultSetMetaData metadata = results.getMetaData();
				assertEquals("file_date", metadata.getColumnName(1));
				assertEquals("Datum", metadata.getColumnName(2));
				assertEquals("Tijd", metadata.getColumnName(3));
				assertEquals("Station", metadata.getColumnName(4));
				assertEquals("P000", metadata.getColumnName(5));
				assertEquals("P001", metadata.getColumnName(6));
				assertEquals("P002", metadata.getColumnName(7));
				assertEquals("P003", metadata.getColumnName(8));
			}

			try (ResultSet results = stmt.executeQuery("SELECT * FROM varlen2"))
			{
				ResultSetMetaData metadata = results.getMetaData();
				assertEquals("file_date", metadata.getColumnName(1));
				assertEquals("Datum", metadata.getColumnName(2));
				assertEquals("Tijd", metadata.getColumnName(3));
				assertEquals("Station", metadata.getColumnName(4));
				assertEquals("P000", metadata.getColumnName(5));
				assertEquals("P001", metadata.getColumnName(6));
			}

			try (ResultSet results = stmt.executeQuery("SELECT * FROM varlen1"))
			{
				assertTrue(results.next());
				assertEquals("007", results.getObject("Station"));
				assertEquals(Double.valueOf("26.54"), results.getObject("P003"));
				assertTrue(results.next());
				assertEquals("007", results.getObject("Station"));
				assertEquals(Double.valueOf("26.54"), results.getObject("P003"));
				assertTrue(results.next());
				assertEquals("007", results.getObject("Station"));
				assertEquals(Double.valueOf("26.54"), results.getObject("P003"));
				assertTrue(results.next());
				assertEquals("001", results.getObject("Station"));
				assertEquals(Double.valueOf("26.55"), results.getObject("P003"));
				assertFalse(results.next());
			}

			try (ResultSet results = stmt.executeQuery("SELECT * FROM varlen2"))
			{
				assertTrue(results.next());
				assertEquals("007", results.getObject("Station"));
				assertTrue(results.next());
				assertEquals("007", results.getObject("Station"));
				assertTrue(results.next());
				assertEquals("013", results.getObject("Station"));
				assertTrue(results.next());
				assertEquals("013", results.getObject("Station"));
				assertFalse(results.next());
			}
		}
	}

	@Test
	public void testTailPrepend() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		props.put("fileTailPrepend", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM varlen1"))
			{
				ResultSetMetaData metadata = results.getMetaData();
				assertEquals("file_date", metadata.getColumnName(1));
				assertEquals("Datum", metadata.getColumnName(2));
				assertEquals("Tijd", metadata.getColumnName(3));
				assertEquals("Station", metadata.getColumnName(4));
				assertEquals("P000", metadata.getColumnName(5));
				assertEquals("P001", metadata.getColumnName(6));
				assertEquals("P002", metadata.getColumnName(7));
				assertEquals("P003", metadata.getColumnName(8));
			}

			try (ResultSet results = stmt.executeQuery("SELECT * FROM varlen2"))
			{
				ResultSetMetaData metadata = results.getMetaData();
				assertEquals("file_date", metadata.getColumnName(1));
				assertEquals("Datum", metadata.getColumnName(2));
				assertEquals("Tijd", metadata.getColumnName(3));
				assertEquals("Station", metadata.getColumnName(4));
				assertEquals("P000", metadata.getColumnName(5));
				assertEquals("P001", metadata.getColumnName(6));
			}
		}
	}

	@Test
	public void testNonExistingTable() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection(
				"jdbc:relique:csv:" + filePath);
			Statement stmt = conn.createStatement())
		{
			try
			{
				try (ResultSet results = stmt.executeQuery("SELECT * FROM not_there"))
				{
					fail("Should not find the table 'not_there'");
				}
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("fileNotFound") + ": "
					+ filePath + File.separator + "not_there.csv", "" + e);
			}
		}

		Properties props = new Properties();
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");

		try (Connection conn = DriverManager.getConnection(
				"jdbc:relique:csv:" + filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM not_there"))
		{
			assertFalse(results.next(),
				"non existing indexed tables are seen as empty");
		}
	}

	@Test
	public void testDuplicatedColumnNamesPlainFails() throws SQLException
	{
		// no bug report, check discussion thread
		// https://sourceforge.net/projects/csvjdbc/forums/forum/56965/topic/2608197
		Properties props = new Properties();
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement())
		{
			// load CSV driver
			try
			{
				try (ResultSet results = stmt.executeQuery("SELECT * FROM duplicate_headers"))
				{
					fail("expected exception java.sql.SQLException: " + CsvResources.getString("duplicateColumns"));
				}
			}
			catch (SQLException e)
			{
				assertTrue(e.toString().contains(CsvResources.getString("duplicateColumns")),
					"wrong exception and/or exception text!");
			}
		}
	}

	@Test
	public void testFixDuplicatedColumnNames() throws SQLException
	{
		Properties props = new Properties();
		props.put("defectiveHeaders", "True");
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM duplicate_headers"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("ID", metadata.getColumnName(1));
			assertEquals("COLUMN2", metadata.getColumnName(2));
			assertEquals("Name", metadata.getColumnName(3));
			assertEquals("COLUMN4", metadata.getColumnName(4));
			assertTrue(results.next());
			assertEquals("1", results.getString("ID"), "1:ID is wrong");
			assertEquals("2", results.getString("COLUMN2"), "2:COLUMN2 is wrong");
			assertEquals("george", results.getString("Name"), "3:Name is wrong");
			assertEquals("joe", results.getString("COLUMN4"), "4:COLUMN4 is wrong");
		}
	}

	@Test
	public void testDuplicatedColumnNamesSuppressHeader() throws SQLException
	{
		// no bug report, check discussion thread
		// https://sourceforge.net/projects/csvjdbc/forums/forum/56965/topic/2608197
		Properties props = new Properties();
		props.put("suppressHeaders", "true");
		props.put("skipLeadingLines", "1");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM duplicate_headers"))
		{
			assertTrue(results.next());
			assertEquals("1", results.getString(1), "1:ID is wrong");
			assertEquals("2", results.getString(2), "2:ID is wrong");
			assertEquals("george", results.getString(3), "3:ID is wrong");
			assertEquals("joe", results.getString(4), "4:ID is wrong");

			assertTrue(results.next());
			assertEquals("2", results.getString(1), "1:ID is wrong");
			assertEquals("2", results.getString(2), "2:ID is wrong");
			assertEquals("aworth", results.getString(3), "3:ID is wrong");
			assertEquals("smith", results.getString(4), "4:ID is wrong");

			assertFalse(results.next());
		}
	}

	/**
	 * This creates several sentences with where and tests they work
	 *
	 * @throws SQLException
	 */
	@Test
	public void testWithNonRepeatedQuotes() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM doublequoted"))
		{
			assertTrue(results.next());
			assertEquals(
				"\"Rechtsform unbekannt\" entsteht durch die Simulation zTELKUS. Es werden Simulationsregeln angewandt.",
				results.getString(10));
		}
	}

	@Test
	public void testWithNonRepeatedQuotesExplicitSQLStyle() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quotestyle", "SQL");
		props.put("commentChar", "C");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM doublequoted"))
		{
			assertTrue(results.next());
			assertEquals(
				"\"Rechtsform unbekannt\" entsteht durch die Simulation zTELKUS. Es werden Simulationsregeln angewandt.",
				results.getString(10));
		}
	}

	@Test
	public void testWithNonRepeatedQuotesExplicitCStyle() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quoteStyle", "C");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM doublequoted"))
		{
			// TODO: this should actually fail!  have a look at testEscapingQuotecharExplicitSQLStyle for the way to check an exception.
			assertTrue(results.next());
			assertEquals(
				"\"Rechtsform unbekannt\" entsteht durch die Simulation zTELKUS. Es werden Simulationsregeln angewandt.",
				results.getString(10));
		}
	}

	@Test
	public void testEscapingQuotecharExplicitCStyle() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quoteStyle", "C");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT F1 FROM doublequoted"))
		{
			assertTrue(results.next());
			assertTrue(results.next());
			assertEquals("doubling \"\"quotechar", results.getObject("F1"));
			assertTrue(results.next());
			assertEquals("escaping quotechar\"", results.getObject("F1"));
		}
	}

	@Test
	public void testEscapingQuotecharExplicitSQLStyle() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quoteStyle", "SQL");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT F1 FROM doublequoted"))
		{
			assertTrue(results.next());
			assertTrue(results.next());
			assertEquals("doubling \\\"\\\"quotechar", results.getObject("F1"));
			assertTrue(results.next());
			assertTrue(results.next());
			try
			{
				results.next();
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("eofInQuotes") + ": 6", "" + e);
			}
		}
	}

	@Test
	public void testBadQuotechar()
	{
		Properties props = new Properties();
		props.put("quotechar", "()");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"	+ filePath, props))
		{
			fail("expected exception java.sql.SQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("invalid") + " quotechar: ()", "" + e,
				"Wrong exception text");
		}
	}

	@Test
	public void testNoQuotechar() throws SQLException
	{
		Properties props = new Properties();
		props.put("quotechar", "");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM uses_quotes"))
		{
			assertTrue(results.next());
			assertEquals("1", results.getString(1), "COLUMN1 is wrong");
			assertEquals("uno", results.getString(2), "COLUMN2 is wrong");
			assertEquals("one", results.getString(3), "COLUMN3 is wrong");
			assertTrue(results.next());
			assertEquals("2", results.getString(1), "COLUMN1 is wrong");
			assertEquals("a 'quote' (source unknown)", results.getString(2), "COLUMN2 is wrong");
			assertEquals("two", results.getString(3), "COLUMN3 is wrong");
			assertTrue(results.next());
			assertEquals("3", results.getString(1), "COLUMN1 is wrong");
			assertEquals("another \"quote\" (also unkown)", results.getString(2), "COLUMN2 is wrong");
			assertEquals("three", results.getString(3), "COLUMN3 is wrong");
		}
	}

	@Test
	public void testLongSeparator() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", "#@");
		props.put("skipLeadingLines", "2");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM evonix"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("ID", metadata.getColumnName(1));
			assertEquals("Name", metadata.getColumnName(2));
			assertEquals("Birthday", metadata.getColumnName(3));
			assertTrue(results.next());
			assertEquals("0", results.getString(1), "1:ID is wrong");
			assertEquals("(Florian)", results.getString(2), "2:Name is wrong");
			assertEquals("01.01.1990", results.getString(3), "3:Birthday is wrong");
			assertTrue(results.next());
			assertEquals("1", results.getString(1), "1:ID is wrong");
			assertEquals("(Tobias)", results.getString(2), "2:Name is wrong");
			assertEquals("01.01.1990", results.getString(3), "3:Birthday is wrong");
			assertTrue(results.next());
			assertEquals("2", results.getString(1), "1:ID is wrong");
			assertEquals("(#Mark)", results.getString(2), "2:Name is wrong");
			assertEquals("01.01.1990", results.getString(3), "3:Birthday is wrong");
			assertTrue(results.next());
			assertEquals("3", results.getString(1), "1:ID is wrong");
			assertEquals("(@Jason)", results.getString(2), "2:Name is wrong");
			assertEquals("01.01.1990", results.getString(3), "3:Birthday is wrong");
			assertTrue(results.next());
			assertEquals("4", results.getString(1), "1:ID is wrong");
			assertEquals("Robert", results.getString(2), "2:Name is wrong");
			assertEquals("01.01.1990", results.getString(3), "3:Birthday is wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testLongCommentChar() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", "#@");
		props.put("commentChar", "rem");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM evonix"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("ID", metadata.getColumnName(1));
			assertEquals("Name", metadata.getColumnName(2));
			assertEquals("Birthday", metadata.getColumnName(3));
			assertTrue(results.next());
			assertEquals("0", results.getString(1), "1:ID is wrong");
			assertEquals("(Florian)", results.getString(2), "2:Name is wrong");
			assertEquals("01.01.1990", results.getString(3), "3:Birthday is wrong");
		}
	}

	@Test
	public void testWithNoData() throws SQLException
	{
		Properties props = new Properties();
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM nodata"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals("Aleph", metadata.getColumnName(1));
			assertEquals("Beth", metadata.getColumnName(2));
			assertEquals("Ghimel", metadata.getColumnName(3));
			assertEquals("Daleth", metadata.getColumnName(4));

			assertFalse(results.next());
		}
	}

	@Test
	public void testWithNoHeader() throws SQLException
	{
		Properties props = new Properties();
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM only_comments"))
		{
			assertFalse(results.next());
		}
	}

	@Test
	public void testConnectionName() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath))
		{
			String url = conn.getMetaData().getURL();
			assertTrue(url.startsWith("jdbc:relique:csv:"));
			assertTrue(url.endsWith(File.separator + "testdata" + File.separator) || url.endsWith(File.separator + "testdata"));
		}
	}

	@Test
	public void testConnectionClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath))
		{
			assertTrue(conn.isValid(0));
			assertFalse(conn.isClosed());
			conn.close();
			assertTrue(conn.isClosed());
			assertFalse(conn.isValid(0));

			/*
			 * Second close is ignored.
			 */
			conn.close();
			try
			{
				conn.createStatement();
				fail("expected exception java.sql.SQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("closedConnection"), "" + e,
					"wrong exception and/or exception text!");
			}
		}
	}

	@Test
	public void testStatementClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			Statement stmt = conn.createStatement())
		{
			assertFalse(stmt.isClosed());
			stmt.close();
			assertTrue(stmt.isClosed());

			/*
			 * Second close is ignored.
			 */
			stmt.close();

			try
			{
				try (ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
				{
					fail("expected exception java.sql.SQLException");
				}
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("statementClosed"), "" + e,
					"wrong exception and/or exception text!");
			}
		}
	}

	@Test
	public void testConnectionClosesStatements() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			Statement stmt1 = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			Statement stmt4 = conn.createStatement())
		{
			conn.close();

			assertTrue(stmt1.isClosed());
			assertTrue(stmt2.isClosed());
			assertTrue(stmt3.isClosed());
			assertTrue(stmt4.isClosed());
		}
	}

	@Test
	public void testStatementCancelled() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
			Statement stmt = conn.createStatement())
		{
			try
			{
				try (ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
				{
					stmt.cancel();
					results.next();
				}
				fail("expected exception java.sql.SQLException: Statement cancelled");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("statementCancelled"),
						"" + e, "wrong exception and/or exception text!");
			}
		}
	}

	@Test
	public void testTrimValues() throws SQLException
	{
		Properties props = new Properties();
		props.put("trimHeaders", "true");
		props.put("trimValues", "true");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM foodstuffs"))
		{
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals(2, metadata.getColumnCount(), "Column Count");
			assertEquals("key", metadata.getColumnName(1), "Column Name 1");
			assertEquals("value", metadata.getColumnName(2), "Column Name 2");

			assertTrue(results.next());
			assertEquals("orange", results.getString(1), "Row 1 key");
			assertEquals("fruit", results.getString(2), "Row 1 value");

			assertTrue(results.next());
			assertEquals("apple", results.getString(1), "Row 2 key");
			assertEquals("fruit", results.getString(2), "Row 2 value");

			assertTrue(results.next());
			assertEquals("corn", results.getString(1), "Row 3 key");
			assertEquals("vegetable", results.getString(2), "Row 3 value");

			assertTrue(results.next());
			assertEquals("lemon", results.getString(1), "Row 4 key");
			assertEquals("fruit", results.getString(2), "Row 4 value");

			assertTrue(results.next());
			assertEquals("tomato", results.getString(1), "Row 5 key");
			assertEquals("who knows?", results.getString(2), "Row 5 value");

			assertTrue(results.next());
			assertEquals(" - ", results.getString(1), "Row 6 key");
			assertEquals(" - ", results.getString(2), "Row 6 value");

			assertFalse(results.next());
		}
	}

	/**
	 * you can access columns that do not have a name by number
	 *
	 * @throws SQLException
	 * @throws ParseException
	 */
	@Test
	public void testNumericDefectiveHeaders() throws SQLException, ParseException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		//props.put("defectiveHeaders", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM twoheaders"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("", metadata.getColumnName(1), "Empty Column Name 1");

			assertTrue(results.next());
			assertEquals("", results.getString(1), "1 is wrong");
			try
			{
				results.getString("");
				fail("expected exception java.sql.SQLException: Can't access columns with empty name by name");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("invalidColumnName") + ": ",
						"" + e, "wrong exception and/or exception text!");
			}

			assertEquals("WNS925", results.getString(2), "2 is wrong");
			assertEquals("WNS925", results.getString("600-P1201"), "2 is wrong");

			assertTrue(results.next());
			assertEquals("2010-02-21 00:00:00", results.getObject(1), "1 is wrong");
			assertEquals("21", results.getString(2), "2 is wrong");
			assertEquals("20", results.getString(3), "3 is wrong");
		}
	}

	@Test
	public void testWithDefectiveHeaders() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("defectiveHeaders", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM twoheaders"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("COLUMN1", metadata.getColumnName(1), "Incorrect Column Name 1");
			assertEquals("600-P1201", metadata.getColumnName(2), "Incorrect Column Name 2");

			assertTrue(results.next());
			assertEquals("", results.getString(1), "1 is wrong");
			assertEquals("", results.getString("COLUMN1"), "1 is wrong");

			assertEquals("WNS925", results.getString(2), "2 is wrong");
			assertEquals("WNS925", results.getString("600-P1201"), "2 is wrong");

			assertTrue(results.next());
			assertEquals("2010-02-21 00:00:00", results.getObject(1), "1 is wrong");
			assertEquals("2010-02-21 00:00:00", results.getObject("COLUMN1"), "1 is wrong");
			assertEquals("21", results.getObject(2), "2 is wrong");
			assertEquals("20", results.getObject(3), "3 is wrong");
		}
	}

	@Test
	public void testWithHeaderMissingColumns() throws SQLException
	{
		Properties props = new Properties();
		props.put("defectiveHeaders", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM defectiveheader"))
		{
			ResultSetMetaData metadata = results.getMetaData();

			assertEquals("IDX", metadata.getColumnName(1), "Incorrect Column Name 1");
			assertEquals("COLUMN2", metadata.getColumnName(2), "Incorrect Column Name 2");
			assertEquals("COLUMN3", metadata.getColumnName(3), "Incorrect Column Name 3");
			assertEquals("COMMENT", metadata.getColumnName(4), "Incorrect Column Name 4");
			assertEquals("COLUMN5", metadata.getColumnName(5), "Incorrect Column Name 5");

			assertTrue(results.next());
			assertEquals("178", results.getString(1), "1 is wrong");
			assertEquals("AAX", results.getString("COLUMN2"), "2 is wrong");
			assertEquals("ED+", results.getString(3), "3 is wrong");
			assertEquals("NONE", results.getString(4), "4 is wrong");
			assertEquals("T", results.getString("COLUMN5"), "5 is wrong");
		}
	}

	@Test
	public void testSkipLeadingDataLines() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("columnTypes", "Timestamp,Double");
		props.put("defectiveHeaders", "True");
		props.put("skipLeadingDataLines", "1");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM twoheaders"))
		{
			assertTrue(results.next());
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2010-02-21 00:00:00", toUTC.format(results.getObject(1)),
				"1 is wrong");
			assertEquals("2010-02-21 00:00:00", toUTC.format(results.getObject("COLUMN1")),
				"1 is wrong");
			assertEquals(Double.valueOf(21), results.getObject(2), "2 is wrong");
			assertEquals(Double.valueOf(20), results.getObject(3), "3 is wrong");
			assertEquals(Double.valueOf(24), results.getObject(4), "4 is wrong");
		}
	}

	@Test
	public void testSkipLeadingDataFromIndexedFile() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		props.put("fileTailPrepend", "True");
		props.put("skipLeadingDataLines", "1");
		// Datum,Tijd,Station,P000,P001,P002,P003
		props.put("columnTypes", "String,Date,Time,String,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM varlen1"))
		{
			assertTrue(results.next());
			assertEquals("007", results.getObject("Station"));
			assertEquals(Double.valueOf("26.54"), results.getObject("P003"));
			assertTrue(results.next());
			assertEquals("001", results.getObject("Station"));
			assertEquals(Double.valueOf("26.55"), results.getObject("P003"));
			assertFalse(results.next());
		}
	}

	@Test
	public void testMaxDataLines() throws SQLException
	{
		Properties props = new Properties();
		props.put("skipLeadingDataLines", "2");
		props.put("maxDataLines", "3");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			assertTrue(results.next());
			assertEquals("B234", results.getString(1), "Incorrect ID Value");
			assertTrue(results.next());
			assertEquals("C456", results.getString(1), "Incorrect ID Value");
			assertTrue(results.next());
			assertEquals("D789", results.getString(1), "Incorrect ID Value");
			assertFalse(results.next());
		}
	}

	@Test
	public void testMaxDataLinesWithOrderBy() throws SQLException
	{
		Properties props = new Properties();
		props.put("maxDataLines", "3");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM sample ORDER BY ID"))
		{
			assertTrue(results.next());
			assertEquals("A123", results.getString(1), "Incorrect ID Value");
			assertTrue(results.next());
			assertEquals("B234", results.getString(1), "Incorrect ID Value");
			assertTrue(results.next());
			assertEquals("Q123", results.getString(1), "Incorrect ID Value");
			assertFalse(results.next());
		}
	}

	@Test
	public void testIgnoreUnparseableInIndexedFile() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "(.)-20081112");
		props.put("fileTailParts", "index");
		// Datum,Tijd,Station,P000,P001,P002,P003
		props.put("columnNames", "Datum,Tijd,Station,P000,P001,P002,P003,INDEX");
		props.put("ignoreNonParseableLines", "True");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM varlen"))
		{
			assertTrue(results.next());
			assertEquals("1", results.getObject("index"));
			assertEquals("007", results.getObject("Station"));
			assertEquals("26.54", results.getObject("P001"));
			assertTrue(results.next());
			assertEquals("007", results.getObject("Station"));
			assertEquals("22.99", results.getObject("P001"));
			assertFalse(results.next());
		}
	}

	@Test
	public void testWrongColumnCount() throws SQLException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM wrong_column_count"))
		{
			try
			{
				while(results.next())
				{
				}
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				// Should fail with the line number in message.
				assertTrue(e.getMessage().contains(CsvResources.getString("wrongColumnCount")));
				assertTrue(e.getMessage().contains("137"));
			}
		}
	}

	@Test
	public void testUnparseableInIndexedFileCausesSQLException() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "(.)-20081112");
		props.put("fileTailParts", "index");
		// Datum,Tijd,Station,P000,P001,P002,P003
		props.put("columnNames", "Datum,Tijd,Station,P000,P001,P002,P003,INDEX");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM varlen"))
		{
			assertTrue(results.next());
			assertEquals("1", results.getObject("index"));
			assertEquals("007", results.getObject("Station"));
			assertEquals("26.54", results.getObject("P001"));
			assertTrue(results.next());
			assertEquals("007", results.getObject("Station"));
			assertEquals("22.99", results.getObject("P001"));
			try
			{
				results.next();
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertTrue(("" + e).contains(CsvResources.getString("wrongColumnCount")));
			}
		}
	}

	@Test
	public void testTimestampFormat() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "UTC");
		props.put("timestampFormat", "dd-MMM-yy hh:mm:ss.SSS aa");
		props.put("columnTypes", "Int,Timestamp");
		if (!Locale.getDefault().equals(Locale.US))
		{
			/*
			 * Ensure that test passes when running on non-English language computers.
			 */
			props.put("locale", Locale.US.toString());
		}

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT T FROM yogesh"))
		{
			assertTrue(results.next());
			Timestamp got = results.getTimestamp(1);
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2013-11-25 13:29:07", toUTC.format(got));
			assertTrue(results.next());
			got = results.getTimestamp(1);
			assertEquals("2013-12-06 11:52:21", toUTC.format(got));
		}
	}

	@Test
	public void testTimestampFormatGermany() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "UTC");
		// A pattern in java.time.format.DateTimeFormatter format
		props.put("timestampFormat", "dd-LLL-yy HH:mm:ss.SSS");
		props.put("useDateTimeFormatter", "true");
		props.put("columnTypes", "Int,Timestamp");
		props.put("locale", Locale.GERMANY.toString());

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT T FROM yogesh_de"))
		{
			assertTrue(results.next());
			Timestamp got = results.getTimestamp(1);
			LocalDateTime gotLocalDateTimeUTC = LocalDateTime.ofInstant(got.toInstant(), ZoneId.of("UTC"));
			// Expect formatted UTC timestamps to be identical to UTC timestamps read from file
			assertEquals("2013-10-25 13:29:07", gotLocalDateTimeUTC.format(toUTCDateTimeFormatter));
			assertTrue(results.next());
			got = results.getTimestamp(1);
			gotLocalDateTimeUTC = LocalDateTime.ofInstant(got.toInstant(), ZoneId.of("UTC"));
			assertEquals("2013-12-06 11:52:21", gotLocalDateTimeUTC.format(toUTCDateTimeFormatter));
		}
	}

	@Test
	public void testTimestampInTimeZoneRome() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "Europe/Rome");
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1"))
		{
			assertTrue(results.next());
			// TODO: getString miserably fails!
			//assertEquals("2001-01-02 12:30:00.0", results.getString("start"));
			Timestamp got = (Timestamp) results.getObject("start");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2001-01-02 11:30:00", toUTC.format(got));
			got = results.getTimestamp("start");
			assertEquals("2001-01-02 11:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = results.getTimestamp("start");
			assertEquals("2004-04-02 10:30:00", toUTC.format(got));

			assertFalse(results.next());
		}
	}

	@Test
	public void testTimestampInTimeZoneSantiago() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "America/Santiago");
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1"))
		{
			assertTrue(results.next());
			// TODO: getString miserably fails!
			//assertEquals("2001-01-02 12:30:00", results.getString("start"));
			Timestamp got = (Timestamp) results.getObject("start");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2001-01-02 15:30:00", toUTC.format(got));
			got = results.getTimestamp("start");
			assertEquals("2001-01-02 15:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = results.getTimestamp("start");
			assertEquals("2004-04-02 16:30:00", toUTC.format(got));

			assertFalse(results.next());
		}
	}

	@Test
	public void testTimestampInTimeZoneGMTPlus0400() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "GMT+04:00");
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1"))
		{
			assertTrue(results.next());
			// TODO: getString miserably fails!
			// assertEquals("2001-01-02 12:30:00", results.getString("start"));
			Timestamp got = (Timestamp) results.getObject("start");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2001-01-02 08:30:00", toUTC.format(got));
			got = results.getTimestamp("start");
			assertEquals("2001-01-02 08:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = results.getTimestamp("start");
			assertEquals("2004-04-02 08:30:00", toUTC.format(got));

			assertFalse(results.next());
		}
	}

	@Test
	public void testTimestampInTimeZoneGMTMinus0400() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "GMT-04:00");
		props.put("columnTypes", "Int,String,String,Timestamp");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1"))
		{
			assertTrue(results.next());
			// TODO: getString miserably fails!
			// assertEquals("2001-01-02 12:30:00", results.getString("start"));
			Timestamp got = (Timestamp) results.getObject("start");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2001-01-02 16:30:00", toUTC.format(got));
			got = results.getTimestamp("start");
			assertEquals("2001-01-02 16:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = results.getTimestamp("start");
			assertEquals("2004-04-02 16:30:00", toUTC.format(got));

			assertFalse(results.next());
		}
	}

	@Test
	public void testAddingDateToTimeInTimeZoneAthens() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "Europe/Athens");
		props.put("columnTypes", "Int,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT ID, NAME, D + T as DT FROM sample8"))
		{
			assertTrue(results.next());
			// TODO: getString miserably fails!
			//assertEquals("2001-01-02 12:30:00", results.getString("start"));
			Timestamp got = (Timestamp) results.getObject("DT");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2010-01-01 23:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2010-02-01 23:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2010-03-27 23:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2010-03-28 02:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2009-10-24 22:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2009-10-25 03:30:00", toUTC.format(got));

			assertFalse(results.next());
		}
	}

	@Test
	public void testAddingDateToTimeInTimeZoneGMTMinus0500() throws SQLException
	{
		Properties props = new Properties();
		props.put("timeZoneName", "GMT-05:00");
		props.put("columnTypes", "Int,String,Date,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt
				.executeQuery("SELECT ID, NAME, D + T as DT FROM sample8"))
		{
			assertTrue(results.next());
			// TODO: getString miserably fails!
			//assertEquals("2001-01-02 12:30:00", results.getString("start"));
			Timestamp got = (Timestamp) results.getObject("DT");
			DateFormat toUTC = getUTCDateFormat();
			assertEquals("2010-01-02 06:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2010-02-02 06:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2010-03-28 06:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2010-03-28 10:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2009-10-25 06:30:00", toUTC.format(got));

			assertTrue(results.next());
			got = (Timestamp) results.getObject("DT");
			assertEquals("2009-10-25 10:30:00", toUTC.format(got));

			assertFalse(results.next());
		}
	}

	@Test
	public void testValuesContainQuotes() throws SQLException
	{
		Properties props = new Properties();
		props.put("separator", ",");
		props.put("quotechar", "'");
		props.put("fileExtension", ".txt");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM uses_quotes"))
		{
			assertTrue(results.next());
			assertEquals("uno", results.getObject("COLUMN2"));
			assertTrue(results.next());
			assertEquals("a 'quote' (source unknown)", results.getObject("COLUMN2"));
			assertTrue(results.next());
			assertEquals("another \"quote\" (also unkown)", results.getObject("COLUMN2"));
			assertTrue(results.next());
			assertEquals("a 'quote\" that gives error", results.getObject("COLUMN2"));
			assertTrue(results.next());
			assertEquals("another not parsable \"quote'", results.getObject("COLUMN2"));
			assertTrue(results.next());
			assertEquals("collecting quotes \"\"''", results.getObject("COLUMN2"));
			assertFalse(results.next());
		}
	}

	@Test
	public void testWithDifferentDecimalSeparator() throws SQLException
	{
		Properties props = new Properties();
		props.put("fileExtension", "");
		props.put("separator", ";");
		props.put("columnTypes", "String,String,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample"))
		{
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next());
			assertTrue(results.next()); // Mackie Messer
			assertEquals(Double.valueOf(34.1), results.getObject(3));
			assertTrue(results.next()); // Polly Peachum
			assertEquals(Double.valueOf(30.5), results.getObject(3));
		}
	}

	@Test
	public void testLiteral() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT name, id, 'Bananas' as t FROM sample"))
		{
			results.next();
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("Bananas", results.getString("T"), "Incorrect ID Value");
			assertEquals("Bananas", results.getString("t"), "Incorrect ID Value");
			results.next();
			assertEquals("Bananas", results.getString("T"), "Incorrect ID Value");
			results.next();
			assertEquals("Bananas", results.getString("T"), "Incorrect ID Value");
			results.next();
			assertEquals("Bananas", results.getString("T"), "Incorrect ID Value");
		}
	}

	@Test
	public void testTableNameAsAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT sample.id FROM sample"))
		{
			results.next();
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
		}
	}

	@Test
	public void testTableAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT S.id, s.Extra_field FROM sample S"))
		{
			results.next();
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("F", results.getString("EXTRA_FIELD"), "Incorrect ID Value");
			assertEquals("Q123", results.getString(1), "Incorrect ID Value");
			assertEquals("F", results.getString(2), "Incorrect ID Value");
		}
	}

	@Test
	public void testTableAliasWithWhere() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT * FROM sample AS S where S.ID='A123'"))
		{
			results.next();
			assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
			assertEquals("A", results.getString("EXTRA_FIELD"), "Incorrect ID Value");
			assertEquals("A123", results.getString(1), "Incorrect ID Value");
			assertEquals("A", results.getString(3), "Incorrect ID Value");
			assertFalse(results.next());
		}
	}

	@Test
	public void testMaxRows() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			stmt.setMaxRows(4);
			assertEquals(4, stmt.getMaxRows(), "getMaxRows() incorrect");

			ResultSet results = stmt.executeQuery("SELECT * FROM sample5");
			// The maxRows value at the time of the query should be used, not the value below.
			stmt.setMaxRows(7);

			assertTrue(results.next(), "Reading row 1 failed");
			assertTrue(results.next(), "Reading row 2 failed");
			assertTrue(results.next(), "Reading row 3 failed");
			assertTrue(results.next(), "Reading row 4 failed");
			assertFalse(results.next(), "Stopping after row 4 failed");
		}
	}

	@Test
	public void testFetchSize() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			stmt.setFetchSize(50);
			assertEquals(50, stmt.getFetchSize(), "getFetchSize() incorrect");

			ResultSet results = stmt.executeQuery("SELECT * FROM sample5");
			assertEquals(50, results.getFetchSize(), "getFetchSize() incorrect");
			results.setFetchSize(20);
			assertEquals(20, results.getFetchSize(), "getFetchSize() incorrect");
		}
	}

	@Test
	public void testFetchDirection() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			stmt.setFetchDirection(ResultSet.FETCH_UNKNOWN);
			assertEquals(ResultSet.FETCH_UNKNOWN, stmt.getFetchDirection(), "getFetchDirection() incorrect");

			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample5"))
			{
				assertEquals(ResultSet.FETCH_UNKNOWN, results.getFetchDirection(), "getFetchDirection() incorrect");
				results.setFetchDirection(ResultSet.FETCH_FORWARD);
				assertEquals(ResultSet.FETCH_FORWARD, results.getFetchDirection(), "getFetchDirection() incorrect");
			}
		}
	}

	@Test
	public void testResultSet() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			// No query executed yet.
			assertNull(stmt.getResultSet());
			assertFalse(stmt.getMoreResults());
			assertEquals(-1, stmt.getUpdateCount(), "Update count wrong");

			ResultSet results1 = stmt.executeQuery("SELECT * FROM sample5");
			ResultSet results2 = stmt.getResultSet();
			assertEquals(results1, results2, "Result sets not equal");
			assertEquals(-1, stmt.getUpdateCount(), "Update count wrong");
			assertFalse(stmt.getMoreResults());
			assertNull(stmt.getResultSet(), "Result set not null");
			assertTrue(results1.isClosed(), "Result set was not closed");
		}
	}

	@Test
	public void testResultSetClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();
			ResultSet results1 = stmt.executeQuery("SELECT * FROM sample5");
			ResultSet results2 = stmt.executeQuery("SELECT * FROM sample"))
		{
			assertTrue(results1.isClosed(), "First result set is not closed");
			assertFalse(results2.isClosed(), "Second result set is closed");
			try
			{
				results1.next();
				fail("Closed result set should throw SQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
			}
		}
	}

	@Test
	public void testResultSetGetFromClosed() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT ID FROM sample"))
		{
			assertTrue(results.next());
			assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
			results.close();
			try
			{
				results.getString("ID");
				fail("Closed result set should throw SQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("closedResultSet"), "" + e);
			}
		}
	}

	@Test
	public void testHeaderlineWithMultipleTables() throws SQLException
	{
		Properties props = new Properties();
		// Define different headerline values for table banks and table transactions.
		props.put("headerline.banks", "BLZ,BANK_NAME");
		props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT * FROM banks where BANK_NAME like 'Sparkasse%'"))
			{
				// Check that column names from headerline are used for table banks.
				assertEquals("BLZ", results.getMetaData().getColumnName(1), "BLZ wrong");
				assertEquals("BANK_NAME", results.getMetaData().getColumnName(2), "BANK_NAME wrong");
				assertFalse(results.next());
			}

			try (ResultSet results = stmt.executeQuery("SELECT * FROM transactions"))
			{
				assertTrue(results.next());

				// Check that column names for table transactions are correct too.
				assertEquals("19-10-2011", results.getString("TRANS_DATE"), "TRANS_DATE wrong");
				assertEquals("3670345", results.getString("FROM_ACCT"), "FROM_ACCT wrong");
				assertEquals("250.00", results.getString("AMOUNT"), "AMOUNT wrong");
			}
		}
	}

	@Test
	public void testEmptyHeaderline() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT COLUMN1, COLUMN2 FROM banks"))
		{
			assertTrue(results.next());
			// Check that default column names are used.
			assertEquals("10000000", results.getString("COLUMN1"), "COLUMN1 wrong");
			assertEquals("Bundesbank (Berlin)", results.getString("COLUMN2"), "COLUMN2 wrong");
		}
	}

	@Test
	public void testWarnings() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			assertTrue(results.next());
			assertNull(results.getWarnings(), "Warnings should be null");
			results.clearWarnings();
			assertNull(results.getWarnings(), "Warnings should still be null");
		}
	}

	@Test
	public void testStringWasNull() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT ID FROM sample"))
		{
			assertTrue(results.next());
			assertEquals("Q123", results.getString(1), "ID wrong");
			assertFalse(results.wasNull());
		}
	}

	@Test
	public void testObjectWasNull() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT ID FROM sample"))
		{
			assertTrue(results.next());
			assertEquals("Q123", results.getObject(1), "ID wrong");
			assertFalse(results.wasNull());
		}
	}

	@Test
	public void testTableReader() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM airline where code='BA'"))
		{
			assertTrue(results.next());
			assertEquals("British Airways", results.getString("NAME"), "NAME wrong");
		}
	}

	@Test
	public void testTableReaderWithBadTable() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
			Statement stmt = conn.createStatement())
		{
			try
			{
				try (ResultSet results = stmt.executeQuery("SELECT * FROM X"))
				{
					fail("Should raise a java.sqlSQLException");
				}
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: Table does not exist: X", "" + e);
			}
		}
	}

	@Test
	public void testTableReaderWithBadReader()
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" + TestCsvDriver.class.getName()))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertEquals("java.sql.SQLException: " + CsvResources.getString("interfaceNotImplemented") + ": org.relique.io.TableReader: " + TestCsvDriver.class.getName(), "" + e);
		}
	}

	@Test
	public void testTableReaderTables() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
			ResultSet results = conn.getMetaData().getTables(null, null, "%", null))
		{
			assertTrue(results.next());
			assertEquals("AIRLINE", results.getString("TABLE_NAME"), "TABLE_NAME wrong");
			assertTrue(results.next());
			assertEquals("AIRPORT", results.getString("TABLE_NAME"), "TABLE_NAME wrong");
			assertFalse(results.next());
		}
	}

	@Test
	public void testTableReaderMetadata() throws SQLException
	{
		String url = "jdbc:relique:csv:class:" + TableReaderTester.class.getName();
		try (Connection conn = DriverManager.getConnection(url))
		{
			assertEquals(url, conn.getMetaData().getURL(), "URL is wrong");
		}
	}

	@Test
	public void testPropertiesInURL() throws SQLException
	{
		Properties props = new Properties();

		/*
		 * Use same directory name logic as in CsvDriver.connect.
		 */
		String path = filePath;
		if (!path.endsWith(File.separator))
			path += File.separator;
		String url = "jdbc:relique:csv:" + path + "?suppressHeaders=true&headerline=BLZ,NAME&commentChar=%23&fileExtension=.txt";
		try (Connection conn = DriverManager.getConnection(url, props);
			Statement stmt = conn.createStatement())
		{
			assertEquals(url, conn.getMetaData().getURL(), "The URL is wrong");

			try (ResultSet results = stmt.executeQuery("SELECT * FROM banks"))
			{
				assertTrue(results.next());
				assertEquals("10000000", results.getString("BLZ"), "The BLZ is wrong");
				assertEquals("Bundesbank (Berlin)", results.getString("NAME"), "The NAME is wrong");
			}
		}
	}

	@Test
	public void testEmptyPropertyInURL() throws SQLException
	{
		Properties props = new Properties();

		/*
		 * Use same directory name logic as in CsvDriver.connect.
		 */
		String path = filePath;
		if (!path.endsWith(File.separator))
			path += File.separator;
		String url = "jdbc:relique:csv:" + path + "?separator=;&fileExtension=";
		try (Connection conn = DriverManager.getConnection(url, props);
			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			assertTrue(results.next());
			assertEquals("Q123", results.getString("ID"), "The ID is wrong");
		}
	}

	@Test
	public void testLiteralWithUnicode() throws SQLException
	{
		Properties props = new Properties();
		props.put("charset", "UTF-8");
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("select id from sample5 where name = '\u00C9rica Jeanine M\u00e9ndez M\u00e9ndez'"))
		{
			assertTrue(results.next());
			assertEquals("08", results.getString(1), "Incorrect ID Value");
			assertFalse(results.next());
		}
	}

	@Test
	public void testDistinct() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select distinct job from sample5"))
		{
			assertTrue(results.next());
			assertEquals("Piloto", results.getString(1), "Incorrect distinct value 1");
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString(1), "Incorrect distinct value 2");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString(1), "Incorrect distinct value 3");
			assertTrue(results.next());
			assertEquals("Office Manager", results.getString(1), "Incorrect distinct value 4");
			assertTrue(results.next());
			assertEquals("Office Employee", results.getString(1), "Incorrect distinct value 5");
			assertFalse(results.next());
		}
	}

	@Test
	public void testDistinctWithAlias() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select distinct S.job from sample5 S"))
		{
			assertTrue(results.next());
			assertEquals("Piloto", results.getString(1), "Incorrect distinct value 1");
			assertTrue(results.next());
			assertEquals("Project Manager", results.getString(1), "Incorrect distinct value 2");
			assertTrue(results.next());
			assertEquals("Finance Manager", results.getString(1), "Incorrect distinct value 3");
			assertTrue(results.next());
			assertEquals("Office Manager", results.getString(1), "Incorrect distinct value 4");
			assertTrue(results.next());
			assertEquals("Office Employee", results.getString(1), "Incorrect distinct value 5");
			assertFalse(results.next());
		}
	}

	@Test
	public void testDistinctWithWhere() throws SQLException
	{
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("select distinct TO_ACCT, TO_BLZ from transactions where AMOUNT<50"))
		{
			assertTrue(results.next());
			assertEquals(27234813, results.getInt(1), "Incorrect distinct TO_ACCT value 1");
			assertEquals(10020500, results.getInt(2), "Incorrect distinct TO_BLZ value 1");
			assertTrue(results.next());
			assertEquals(3670345, results.getInt(1), "Incorrect distinct TO_ACCT value 2");
			assertEquals(10010010, results.getInt(2), "Incorrect distinct TO_BLZ value 2");
			assertFalse(results.next());
		}
	}

	@Test
	public void testNoTable() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT 'Hello', 'World' as W, 5+7"))
		{
			assertTrue(results.next());
			ResultSetMetaData metadata = results.getMetaData();
			assertEquals(Types.VARCHAR, metadata.getColumnType(1), "column 1 type is incorrect");
			assertEquals(Types.VARCHAR, metadata.getColumnType(2), "column 2 type is incorrect");
			assertEquals(Types.INTEGER, metadata.getColumnType(3), "column 3 type is incorrect");

			assertEquals("W", metadata.getColumnName(2), "column 2 name is incorrect");

			assertEquals("Hello", results.getString(1), "column 1 value is incorrect");
			assertEquals("World", results.getString(2), "column 2 value is incorrect");
			assertEquals(12, results.getInt(3), "column 3 value is incorrect");
			assertFalse(results.next());
		}
	}

	@Test
	public void testExtraResultSetNext() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
				.executeQuery("SELECT ID FROM sample5 WHERE ID < 3"))
		{
			assertTrue(results.next());
			assertTrue(results.next());
			assertFalse(results.next());
			assertFalse(results.next());
			assertFalse(results.next());
		}
	}

	@Test
	public void testGetRow() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement())
		{
			// Select the ID and NAME columns from sample.csv
			try (ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample"))
			{
				assertEquals(0, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertTrue(results.next());
				assertEquals("Q123", results.getString("ID"), "Incorrect ID Value");
				assertEquals(1, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertTrue(results.next());
				assertEquals(2, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(3, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(4, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(5, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(6, results.getRow(), "incorrect row #");
				assertFalse(results.next());
				assertEquals(0, results.getRow(), "incorrect row #");
				assertTrue(results.isAfterLast());
			}

			try (ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample ORDER BY ID"))
			{
				assertEquals(0, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertTrue(results.next());
				assertEquals("A123", results.getString("ID"), "Incorrect ID Value");
				assertEquals(1, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertTrue(results.next());
				assertEquals(2, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(3, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(4, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(5, results.getRow(), "incorrect row #");
				assertTrue(results.next());
				assertEquals(6, results.getRow(), "incorrect row #");
				assertFalse(results.next());
				assertEquals(0, results.getRow(), "incorrect row #");
				assertTrue(results.isAfterLast());
			}

			try (ResultSet results = stmt.executeQuery("SELECT COUNT(*) FROM sample"))
			{
				assertEquals(0, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertTrue(results.next());
				assertEquals(6, results.getInt(1), "Incorrect COUNT Value");
				assertEquals(1, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertFalse(results.next());
				assertEquals(0, results.getRow(), "incorrect row #");
				assertTrue(results.isAfterLast());
			}

			// Test result set returning no rows.
			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample WHERE ID = 'unknown'"))
			{
				assertEquals(0, results.getRow(), "incorrect row #");
				assertFalse(results.isAfterLast());
				assertFalse(results.next());
				assertFalse(results.isAfterLast());
			}
		}
	}

	@Test
	public void testNoCurrentRow() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

			Statement stmt = conn.createStatement();

			// Select the ID and NAME columns from sample.csv
			ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample"))
		{
			try
			{
				results.getString(1);
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertEquals("java.sql.SQLException: " + CsvResources.getString("noCurrentRow"), "" + e);
			}
		}
	}

	@Test
	public void testWriteToCsv() throws SQLException, UnsupportedEncodingException, IOException
	{
		Properties props = new Properties();

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT * FROM sample"))
		{
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			PrintStream printStream = new PrintStream(byteStream);

			/*
			 * Check that writing ResultSet to CSV file generates exactly the same CSV file
			 * that the query was originally read from.
			 */
			CsvDriver.writeToCsv(results, printStream, true);

			try (BufferedReader reader1 = new BufferedReader(new FileReader(filePath + File.separator + "sample.csv"));
				BufferedReader reader2 = new BufferedReader(new StringReader(byteStream.toString("US-ASCII"))))
			{
				String line1 = reader1.readLine();
				String line2 = reader2.readLine();

				while (line1 != null || line2 != null)
				{
					assertTrue(line1 != null, "line1 is null");
					assertTrue(line2 != null, "line2 is null");
					assertEquals(line1, line2, "lines do not match");
					line1 = reader1.readLine();
					line2 = reader2.readLine();
				}
			}
		}
	}

	@Test
	public void testWriteToCsvWithDates() throws SQLException, UnsupportedEncodingException, IOException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Int,Date,Time,Timestamp");
		props.put("dateFormat", "dd-MMM-yy");
		props.put("timeFormat", "hh:mm:ss.SSS aa");
		props.put("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS");
		if (!Locale.getDefault().equals(Locale.US))
		{
			/*
			 * Ensure that test passes when running on non-English language computers.
			 */
			props.put("locale", Locale.US.toString());
		}

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM sunil_date_time"))
		{
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			PrintStream printStream = new PrintStream(byteStream);

			/*
			 * Check that writing ResultSet to CSV file generates exactly the same CSV file
			 * that the query was originally read from.
			 */
			boolean writeHeaderLine = true;
			CsvDriver.writeToCsv(results, printStream, writeHeaderLine);

			try (BufferedReader reader1 = new BufferedReader(new FileReader(filePath + File.separator + "sunil_date_time.csv"));
				BufferedReader reader2 = new BufferedReader(new StringReader(byteStream.toString("US-ASCII"))))
			{
				String line1 = reader1.readLine();
				String line2 = reader2.readLine();

				while (line1 != null || line2 != null)
				{
					assertTrue(line1 != null, "line1 is null");
					assertTrue(line2 != null, "line2 is null");
					assertTrue(line1.equalsIgnoreCase(line2), "lines do not match");
					line1 = reader1.readLine();
					line2 = reader2.readLine();
				}
				assertNull(line1, "line1 not empty");
				assertNull(line2, "line2 not empty");
			}
		}
	}

	@Test
	public void testBooleanConversion() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,Boolean");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM bool"))
		{
			assertTrue(results.next());
			assertEquals(Boolean.FALSE, Boolean.valueOf(results.getBoolean(1)), "incorrect I");
			assertEquals(Boolean.TRUE, Boolean.valueOf(results.getBoolean(2)), "incorrect J");
			assertEquals(Boolean.FALSE, Boolean.valueOf(results.getBoolean(3)), "incorrect K");
			assertEquals(Boolean.TRUE, Boolean.valueOf(results.getBoolean(4)), "incorrect L");
			assertTrue(results.next());
			assertEquals(Boolean.TRUE, Boolean.valueOf(results.getBoolean("I")), "incorrect I");
			assertEquals(Boolean.FALSE, Boolean.valueOf(results.getBoolean("J")), "incorrect J");
			assertEquals(Boolean.FALSE, Boolean.valueOf(results.getBoolean("K")), "incorrect K");
			assertEquals(Boolean.FALSE, Boolean.valueOf(results.getBoolean("L")), "incorrect L");
		}
	}

	@Test
	public void testExecuteSingleStatement() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			boolean hasResults = stmt.execute("SELECT * FROM sample");
			assertTrue(hasResults, "execute hasResults");
			ResultSet results = stmt.getResultSet();
			assertNotNull(results);
			assertTrue(results.next());
			assertEquals("Q123", results.getString(1), "The ID is wrong");
			assertFalse(stmt.getMoreResults(), "getMoreResults");
		}
	}

	@Test
	public void testExecuteMultipleStatements() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

			Statement stmt = conn.createStatement())
		{
			boolean hasResults = stmt.execute("SELECT ID FROM sample;\nSELECT Name FROM sample2;");
			assertTrue(hasResults, "execute hasResults");
			try (ResultSet results1 = stmt.getResultSet())
			{
				assertNotNull(results1);
				assertTrue(results1.next());
				assertEquals("Q123", results1.getString(1), "The ID is wrong");

				assertTrue(stmt.getMoreResults(), "getMoreResults");
				try (ResultSet results2 = stmt.getResultSet())
				{
					assertNotNull(results2);
					assertTrue(results1.isClosed());
					assertTrue(results2.next());
					assertEquals("Aman", results2.getString(1), "The Name is wrong");
					assertFalse(stmt.getMoreResults(), "getMoreResults");
					assertTrue(results2.isClosed());
				}
			}
		}
	}

	@Test
	public void testUserSqlFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("function.POW", "java.lang.Math.pow(double, double)");
		props.put("function.BITCOUNT", "java.lang.Integer.bitCount(int i)");
		props.put("function.PROPERTY", "java.lang.System.getProperty(String)");
		props.put("function.RLIKE", "java.util.regex.Pattern.matches(String regex,CharSequence input)");
		props.put("function.CURRENTTIMEMILLIS", "java.lang.System.currentTimeMillis()");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement())
		{
			try (ResultSet results = stmt.executeQuery("SELECT POW(C1, 2) FROM numeric"))
			{
				assertTrue(results.next());
				assertEquals(99 * 99, Math.round(results.getDouble(1)), "pow is wrong");
				assertTrue(results.next());
				assertEquals(-22 * -22, Math.round(results.getDouble(1)), "pow is wrong");
			}

			try (ResultSet results = stmt.executeQuery("SELECT BITCOUNT(C1) FROM numeric"))
			{
				assertTrue(results.next());
				assertEquals(Integer.bitCount(99), results.getInt(1), "bitcount is wrong");
				assertTrue(results.next());
				assertEquals(Integer.bitCount(-22), results.getInt(1), "bitcount is wrong");
			}

			String separator = System.getProperty("file.separator");
			try (ResultSet results = stmt.executeQuery("SELECT PROPERTY('file.separator') || ID FROM sample"))
			{
				assertTrue(results.next());
				assertEquals(separator + "Q123", results.getString(1), "property is wrong");
				assertTrue(results.next());
				assertEquals(separator + "A123", results.getString(1), "property is wrong");
			}

			try (ResultSet results = stmt.executeQuery("SELECT * FROM sample WHERE RLIKE('.*234', ID) = 'true'"))
			{
				assertTrue(results.next());
				assertEquals("B234", results.getString(1), "ID is wrong");
				assertTrue(results.next());
				assertEquals("X234", results.getString(1), "ID is wrong");
				assertFalse(results.next());
			}

			long t1 = System.currentTimeMillis();
			try (ResultSet results = stmt.executeQuery("SELECT CurrentTimeMillis()"))
			{
				assertTrue(results.next());
				long t = results.getLong(1);
				long t2 = System.currentTimeMillis();
				assertTrue(t >= t1 && t <= t2, "CurrentTimeMillis is wrong");
			}
		}
	}

	@Test
	public void testVarargsUserSqlFunction() throws SQLException
	{
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		props.put("function.FORMAT", "java.lang.String.format(String, Object...)");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt.executeQuery("SELECT FORMAT('%04d%06d %x', C1, C2, 255) FROM numeric"))
		{
			assertTrue(results.next());
			assertEquals("0099-01010 ff", results.getString(1), "format is wrong");
			assertTrue(results.next());
			assertEquals("-022000015 ff", results.getString(1), "format is wrong");
		}
	}

	@Test
	public void testBadUserSqlFunction()
	{
		Properties props = new Properties();
		props.put("function.BAD", "java.lang.Math.bad(double)");

		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props))
		{
			fail("Should raise a java.sqlSQLException");
		}
		catch (SQLException e)
		{
			assertTrue(e.getMessage().contains(CsvResources.getString("noFunctionMethod")));
		}
	}

	@Test
	public void testSavepoints() throws SQLException
	{
		try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath))
		{
			Savepoint savepoint1 = conn.setSavepoint();
			savepoint1.getSavepointId();
			conn.rollback(savepoint1);
			Savepoint savepoint2 = conn.setSavepoint("name1");
			String name = savepoint2.getSavepointName();
			assertEquals("name1", name, "Incorrect Savepoint name");
			conn.rollback(savepoint2);
			conn.close();

			try
			{
				conn.setSavepoint();
				fail("Should raise a java.sqlSQLException");
			}
			catch (SQLException e)
			{
				assertTrue(e.getMessage().contains(CsvResources.getString("closedConnection")));
			}
		}
	}
}
