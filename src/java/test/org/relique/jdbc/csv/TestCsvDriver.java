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
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package test.org.relique.jdbc.csv;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;
import java.sql.Types;

import org.relique.jdbc.csv.CsvResultSet;

import junit.framework.TestCase;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Chetan Gupta
 * @author Mario Frasca
 * @version $Id: TestCsvDriver.java,v 1.72 2011/11/01 10:53:42 simoc Exp $
 */
public class TestCsvDriver extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;
	private DateFormat toUTC;

	public TestCsvDriver(String name) {
		super(name);
	}

	protected void setUp() {
		filePath = System.getProperty(SAMPLE_FILES_LOCATION_PROPERTY);
		if (filePath == null)
			filePath = RunTests.DEFAULT_FILEPATH;
		assertNotNull("Sample files location property not set !", filePath);

		// load CSV driver
		try {
			Class.forName("org.relique.jdbc.csv.CsvDriver");
		} catch (ClassNotFoundException e) {
			fail("Driver is not in the CLASSPATH -> " + e);
		}
		toUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		toUTC.setTimeZone(TimeZone.getTimeZone("UTC"));  

	}

	public void testWithDefaultValues() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");

		results.next();
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "\"S,\"", results
				.getString("NAME"));
		assertEquals("Incorrect EXTRA_FIELD Value", "F", results
				.getString("EXTRA_FIELD"));

		assertEquals("Incorrect Column 1 Value", "\"S,\"", results.getString(1));
		assertEquals("Incorrect Column 2 Value", "Q123", results.getString(2));
		assertEquals("Incorrect Column 3 Value", "F", results.getString(3));

		results.next();
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Jonathan Ackerman", results
				.getString("NAME"));
		assertEquals("Incorrect EXTRA_FIELD Value", "A", results
				.getString("EXTRA_FIELD"));

		results.next();
		assertEquals("Incorrect ID Value", "B234", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Grady O'Neil", results
				.getString("NAME"));
		assertEquals("Incorrect EXTRA_FIELD Value", "B", results
				.getString("EXTRA_FIELD"));

		results.next();
		assertEquals("Incorrect ID Value", "C456", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Susan, Peter and Dave", results
				.getString("NAME"));
		assertEquals("Incorrect EXTRA_FIELD Value", "C", results
				.getString("EXTRA_FIELD"));

		results.next();
		assertEquals("Incorrect ID Value", "D789", results.getString("ID"));
		assertEquals("Incorrect NAME Value", "Amelia \"meals\" Maurice",
				results.getString("NAME"));
		assertEquals("Incorrect EXTRA_FIELD Value", "E", results
				.getString("EXTRA_FIELD"));

		results.next();
		assertEquals("Incorrect ID Value", "X234", results.getString("ID"));
		assertEquals("Incorrect NAME Value",
				"Peter \"peg leg\", Jimmy & Samantha \"Sam\"", results
						.getString("NAME"));
		assertEquals("Incorrect EXTRA_FIELD Value", "G", results
				.getString("EXTRA_FIELD"));

		results.close();
		stmt.close();
		conn.close();
	}

	/**
	 * This creates several sentences with where and tests they work
	 * 
	 * @throws SQLException
	 */
	public void testWhereSimple() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM sample4 WHERE ID='03'");
		assertTrue(results.next());
		assertEquals("The name is wrong", "Maria Cristina Lucero", results
				.getString("Name"));
		try {
			results.getString("Job");
			fail("Should not find the column 'Job'");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Column not found: invalid index: 0", "" + e);
		}
		assertTrue(!results.next());
	}

	/**
	 * fields in different order than in source file.
	 * 
	 * @throws SQLException
	 */
	public void testWhereShuffled() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT Job,ID,Name FROM sample4 WHERE ID='02'");
		assertTrue("no results found - should be one", results.next());
		assertEquals("The name is wrong", "Mauricio Hernandez", results
				.getString("Name"));
		assertEquals("The job is wrong", "Project Manager", results
				.getString("Job"));
		assertTrue("more than one matching records", !results.next());
	}

	public void testWithProperties() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", "");
		props.put("separator", ";");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("Q123"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"\"S;\""));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("F"));

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("A123"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"Jonathan Ackerman"));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("A"));

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("B234"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"Grady O'Neil"));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("B"));

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("C456"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"Susan; Peter and Dave"));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("C"));

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("D789"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"Amelia \"meals\" Maurice"));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("E"));

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("X234"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"Peter \"peg leg\"; Jimmy & Samantha \"Sam\""));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("G"));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMetadata() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM sample3");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("Incorrect Table Name", "sample3", metadata
				.getTableName(0));

		assertEquals("Incorrect Column Name 1", "column 1", metadata
				.getColumnName(1));
		assertEquals("Incorrect Column Name 2", "column \"2\" two", metadata
				.getColumnName(2));
		assertEquals("Incorrect Column Name 3", "Column 3", metadata
				.getColumnName(3));
		assertEquals("Incorrect Column Name 4", "CoLuMn4", metadata
				.getColumnName(4));
		assertEquals("Incorrect Column Name 5", "COLumn5", metadata
				.getColumnName(5));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMetadataWithSupressedHeaders() throws SQLException {
		Properties props = new Properties();
		props.put("suppressHeaders", "true");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM sample");

		ResultSetMetaData metadata = results.getMetaData();

		assertTrue("Incorrect Table Name", metadata.getTableName(0).equals(
				"sample"));

		assertTrue("Incorrect Column Name 1", metadata.getColumnName(1).equals(
				"COLUMN1"));
		assertTrue("Incorrect Column Name 2", metadata.getColumnName(2).equals(
				"COLUMN2"));
		assertTrue("Incorrect Column Name 3", metadata.getColumnName(3).equals(
				"COLUMN3"));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMetadataWithColumnType() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id, name, job, start FROM sample5");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("type of column 1 is incorrect", Types.INTEGER, metadata
				.getColumnType(1));
		assertEquals("type of column 2 is incorrect", Types.VARCHAR, metadata
				.getColumnType(2));
		assertEquals("type of column 3 is incorrect", Types.VARCHAR, metadata
				.getColumnType(3));
		assertEquals("type of column 4 is incorrect", Types.TIMESTAMP, metadata
				.getColumnType(4));

		assertEquals("type of column 1 is incorrect", "Int", metadata
				.getColumnTypeName(1));
		assertEquals("type of column 2 is incorrect", "String", metadata
				.getColumnTypeName(2));
		assertEquals("type of column 3 is incorrect", "String", metadata
				.getColumnTypeName(3));
		assertEquals("type of column 4 is incorrect", "Timestamp", metadata
				.getColumnTypeName(4));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMetadataWithColumnTypeShuffled() throws SQLException {
		// TODO: this test fails!
		Properties props = new Properties();
		// header in file: ID,Name,Job,Start,timeoffset
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT start, id, name, job FROM sample5");

		ResultSetMetaData metadata = results.getMetaData();

		// TODO - this fails
		assertEquals("type of column 1 is incorrect", Types.TIMESTAMP, metadata
				.getColumnType(1));
		assertEquals("type of column 2 is incorrect", Types.INTEGER, metadata
				.getColumnType(2));
		assertEquals("type of column 3 is incorrect", Types.VARCHAR, metadata
				.getColumnType(3));
		assertEquals("type of column 4 is incorrect", Types.VARCHAR, metadata
				.getColumnType(4));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testMetadataWithOperations() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id, start+timeoffset AS ts, 999 as C3, id - 4 as C4, " +
						"ID * 1.1 as C5, Name+JOB AS c6, '.com' as C7, 'Mr '+Name as C8 FROM sample5");
		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("type of column 1 is incorrect", Types.INTEGER, metadata
				.getColumnType(1));
		assertEquals("type of column 2 is incorrect", Types.TIMESTAMP, metadata
				.getColumnType(2));
		assertEquals("type of column 3 is incorrect", Types.INTEGER, metadata
				.getColumnType(3));
		assertEquals("type of column 4 is incorrect", Types.INTEGER, metadata
				.getColumnType(4));
		assertEquals("type of column 5 is incorrect", Types.DOUBLE, metadata
				.getColumnType(5));
		assertEquals("type of column 6 is incorrect", Types.VARCHAR, metadata
				.getColumnType(6));
		assertEquals("type of column 7 is incorrect", Types.VARCHAR, metadata
				.getColumnType(7));
		assertEquals("type of column 8 is incorrect", Types.VARCHAR, metadata
				.getColumnType(8));
	}

	public void testMetadataWithTableAlias() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select sx.id, sx.name as name from sample as sx");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("Incorrect Table Name", "sample", metadata.getTableName(0));

		assertEquals("Incorrect Column Name 1", "ID", metadata.getColumnName(1));
		assertEquals("Incorrect Column Name 2", "NAME", metadata.getColumnName(2));
	}

	public void testColumnTypesUserSpecified() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");

		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results
				.getObject("id"));
		assertEquals("Integer column 1 is wrong", new Integer(1), results
				.getObject(1));
		java.sql.Date shouldBe = java.sql.Date.valueOf("2001-01-02");
		assertEquals("Date column Start is wrong", shouldBe, results
				.getObject("start"));
		assertEquals("Date column 4 is wrong", shouldBe, results.getObject(4));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results
				.getObject("name"));
	}

	public void testColumnTypesUserSpecifiedShuffled() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Start, Name, Job "
				+ "FROM sample5 WHERE Job = 'Project Manager'");

		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results
				.getObject("id"));
		assertEquals("Integer column 1 is wrong", new Integer(1), results
				.getObject(1));
		java.sql.Date shouldBe = java.sql.Date.valueOf("2001-01-02");
		assertEquals("Date column Start is wrong", shouldBe, results
				.getObject("start"));
		assertEquals("Date column 4 is wrong", shouldBe, results.getObject(2));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results
				.getObject("name"));
	}

	public void testColumnTypesUserSpecifiedTS() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");

		assertTrue(results.next());
		DateFormat dfp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		assertEquals("Integer column ID is wrong", new Integer(1), results
				.getObject("id"));
		assertEquals("Integer column 1 is wrong", new Integer(1), results
				.getObject(1));
		assertEquals("Date column Start is wrong", dfp.parse(results
				.getString("start")), results.getObject("start"));
		assertEquals("Date column 4 is wrong", dfp.parse(results
				.getString("start")), results.getObject(4));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results
				.getObject("name"));
	}

	/**
	 * @throws SQLException
	 * @throws ParseException
	 */
	public void testColumnTypesInferFromData() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5");

		assertTrue(results.next());
		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("type of column 1 is incorrect", Types.INTEGER, metadata
				.getColumnType(1));
		assertEquals("type of column 2 is incorrect", Types.VARCHAR, metadata
				.getColumnType(2));
		assertEquals("type of column 3 is incorrect", Types.VARCHAR, metadata
				.getColumnType(3));
		assertEquals("type of column 4 is incorrect", Types.TIMESTAMP, metadata
				.getColumnType(4));
	}

	/**
	 * @throws SQLException
	 * @throws ParseException
	 */
	public void testColumnTypesNumeric() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT C1, C2, C3, C4, C5, C6, C7 "
				+ "FROM numeric");

		assertTrue(results.next());
		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("type of column 1 is incorrect", Types.TINYINT, metadata
				.getColumnType(1));
		assertEquals("type of column 2 is incorrect", Types.SMALLINT, metadata
				.getColumnType(2));
		assertEquals("type of column 3 is incorrect", Types.INTEGER, metadata
				.getColumnType(3));
		assertEquals("type of column 4 is incorrect", Types.BIGINT, metadata
				.getColumnType(4));
		assertEquals("type of column 5 is incorrect", Types.FLOAT, metadata
				.getColumnType(5));
		assertEquals("type of column 6 is incorrect", Types.DOUBLE, metadata
				.getColumnType(6));
		assertEquals("type of column 7 is incorrect", Types.DECIMAL, metadata
				.getColumnType(7));
	}

	public void testColumnTypesDefaultBehaviour() throws SQLException,
			ParseException {
		Properties props = new Properties();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");

		assertTrue(results.next());
		assertEquals("the start time is wrong", "2001-01-02 12:30:00", results
				.getObject("start"));
		assertEquals("The ID is wrong", "01", results.getObject("id"));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results
				.getObject("name"));
	}

	/**
	 * @throws SQLException
	 */
	public void testBadColumnTypesFails() throws SQLException {		
		try {
			Properties props = new Properties();
			props.put("columnTypes", "Varchar,Varchar");
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
					.executeQuery("SELECT Id, Name FROM sample");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column type: Varchar", "" + e);
		}
	}

	/**
	 * @throws SQLException
	 */
	public void testBadColumnNameFails() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

			Statement stmt = conn.createStatement();

			ResultSet results = stmt
					.executeQuery("SELECT Id, XXXX FROM sample");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column name: XXXX", "" + e);
		}
	}

	public void testColumnTypesWithSelectStar() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * "
				+ "FROM sample5 WHERE Job = 'Project Manager'");

		assertTrue(results.next());
		String target = "2001-01-02 12:30:00";
		assertEquals("the start time is wrong", target, toUTC.format(results
				.getObject("start")));
		assertEquals("The ID is wrong", new Integer(1), results.getObject("id"));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results
				.getObject("name"));
	}

	public void testWithSuppressedHeaders() throws SQLException {
		Properties props = new Properties();
		props.put("suppressHeaders", "true");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM sample");

		// header is now treated as normal data line
		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("ID"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("NAME"));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("EXTRA_FIELD"));

		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("Q123"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("\"S,\""));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("F"));

		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("A123"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("Jonathan Ackerman"));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("A"));

		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("B234"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("Grady O'Neil"));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("B"));

		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("C456"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("Susan, Peter and Dave"));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("C"));

		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("D789"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("Amelia \"meals\" Maurice"));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("E"));

		results.next();
		assertTrue("Incorrect COLUMN1 Value", results.getString("COLUMN1")
				.equals("X234"));
		assertTrue("Incorrect COLUMN2 Value", results.getString("COLUMN2")
				.equals("Peter \"peg leg\", Jimmy & Samantha \"Sam\""));
		assertTrue("Incorrect COLUMN3 Value", results.getString("COLUMN3")
				.equals("G"));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testRelativePath() throws SQLException {
		// break up file path to test relative paths
		String parentPath = new File(filePath).getParent();
		String subPath = new File(filePath).getName();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ parentPath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM ."
						+ File.separator + subPath + File.separator + "sample");

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("Q123"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"\"S,\""));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("F"));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testWhereMultipleResult() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT ID, Name, Job FROM sample4 WHERE Job = 'Project Manager'");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertTrue(!results.next());
	}

	public void testFieldAsAlias() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		//TODO using alias j in the WHERE clause is not valid SQL.  Should we really test this?
		ResultSet results = stmt
				.executeQuery("SELECT ID as i, Name as n, Job as j FROM sample4 WHERE j='Project Manager'");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("i"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("i"));
		assertEquals("The name is wrong", "Mauricio Hernandez", results
				.getString("N"));
		assertEquals("The name is wrong", "Mauricio Hernandez", results
				.getString(2));
		assertEquals("The job is wrong", "Project Manager", results
				.getString("J"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("i"));
		assertTrue(!results.next());
	}

	public void testSelectStar() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		CsvResultSet results = (CsvResultSet) stmt
				.executeQuery("SELECT * FROM sample4");
		assertEquals("ID", results.getMetaData().getColumnName(1).toString());
		assertEquals("[ID]", results.getMetaData().getColumnLabel(1).toString());

		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("id"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("id"));
		assertEquals("The name is wrong", "Mauricio Hernandez", results
				.getString("Name"));
		assertEquals("The name is wrong", "Mauricio Hernandez", results
				.getString(2));
		assertEquals("The job is wrong", "Project Manager", results
				.getString("Job"));
		assertTrue(results.next());
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("id"));
		assertTrue(!results.next());
	}

	public void testSelectNull() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT ID, null as ID2 FROM sample4");
		assertEquals("ID2", results.getMetaData().getColumnName(2));
		assertTrue(results.next());
		assertEquals("The ID2 is wrong", null, results.getString("id2"));
		assertTrue(results.next());
		assertEquals("The ID2 is wrong", null, results.getObject("id2"));
	}

	public void testLiteralAsAlias() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT Job j,ID i,Name n, 0 c FROM sample4");
		assertTrue("no results found - should be all", results.next());
		assertTrue("no results found - should be all", results.next());
		assertEquals("The literal c is wrong", "0", results.getString("c"));
		assertEquals("The literal c is wrong", "0", results.getString(4));
		assertEquals("The name is wrong", "Mauricio Hernandez", results
				.getString("N"));
		assertEquals("The job is wrong", "Project Manager", results
				.getString("J"));
	}

	/**
	 * This returns no results with where and tests if this still works
	 * 
	 * @throws SQLException
	 */
	public void testWhereNoResults() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT ID,Name FROM sample4 WHERE ID='05'");
		assertFalse(results.next());
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void testSelectStarWhereMultipleResult() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE Job = 'Project Manager'");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertTrue(!results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithAndOperator() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE Job = 'Project Manager' AND Name = 'Mauricio Hernandez'");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(!results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithBetweenOperator() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE id BETWEEN '02' AND '03'");

		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertTrue(!results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithLikeOperatorPercent() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample4 WHERE Name LIKE 'Ma%'");

		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertFalse(results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithLikeOperatorUnderscore() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample where id like '_234'");

		assertTrue(results.next());
		assertEquals("The ID is wrong", "B234", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "X234", results.getString("ID"));
		assertFalse(results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithUnselectedColumn() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT Name, Job FROM sample4 WHERE id = '04'");
		assertTrue(results.next());
		try {
			results.getString("id");
			fail("Should not find the column 'id'");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Column not found: invalid index: 0", "" + e);
		}
		assertEquals("The name is wrong", "Felipe Grajales", results
				.getString("name"));
		assertTrue(!results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithBadColumnName() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample where XXXX='123'");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column name: XXXX", "" + e);
		}
	}

	/**
	 * @throws SQLException
	 */
	public void testWhereWithExponentialNumbers() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT c4 FROM numeric WHERE c4 >= 2.00e+6 and c4 <= 9e15 and c1 < 1.e-1");
		assertTrue(results.next());

		double d = results.getDouble("c4");
		long l = Math.round(d);
		assertEquals("The c4 is wrong", l, 990000000000l);
		assertFalse(results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void test1073375() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("separator", "\t");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM jo");
		assertTrue(results.next());
		try {
			results.getString("id");
			fail("Should not find the column 'id'");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Column not found: invalid index: 0", "" + e);
		}
		assertEquals("The name is wrong", "3", results.getString("rc"));
		// would like to test the full_name_nd, but can't insert the Arabic
		// string in the code
		assertTrue(results.next());
		assertEquals("The name is wrong", "3", results.getString("rc"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "3", results.getString("rc"));
		assertEquals("The name is wrong", "Tall Dhayl", results
				.getString("full_name_nd"));
	}

	/**
	 * @throws SQLException
	 */
	public void test0733215() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM witheol");
		assertTrue(results.next());
		assertEquals("The name is wrong", "1", results.getString("key"));
		// would like to test the full_name_nd, but can't insert the Arabic
		// string in the code
		assertTrue(results.next());
		assertEquals("The name is wrong", "2", results.getString("key"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "3", results.getString("key"));
		assertEquals("The name is wrong", "123\n456\n789", results
				.getString("value"));
		assertTrue(!results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void test3091923() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".csv");
		props.put("separator", ";");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM badquoted");
		assertTrue(results.next());
		assertEquals("The name is wrong", "Rechtsform unbekannt", results.getString("F2"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Rechtsform \nunbekannt", results.getString("F2"));
		assertTrue(results.next());
		assertEquals("The name is wrong", "Rechtsform unbekannt", results.getString("F2"));
		try {
			results.next();
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: EOF reached inside quoted mode", "" + e);
		}
	}

	public void testColumnWithDot() throws SQLException {

		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT datum, tijd, station, ai007.000 as value FROM test-001-20081112");
		assertTrue(results.next());
		assertEquals("The name is wrong", "20-12-2007", results
				.getString("datum"));
		assertEquals("The name is wrong", "10:59:00", results.getString("tijd"));
		assertEquals("The name is wrong", "007", results.getString("station"));
		assertEquals("The name is wrong", "0.0", results.getString("value"));
	}

	/**
	 * accessing indexed files that do not exist is the same as accessing an
	 * empty table. no "file not found" error
	 * 
	 * @throws SQLException
	 */
	public void testFromNonExistingIndexedTable() throws SQLException {

		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT location,station,datum,tijd,file_date FROM test57");

		assertFalse(results.next());
	}

	public void testFromIndexedTable() throws SQLException {

		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT location,station,datum,tijd,file_date FROM test");

		ResultSetMetaData metadata = results.getMetaData();

		assertTrue("Incorrect Table Name", metadata.getTableName(0).equals(
				"test"));

		assertEquals("Incorrect Column Name 1", metadata.getColumnName(1),
				"LOCATION");
		assertEquals("Incorrect Column Name 2", metadata.getColumnName(2),
				"STATION");
		assertEquals("Incorrect Column Name 3", metadata.getColumnName(3),
				"DATUM");
		assertEquals("Incorrect Column Name 4", metadata.getColumnName(4),
				"TIJD");

		assertTrue(results.next());
		for (int i = 1; i < 12; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 12; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 12; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 36; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 36; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 36; i++) {
			assertTrue(results.next());
		}
		assertFalse(results.next());
	}

	public void testFromIndexedTablePrepend() throws SQLException {

		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("fileTailPattern", "-([0-9]{3})-([0-9]{8})");
		props.put("fileTailParts", "location,file_date");
		props.put("indexedFiles", "True");
		props.put("fileTailPrepend", "True");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT location,file_date,datum,tijd,station FROM test");

		ResultSetMetaData metadata = results.getMetaData();

		assertTrue("Incorrect Table Name", metadata.getTableName(0).equals(
				"test"));

		assertEquals("Incorrect Column Name 1", metadata.getColumnName(1),
				"LOCATION");
		assertEquals("Incorrect Column Name 1", metadata.getColumnName(2),
				"FILE_DATE");
		assertEquals("Incorrect Column Name 1", metadata.getColumnName(3),
				"DATUM");
		assertEquals("Incorrect Column Name 2", metadata.getColumnName(4),
				"TIJD");
		assertEquals("Incorrect Column Name 3", metadata.getColumnName(5),
				"STATION");

		assertTrue(results.next());
		for (int i = 1; i < 12; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 12; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 12; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 36; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 36; i++) {
			assertTrue(results.next());
		}
		for (int i = 0; i < 36; i++) {
			assertTrue(results.next());
		}
		assertFalse(results.next());
	}

	/**
	 * @throws SQLException
	 */
	public void testAddingFields() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id + ' ' + job as mix FROM sample4");
		assertTrue(results.next());
		assertEquals("The mix is wrong", "01 Project Manager", results
				.getString("mix"));
		assertTrue(results.next());
		assertEquals("The mix is wrong", "02 Project Manager", results
				.getString("mix"));
		assertTrue(results.next());
		assertEquals("The mix is wrong", "03 Finance Manager", results
				.getString("mix"));
		assertTrue(results.next());
		assertEquals("The mix is wrong", "04 Project Manager", results
				.getString("mix"));
		assertTrue(!results.next());
	}

	public void testReadingTime() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Object expect;
		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id, timeoffset FROM sample5");

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("12:30:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("12:35:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("12:40:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("12:45:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("01:00:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("01:00:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("01:00:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("00:00:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("00:10:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertTrue(results.next());
		expect = java.sql.Time.valueOf("01:23:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));

		assertFalse(results.next());
	}

	public void testAddingDatePlusTime() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Object expect;
		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id, start, timeoffset, start+timeoffset AS ts FROM sample5 WHERE id=41 OR id=4");

		assertTrue(results.next());
		expect = java.sql.Date.valueOf("2001-04-02");
		assertEquals("Date is a Date", expect.getClass(), results.getObject(
				"start").getClass());
		assertEquals("Date is a Date", expect, results.getObject("start"));
		expect = java.sql.Time.valueOf("12:30:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));
		expect = java.sql.Timestamp.valueOf("2001-04-02 12:30:00");
		assertEquals("adding Date to Time", expect.getClass(), results
				.getObject("ts").getClass());
		assertEquals("adding Date to Time", ((Timestamp) expect).toString(), toUTC
				.format(results.getObject("ts")) + ".0");

		assertTrue(results.next());
		expect = java.sql.Date.valueOf("2004-04-02");
		assertEquals("Date is a Date", expect.getClass(), results.getObject(
				"start").getClass());
		assertEquals("Date is a Date", expect, results.getObject("start"));
		expect = java.sql.Time.valueOf("01:00:00");
		assertEquals("Time is a Time", expect.getClass(), results.getObject(
				"timeoffset").getClass());
		assertEquals("Time is a Time", expect, results.getObject("timeoffset"));
		expect = java.sql.Timestamp.valueOf("2004-04-02 01:00:00");
		assertEquals("adding Date to Time", expect.getClass(), results
				.getObject("ts").getClass());
		assertEquals("adding Date to Time", ((Timestamp) expect).toString(), toUTC
				.format(results.getObject("ts")) + ".0");

		assertFalse(results.next());
	}

	public void testAddingTimestampPlusInteger() throws SQLException {
		// misusing Date+Time to get a Timestamp, but here we are just
		// interested in doing Timestamp +/- Integer
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		props.put("timeFormat", "HHmm");
		props.put("dateFormat", "yyyy-MM-dd");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Object expect;
		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id, start+timeoffset+61000 AS ts, start+timeoffset-61000 AS ts2 FROM sample5 WHERE id=41 OR id=4");

		assertTrue(results.next());
		expect = java.sql.Timestamp.valueOf("2001-04-02 12:31:01");
		assertEquals("adding Date + Time + Int", expect.getClass(), results
				.getObject("ts").getClass());
		assertEquals("adding Date to Time", ((Timestamp) expect).toString(), toUTC
				.format(results.getObject("ts")) + ".0");
		expect = java.sql.Timestamp.valueOf("2001-04-02 12:28:59");
		assertEquals("adding Date + Time - Int", expect.getClass(), results
				.getObject("ts2").getClass());
		assertEquals("adding Date to Time", ((Timestamp) expect).toString(), toUTC
				.format(results.getObject("ts2")) + ".0");

		assertTrue(results.next());
		expect = java.sql.Timestamp.valueOf("2004-04-02 01:01:01");
		assertEquals("adding Date to Time", expect.getClass(), results
				.getObject("ts").getClass());
		assertEquals("adding Date to Time", ((Timestamp) expect).toString(), toUTC
				.format(results.getObject("ts")) + ".0");
		expect = java.sql.Timestamp.valueOf("2004-04-02 00:58:59");
		assertEquals("adding Date to Time", expect.getClass(), results
				.getObject("ts2").getClass());
		assertEquals("adding Date to Time", ((Timestamp) expect).toString(), toUTC
				.format(results.getObject("ts2")) + ".0");

		assertFalse(results.next());
	}

	public void testWithComments() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "String,Int,Float,String");
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM with_comments");

		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("name", metadata.getColumnName(1));
		assertEquals("id", metadata.getColumnName(2));
		assertEquals("value", metadata.getColumnName(3));
		assertEquals("comment", metadata.getColumnName(4));

		assertTrue(results.next());
		assertEquals(new Integer(1), results.getObject(2));
		assertTrue(results.next());
		assertEquals(new Integer(2), results.getObject(2));
		assertTrue(results.next());
		assertEquals(new Integer(3), results.getObject(2));
		assertFalse(results.next());
	}

	public void testWithoutComments() throws SQLException {
		Properties props = new Properties();
		props.put("commentChar", "");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM sample5");

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

	public void testSkippingLeadingLines() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "String,Int,Float,String");
		props.put("skipLeadingLines", "3");
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM with_comments");

		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("name", metadata.getColumnName(1));
		assertEquals("id", metadata.getColumnName(2));
		assertEquals("value", metadata.getColumnName(3));
		assertEquals("comment", metadata.getColumnName(4));

		assertTrue(results.next());
		assertEquals(new Integer(1), results.getObject(2));
		assertTrue(results.next());
		assertEquals(new Integer(2), results.getObject(2));
		assertTrue(results.next());
		assertEquals(new Integer(3), results.getObject(2));
		assertFalse(results.next());
	}

	public void testNonParseableWithColumnTypes() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "String,String,String,String,Int,Float");
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");
		props.put("suppressHeaders", "true");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM with_leading_trash");

		assertTrue(results.next());
		assertEquals("12:20", results.getString(2));
		assertTrue(results.next());
		assertEquals("12:30", results.getString(2));
		assertTrue(results.next());
		assertEquals("12:40", results.getObject(2));
		assertFalse(results.next());
	}

	public void testNonParseableWithHeaderline() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "Date;Time;TimeZone;Unit;Quality;Value");
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");
		props.put("suppressHeaders", "true");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM with_leading_trash");

		assertTrue(results.next());
		assertEquals("12:20", results.getString(2));
		assertTrue(results.next());
		assertEquals("12:30", results.getString(2));
		assertTrue(results.next());
		assertEquals("12:40", results.getObject(2));
		assertFalse(results.next());
	}

	public void testNonParseable() throws SQLException {
		Properties props = new Properties();
		props.put("ignoreNonParseableLines", "True");
		props.put("separator", ";");
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM with_leading_trash");

		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("12:20", metadata.getColumnName(2));

		assertTrue(results.next());
		assertEquals("12:30", results.getString(2));
		assertTrue(results.next());
		assertEquals("12:40", results.getObject(2));
		assertFalse(results.next());
	}

	public void testVariableColumnCount() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		props.put("fileTailPrepend", "True");
		props.put("columnTypes", "String,Date,Time,String,Double");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();

		ResultSet results = null;
		ResultSetMetaData metadata;

		results = stmt.executeQuery("SELECT * FROM varlen1");
		metadata = results.getMetaData();
		assertEquals("file_date", metadata.getColumnName(1));
		assertEquals("Datum", metadata.getColumnName(2));
		assertEquals("Tijd", metadata.getColumnName(3));
		assertEquals("Station", metadata.getColumnName(4));
		assertEquals("P000", metadata.getColumnName(5));
		assertEquals("P001", metadata.getColumnName(6));
		assertEquals("P002", metadata.getColumnName(7));
		assertEquals("P003", metadata.getColumnName(8));

		results = stmt.executeQuery("SELECT * FROM varlen2");
		metadata = results.getMetaData();
		assertEquals("file_date", metadata.getColumnName(1));
		assertEquals("Datum", metadata.getColumnName(2));
		assertEquals("Tijd", metadata.getColumnName(3));
		assertEquals("Station", metadata.getColumnName(4));
		assertEquals("P000", metadata.getColumnName(5));
		assertEquals("P001", metadata.getColumnName(6));

		results = stmt.executeQuery("SELECT * FROM varlen1");
		assertTrue(results.next());
		assertEquals("007", results.getObject("Station"));
		assertEquals(new Double("26.54"), results.getObject("P003"));
		assertTrue(results.next());
		assertEquals("007", results.getObject("Station"));
		assertEquals(new Double("26.54"), results.getObject("P003"));
		assertTrue(results.next());
		assertEquals("007", results.getObject("Station"));
		assertEquals(new Double("26.54"), results.getObject("P003"));
		assertTrue(results.next());
		assertEquals("001", results.getObject("Station"));
		assertEquals(new Double("26.55"), results.getObject("P003"));
		assertFalse(results.next());

		results = stmt.executeQuery("SELECT * FROM varlen2");
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

	public void testTailPrepend() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		props.put("fileTailPrepend", "True");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();

		ResultSet results = null;
		ResultSetMetaData metadata = null;

		results = stmt.executeQuery("SELECT * FROM varlen1");
		metadata = results.getMetaData();
		assertEquals("file_date", metadata.getColumnName(1));
		assertEquals("Datum", metadata.getColumnName(2));
		assertEquals("Tijd", metadata.getColumnName(3));
		assertEquals("Station", metadata.getColumnName(4));
		assertEquals("P000", metadata.getColumnName(5));
		assertEquals("P001", metadata.getColumnName(6));
		assertEquals("P002", metadata.getColumnName(7));
		assertEquals("P003", metadata.getColumnName(8));

		results = stmt.executeQuery("SELECT * FROM varlen2");
		metadata = results.getMetaData();
		assertEquals("file_date", metadata.getColumnName(1));
		assertEquals("Datum", metadata.getColumnName(2));
		assertEquals("Tijd", metadata.getColumnName(3));
		assertEquals("Station", metadata.getColumnName(4));
		assertEquals("P000", metadata.getColumnName(5));
		assertEquals("P001", metadata.getColumnName(6));
	}

	public void testNonExistingTable() throws SQLException {
		Statement stmt = DriverManager.getConnection(
				"jdbc:relique:csv:" + filePath).createStatement();

		try {
			stmt.executeQuery("SELECT * FROM not_there");
			fail("Should not find the table 'not_there'");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Cannot open data file '"
					+ filePath + "/not_there.csv'  !", "" + e);
		}

		Properties props = new Properties();
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		stmt = DriverManager.getConnection("jdbc:relique:csv:" + filePath,
				props).createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM not_there");
		assertFalse("non existing indexed tables are seen as empty", results
				.next());
	}

	public void testDuplicatedColumnNamesPlainFails() throws SQLException {
		// no bug report, check discussion thread
		// https://sourceforge.net/projects/csvjdbc/forums/forum/56965/topic/2608197
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		// load CSV driver
		try {
			stmt.executeQuery("SELECT * FROM duplicate_headers");
			fail("expected exception java.sql.SQLException: Table contains duplicate column names");
		} catch (SQLException e) {
			assertEquals("wrong exception and/or exception text!",
					"java.sql.SQLException: Table contains duplicate column names",
					"" + e);
		}
	}

	public void testDuplicatedColumnNamesSuppressHeader() throws SQLException {
		// no bug report, check discussion thread
		// https://sourceforge.net/projects/csvjdbc/forums/forum/56965/topic/2608197
		Properties props = new Properties();
		props.put("suppressHeaders", "true");
		props.put("skipLeadingLines", "1");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM duplicate_headers");

		assertTrue(results.next());
		assertEquals("1:ID is wrong", "1", results.getString(1));
		assertEquals("2:ID is wrong", "2", results.getString(2));
		assertEquals("3:ID is wrong", "george", results.getString(3));
		assertEquals("4:ID is wrong", "joe", results.getString(4));

		assertTrue(results.next());
		assertEquals("1:ID is wrong", "2", results.getString(1));
		assertEquals("2:ID is wrong", "2", results.getString(2));
		assertEquals("3:ID is wrong", "aworth", results.getString(3));
		assertEquals("4:ID is wrong", "smith", results.getString(4));

		assertFalse(results.next());
	}

	/**
	 * This creates several sentences with where and tests they work
	 * 
	 * @throws SQLException
	 */
	public void testWithNonRepeatedQuotes() throws SQLException {
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM doublequoted");
		assertTrue(results.next());
		assertEquals(
				"\"Rechtsform unbekannt\" entsteht durch die Simulation zTELKUS. Es werden Simulationsregeln angewandt.",
				results.getString(10));
	}

	public void testWithNonRepeatedQuotesExplicitSQLStyle() throws SQLException {
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quotestyle", "SQL");
		props.put("commentChar", "C");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM doublequoted");
		assertTrue(results.next());
		assertEquals(
				"\"Rechtsform unbekannt\" entsteht durch die Simulation zTELKUS. Es werden Simulationsregeln angewandt.",
				results.getString(10));
	}

	public void testWithNonRepeatedQuotesExplicitCStyle() throws SQLException {
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quoteStyle", "C");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM doublequoted");
		// TODO: this should actually fail!  have a look at testEscapingQuotecharExplicitSQLStyle for the way to check an exception.
		assertTrue(results.next());
		assertEquals(
				"\"Rechtsform unbekannt\" entsteht durch die Simulation zTELKUS. Es werden Simulationsregeln angewandt.",
				results.getString(10));
	}

	public void testEscapingQuotecharExplicitCStyle() throws SQLException {
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quoteStyle", "C");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT F1 FROM doublequoted");
		assertTrue(results.next());
		assertTrue(results.next());
		assertEquals("doubling \"\"quotechar", results.getObject("F1"));
		assertTrue(results.next());
		assertEquals("escaping quotechar\"", results.getObject("F1"));
	}

	public void testEscapingQuotecharExplicitSQLStyle() throws SQLException {
		Properties props = new Properties();
		props.put("separator", ";");
		props.put("quotechar", "'");
		props.put("quoteStyle", "SQL");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT F1 FROM doublequoted");
		assertTrue(results.next());
		assertTrue(results.next());
		assertEquals("doubling \\\"\\\"quotechar", results.getObject("F1"));
		assertTrue(results.next());
		assertTrue(results.next());
		try {
			results.next();
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: EOF reached inside quoted mode", "" + e);
		}
	}

	public void testWithNoData() throws SQLException {
		Properties props = new Properties();
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM nodata");

		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("Aleph", metadata.getColumnName(1));
		assertEquals("Beth", metadata.getColumnName(2));
		assertEquals("Ghimel", metadata.getColumnName(3));
		assertEquals("Daleth", metadata.getColumnName(4));

		assertFalse(results.next());
	}

	public void testWithNoHeader() throws SQLException {
		Properties props = new Properties();
		props.put("commentChar", "#");
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM only_comments");
		assertFalse(results.next());
	}

	public void testConnectionName() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		String url = conn.getMetaData().getURL();
		assertTrue(url.startsWith("jdbc:relique:csv:"));
		assertTrue(url.endsWith("/testdata/"));
	}
	
	/**
	 * you can access columns that do not have a name by number
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	public void testNumericDefectiveHeaders() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		//props.put("defectiveHeaders", "True");
		
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM twoheaders");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("Empty Column Name 1", "", metadata.getColumnName(1));

		assertTrue(results.next());
		assertEquals("1 is wrong", "", results.getString(1));
		try {
			results.getString("");
			fail("expected exception java.sql.SQLException: Can't access columns with empty name by name");
		} catch (SQLException e) {
			assertEquals("wrong exception and/or exception text!",
					"java.sql.SQLException: Can't access columns with empty name by name",
					"" + e);
		}

		assertEquals("2 is wrong", "WNS925", results.getString(2));
		assertEquals("2 is wrong", "WNS925", results.getString("600-P1201"));

		assertTrue(results.next());
		assertEquals("1 is wrong", "2010-02-21 00:00:00", results.getObject(1));
		assertEquals("2 is wrong", "21", results.getString(2));
		assertEquals("3 is wrong", "20", results.getString(3));
	}
	
	public void testWithDefectiveHeaders() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("defectiveHeaders", "True");
		
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM twoheaders");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("Incorrect Column Name 1", "COLUMN1", metadata.getColumnName(1));
		assertEquals("Incorrect Column Name 2", "600-P1201", metadata.getColumnName(2));

		assertTrue(results.next());
		assertEquals("1 is wrong", "", results.getString(1));
		assertEquals("1 is wrong", "", results.getString("COLUMN1"));

		assertEquals("2 is wrong", "WNS925", results.getString(2));
		assertEquals("2 is wrong", "WNS925", results.getString("600-P1201"));

		assertTrue(results.next());
		assertEquals("1 is wrong", "2010-02-21 00:00:00", results.getObject(1));
		assertEquals("1 is wrong", "2010-02-21 00:00:00", results.getObject("COLUMN1"));
		assertEquals("2 is wrong", "21", results.getObject(2));
		assertEquals("3 is wrong", "20", results.getObject(3));
	}
	
	public void testSkipLeadingDataLines() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("columnTypes", "Timestamp,Double");
		props.put("defectiveHeaders", "True");
		props.put("skipLeadingDataLines", "1");
		
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM twoheaders");

		assertTrue(results.next());
		assertEquals("1 is wrong", "2010-02-21 00:00:00", toUTC.format(results
				.getObject(1)));
		assertEquals("1 is wrong", "2010-02-21 00:00:00", toUTC.format(results
				.getObject("COLUMN1")));
		assertEquals("2 is wrong", new Double(21), results.getObject(2));
		assertEquals("3 is wrong", new Double(20), results.getObject(3));
		assertEquals("4 is wrong", new Double(24), results.getObject(4));

	}
	
	public void testSkipLeadingDataFromIndexedFile() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "-([0-9]{8})");
		props.put("fileTailParts", "file_date");
		props.put("fileTailPrepend", "True");
		props.put("skipLeadingDataLines", "1");
		// Datum,Tijd,Station,P000,P001,P002,P003
		props.put("columnTypes", "String,Date,Time,String,Double");

		ResultSet results = null;

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();

		results = stmt.executeQuery("SELECT * FROM varlen1");
		assertTrue(results.next());
		assertEquals("007", results.getObject("Station"));
		assertEquals(new Double("26.54"), results.getObject("P003"));
		assertTrue(results.next());
		assertEquals("001", results.getObject("Station"));
		assertEquals(new Double("26.55"), results.getObject("P003"));
		assertFalse(results.next());
	}

	public void testIgnoreUnparseableInIndexedFile() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "(.)-20081112");
		props.put("fileTailParts", "index");
		// Datum,Tijd,Station,P000,P001,P002,P003
		props.put("columnNames", "Datum,Tijd,Station,P000,P001,P002,P003,INDEX");
		props.put("ignoreNonParseableLines", "True");

		ResultSet results = null;

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();

		results = stmt.executeQuery("SELECT * FROM varlen");
		assertTrue(results.next());
		assertEquals("1", results.getObject("index"));
		assertEquals("007", results.getObject("Station"));
		assertEquals("26.54", results.getObject("P001"));
		assertTrue(results.next());
		assertEquals("007", results.getObject("Station"));
		assertEquals("22.99", results.getObject("P001"));
		assertFalse(results.next());
	}

	public void testUnparseableInIndexedFileCausesSQLException() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		props.put("indexedFiles", "True");
		props.put("fileTailPattern", "(.)-20081112");
		props.put("fileTailParts", "index");
		// Datum,Tijd,Station,P000,P001,P002,P003
		props.put("columnNames", "Datum,Tijd,Station,P000,P001,P002,P003,INDEX");

		ResultSet results = null;

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();

		results = stmt.executeQuery("SELECT * FROM varlen");
		assertTrue(results.next());
		assertEquals("1", results.getObject("index"));
		assertEquals("007", results.getObject("Station"));
		assertEquals("26.54", results.getObject("P001"));
		assertTrue(results.next());
		assertEquals("007", results.getObject("Station"));
		assertEquals("22.99", results.getObject("P001"));
		try {
			results.next();
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: data contains 6 columns, expected 8", "" + e);
		}
	}

	// Actually this is more a Connection test than a Driver test
	public void testRaiseUnsupportedOperationException() throws SQLException {
		Properties props = new Properties();
		props.put("raiseUnsupportedOperationException", "false");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		conn.setAutoCommit(true);
		conn.getAutoCommit();
		conn.setReadOnly(true);
	}

	public void testTimestampInTimeZoneRome() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("timeZoneName", "Europe/Rome");
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1");

		assertTrue(results.next());
		// TODO: getString miserably fails!
		//assertEquals("2001-01-02 12:30:00.0", results.getString("start"));
		Timestamp got = (Timestamp) results.getObject("start");
		assertEquals("2001-01-02 11:30:00", toUTC.format(got));
		got = results.getTimestamp("start");
		assertEquals("2001-01-02 11:30:00", toUTC.format(got));

		assertTrue(results.next());
		got = results.getTimestamp("start");
		assertEquals("2004-04-02 10:30:00", toUTC.format(got));

		assertFalse(results.next());
	}

	public void testTimestampInTimeZoneSantiago() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("timeZoneName", "America/Santiago");
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1");

		assertTrue(results.next());
		// TODO: getString miserably fails!
		//assertEquals("2001-01-02 12:30:00", results.getString("start"));
		Timestamp got = (Timestamp) results.getObject("start");
		assertEquals("2001-01-02 15:30:00", toUTC.format(got));
		got = results.getTimestamp("start");
		assertEquals("2001-01-02 15:30:00", toUTC.format(got));

		assertTrue(results.next());
		got = results.getTimestamp("start");
		assertEquals("2004-04-02 16:30:00", toUTC.format(got));

		assertFalse(results.next());
	}

	public void testTimestampInTimeZoneGMTPlus0400() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("timeZoneName", "GMT+04:00");
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1");

		assertTrue(results.next());
		// TODO: getString miserably fails!
		// assertEquals("2001-01-02 12:30:00", results.getString("start"));
		Timestamp got = (Timestamp) results.getObject("start");
		assertEquals("2001-01-02 08:30:00", toUTC.format(got));
		got = results.getTimestamp("start");
		assertEquals("2001-01-02 08:30:00", toUTC.format(got));

		assertTrue(results.next());
		got = results.getTimestamp("start");
		assertEquals("2004-04-02 08:30:00", toUTC.format(got));

		assertFalse(results.next());
	}

	public void testTimestampInTimeZoneGMTMinus0400() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("timeZoneName", "GMT-04:00");
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample5 WHERE ID=9 OR ID=1");

		assertTrue(results.next());
		// TODO: getString miserably fails!
		// assertEquals("2001-01-02 12:30:00", results.getString("start"));
		Timestamp got = (Timestamp) results.getObject("start");
		assertEquals("2001-01-02 16:30:00", toUTC.format(got));
		got = results.getTimestamp("start");
		assertEquals("2001-01-02 16:30:00", toUTC.format(got));

		assertTrue(results.next());
		got = results.getTimestamp("start");
		assertEquals("2004-04-02 16:30:00", toUTC.format(got));

		assertFalse(results.next());
	}

	public void testAddingDateToTimeInTimeZoneAthens() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("timeZoneName", "Europe/Athens");
		props.put("columnTypes", "Int,String,Date,Time");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT ID, NAME, D + T as DT FROM sample8");

		assertTrue(results.next());
		// TODO: getString miserably fails!
		//assertEquals("2001-01-02 12:30:00", results.getString("start"));
		Timestamp got = (Timestamp) results.getObject("DT");
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

	public void testAddingDateToTimeInTimeZoneGMTMinus0500() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("timeZoneName", "GMT-05:00");
		props.put("columnTypes", "Int,String,Date,Time");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt
				.executeQuery("SELECT ID, NAME, D + T as DT FROM sample8");

		assertTrue(results.next());
		// TODO: getString miserably fails!
		//assertEquals("2001-01-02 12:30:00", results.getString("start"));
		Timestamp got = (Timestamp) results.getObject("DT");
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

	public void testValuesContainQuotes() throws SQLException {
		Properties props = new Properties();
		props.put("separator", ",");
		props.put("quotechar", "'");
		props.put("fileExtension", ".txt");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM uses_quotes");
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

	public void testWithDifferentDecimalSeparator() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", "");
		props.put("separator", ";");
		props.put("columnTypes", "String,String,Double");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");
		assertTrue(results.next());
		assertTrue(results.next());
		assertTrue(results.next());
		assertTrue(results.next());
		assertTrue(results.next());
		assertTrue(results.next());
		assertTrue(results.next()); // Mackie Messer
		assertEquals(new Double(34.1), results.getObject(3));
		assertTrue(results.next()); // Polly Peachum
		assertEquals(new Double(30.5), results.getObject(3));
		
	}

	public void testLiteral() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT name, id, 'Bananas' as t FROM sample");

		results.next();
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect ID Value", "Bananas", results.getString("T"));
		assertEquals("Incorrect ID Value", "Bananas", results.getString("t"));
		results.next();
		assertEquals("Incorrect ID Value", "Bananas", results.getString("T"));
		results.next();
		assertEquals("Incorrect ID Value", "Bananas", results.getString("T"));
		results.next();
		assertEquals("Incorrect ID Value", "Bananas", results.getString("T"));
	}

	public void testTableAlias() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT S.id, s.Extra_field FROM sample S");

		results.next();
		assertEquals("Incorrect ID Value", "Q123", results.getString("ID"));
		assertEquals("Incorrect ID Value", "F", results.getString("EXTRA_FIELD"));
		assertEquals("Incorrect ID Value", "Q123", results.getString(1));
		assertEquals("Incorrect ID Value", "F", results.getString(2));
	}
	
	public void testTableAliasWithWhere() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT * FROM sample AS S where S.ID='A123'");

		results.next();
		assertEquals("Incorrect ID Value", "A123", results.getString("ID"));
		assertEquals("Incorrect ID Value", "A", results.getString("EXTRA_FIELD"));
		assertEquals("Incorrect ID Value", "A123", results.getString(1));
		assertEquals("Incorrect ID Value", "A", results.getString(3));
		assertFalse(results.next());
	}

	public void testMaxRows() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();
		stmt.setMaxRows(4);
		assertEquals("getMaxRows() incorrect", 4, stmt.getMaxRows());

		ResultSet results = stmt.executeQuery("SELECT * FROM sample5");
		// The maxRows value at the time of the query should be used, not the value below.
		stmt.setMaxRows(7);

		assertTrue("Reading row 1 failed", results.next());
		assertTrue("Reading row 2 failed", results.next());
		assertTrue("Reading row 3 failed", results.next());
		assertTrue("Reading row 4 failed", results.next());
		assertFalse("Stopping after row 4 failed", results.next());
	}
}
