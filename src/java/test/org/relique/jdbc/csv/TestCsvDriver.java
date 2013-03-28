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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.TestCase;

import org.relique.jdbc.csv.CsvResultSet;

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
			assertEquals("java.sql.SQLException: Column not found: Job", "" + e);
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

	public void testFindColumn() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM sample");

		assertEquals("Incorrect Column", 1, results.findColumn("ID"));
		assertEquals("Incorrect Column", 2, results.findColumn("Name"));
		assertEquals("Incorrect Column", 3, results.findColumn("EXTRA_FIELD"));

		try {
			results.findColumn("foo");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Column not found: foo", "" + e);
		}

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

	public void testMetadataWithColumnSize() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id+1 AS ID1, name, job, start FROM sample5");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("size of column 1 is incorrect", 20, metadata
				.getColumnDisplaySize(1));
		assertEquals("size of column 2 is incorrect", 20, metadata
				.getColumnDisplaySize(2));
		assertEquals("size of column 3 is incorrect", 20, metadata
				.getColumnDisplaySize(3));
		assertEquals("size of column 4 is incorrect", 20, metadata
				.getColumnDisplaySize(4));

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

	public void testMetadataColumnLabels() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT id * 10 as XID, name, 1000 as dept FROM sample5");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("name of column 1 is incorrect", "XID", metadata.getColumnName(1));
		assertEquals("label of column 1 is incorrect", "XID", metadata.getColumnLabel(1));
		assertEquals("name of column 2 is incorrect", "NAME", metadata.getColumnName(2));
		assertEquals("label of column 2 is incorrect", "NAME", metadata.getColumnLabel(2));
		assertEquals("name of column 3 is incorrect", "DEPT", metadata.getColumnName(3));
		assertEquals("label of column 3 is incorrect", "DEPT", metadata.getColumnLabel(3));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testDatabaseMetadataTableTypes() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getTableTypes();
		assertTrue(results.next());
		assertEquals("Wrong table type", "TABLE", results.getString(1));
		assertFalse(results.next());
	}

	public void testDatabaseMetadataSchemas() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getSchemas();
		assertTrue(results.next());
		assertEquals("Wrong schema", "PUBLIC", results.getString(1));
		assertFalse(results.next());
	}

	public void testDatabaseMetadataProcedures() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getProcedures(null, null, "*");
		assertFalse(results.next());
	}

	public void testDatabaseMetadataUDTs() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getUDTs(null, null, "test", null);
		assertFalse(results.next());
	}

	public void testDatabaseMetadataPrimaryKeys() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getPrimaryKeys(null, null, "sample");
		assertFalse(results.next());
	}

	public void testDatabaseMetadataCatalogs() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getCatalogs();
		assertFalse(results.next());
	}

	public void testDatabaseMetadataTypeInfo() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet results = metadata.getTypeInfo();
		assertTrue(results.next());
		assertEquals("TYPE_NAME is wrong", "String", results.getString("TYPE_NAME"));
		assertEquals("DATA_TYPE is wrong", Types.VARCHAR, results.getInt("DATA_TYPE"));
		assertEquals("NULLABLE is wrong", DatabaseMetaData.typeNullable, results.getShort("NULLABLE"));
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

			stmt.executeQuery("SELECT Id, Name FROM sample");
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

			stmt.executeQuery("SELECT Id, XXXX FROM sample");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column name: XXXX", "" + e);
		}
	}

	public void testEmptyColumnTypesFails() throws SQLException {		
		try {
			Properties props = new Properties();
			props.put("columnTypes", ",");
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
					+ filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT * FROM sample");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column types: ,", "" + e);
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

	public void testColumnTypesWithMultipleTables() throws SQLException,
			ParseException {
		Properties props = new Properties();
		props.put("columnTypes.sample5", "Int,String,String,Timestamp");
		props.put("columnTypes.sample", "String");
		// Give empty list so column types are inferred from data.
		props.put("columnTypes.numeric", "");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * from sample5");
		assertTrue(results.next());
		assertEquals("The sample5 ID is wrong", new Integer(41), results.getObject("id"));
		assertEquals("The sample5 Job is wrong", "Piloto", results.getObject("job"));
		results.close();
		results = stmt.executeQuery("SELECT ID,EXTRA_FIELD from sample");
		assertTrue(results.next());
		assertEquals("The sample ID is wrong", "Q123", results.getObject(1));
		assertEquals("The sample EXTRA_FIELD is wrong", "F", results.getObject(2));
		results.close();

		// column types are inferred from data.
		results = stmt.executeQuery("SELECT C2, 'X' as X from numeric");
		assertTrue(results.next());
		assertEquals("The numeric C2 is wrong", new Integer(-1010), results.getObject(1));
		assertEquals("The numeric X is wrong", "X", results.getObject(2));
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

	public void testWithSuppressedHeadersMultiline() throws SQLException {
		Properties props = new Properties();
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM embassies");

		// Test selecting from a file with a multi-line value as the first record.
		assertTrue(results.next());
		assertEquals("Incorrect COLUMN1 Value", "Germany", results.getString("COLUMN1"));
		assertEquals("Incorrect COLUMN2 Value", "Wallstrasse 76-79,\n10179 Berlin", results.getString("COLUMN2"));
		assertTrue(results.next());
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
		assertEquals("ID", results.getMetaData().getColumnLabel(1).toString());

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

	public void testColumnWithoutAlias() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT 44, lower(job), ID*2, 'hello', 44 from sample4");
		assertTrue("no results found", results.next());
		assertEquals("Number 44 is wrong", 44, results.getInt(1));
		assertEquals("lower(job) is wrong", "project manager", results.getString(2));
		assertEquals("ID*2 is wrong", 2, results.getInt(3));
		assertEquals("String 'hello' is wrong", "hello", results.getString(4));
		assertEquals("Number 44 is wrong", 44, results.getInt(5));
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
	 * 
	 * @throws SQLException
	 */
	public void testSelectStarWithTableAlias() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT tbl.* FROM sample4 tbl");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertEquals("The Job is wrong", "Project Manager", results.getString(3));
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
			assertEquals("java.sql.SQLException: Column not found: id", "" + e);
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

	public void testWhereWithDates() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,Date,Time");
		props.put("dateFormat", "yyyy-MM-dd");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT ID from sample8 where d = '2010-02-02'");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt(1));
		assertFalse(results.next());

		results = stmt.executeQuery("SELECT ID from sample8 where '2010-03-24' < d");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt(1));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 6, results.getInt(1));
		assertFalse(results.next());
	}

	public void testWhereWithIn() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Name from sample5 where id in (3, 4, 5)");

		assertTrue(results.next());
		assertEquals("The Name is wrong", "Maria Cristina Lucero", results.getString("Name"));
		assertTrue(results.next());
		assertEquals("The Name is wrong", "Felipe Grajales", results.getString("Name"));
		assertTrue(results.next());
		assertEquals("The Name is wrong", "Melquisedec Rojas Castillo", results.getString("Name"));
		assertFalse(results.next());
	}

	public void testWhereWithInNoResults() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Name from sample5 where id in (23, 24, 25)");

		assertFalse(results.next());
	}

	public void testWhereWithNotIn() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Id from sample where Id not in ('A123', 'B234', 'X234')");

		assertTrue(results.next());
		assertEquals("The Id is wrong", "Q123", results.getString("Id"));
		assertTrue(results.next());
		assertEquals("The Id is wrong", "C456", results.getString("Id"));
		assertTrue(results.next());
		assertEquals("The Id is wrong", "D789", results.getString("Id"));
		assertFalse(results.next());
	}

	public void testWhereWithInEmpty() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

		Statement stmt = conn.createStatement();

		try {
			stmt.executeQuery("select * from sample where Name in ()");
			fail("SQL Query should fail");
		} catch (SQLException e) {
			assertTrue(e.getMessage().startsWith("Syntax Error"));
		}
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
			assertEquals("java.sql.SQLException: Column not found: id", "" + e);
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

	public void testNoPatternGroupFromIndexedTable() throws SQLException {

		Properties props = new Properties();
        props.put("fileExtension", ".txt");
        props.put("commentChar", "#");
        props.put("indexedFiles", "true");

        // No groups in ()'s used in regular expression
        props.put("fileTailPattern", ".*");
        props.put("suppressHeaders", "true");
        props.put("headerline", "BLZ,BANK_NAME");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM banks");

		assertTrue(results.next());
		assertEquals("The BLZ is wrong", "10000000", results.getString("BLZ"));
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

	/**
	 * @throws SQLException
	 */
	public void testAddingAndMultiplyingFields() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT 1 + ID * 2 as N1, ID * -3 + 1 as N2, ID+1+2*3+4 as N3 FROM sample5");
		assertTrue(results.next());
		assertEquals("N1 is wrong", 83, results.getInt("N1"));
		assertEquals("N2 is wrong", -122, results.getInt("N2"));
		assertEquals("N3 is wrong", 52, results.getInt("N3"));
	}

	/**
	 * @throws SQLException
	 */
	public void testParentheses() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT ( ID + 1 ) as N1, ((ID + 2) * 3) as N2, (3) as N3 FROM sample5 where ( Job = 'Piloto' )");
		assertTrue(results.next());
		assertEquals("N1 is wrong", 42, results.getInt("N1"));
		assertEquals("N2 is wrong", 129, results.getInt("N2"));
		assertEquals("N3 is wrong", 3, results.getInt("N3"));
	}

	/**
	 * @throws SQLException
	 */
	public void testBadParenthesesFails() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		try {
			stmt.executeQuery("SELECT ((ID + 1) as N1 FROM sample5");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertTrue(e.getMessage().startsWith("Syntax Error"));	
		}
	}

	public void testLowerFunction() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select lower(job) as ljob, lower('AA') as CAT from sample5");
		assertTrue(results.next());
		assertEquals("The JOB is wrong", "piloto", results.getString(1));
		assertEquals("The CAT is wrong", "aa", results.getString(2));
		assertTrue(results.next());
		assertEquals("The JOB is wrong", "project manager", results.getString(1));
		assertEquals("The CAT is wrong", "aa", results.getString(2));

		results = stmt.executeQuery("select ID from sample5 where lower(job) = lower('FINANCE MANAGER')");
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt(1));
		assertFalse(results.next());		
	}

	public void testUpperFunction() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "BLZ,BANK_NAME");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,String");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select UPPER(BANK_NAME) as UC, UPPER('Credit' + 'a') as N2, upper(7) as N3 from banks");
		assertTrue(results.next());
		assertEquals("The BANK_NAME is wrong", "BUNDESBANK (BERLIN)", results.getString(1));
		assertEquals("N2 is wrong", "CREDITA", results.getString(2));
		assertEquals("N3 is wrong", "7", results.getString(3));

		results = stmt.executeQuery("select BLZ from banks where UPPER(BANK_NAME) = 'POSTBANK (BERLIN)'");
		assertTrue(results.next());
		assertEquals("The BLZ is wrong", 10010010, results.getInt(1));
		assertFalse(results.next());		
	}

	public void testLengthFunction() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Date,Time");
		props.put("charset", "UTF-8");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select length(Name) as X, length(Job) as Y, length('') as Z from sample5 where id = 8");
		assertTrue(results.next());
		assertEquals("The Length is wrong", 27, results.getInt(1));
		assertEquals("The Length is wrong", 15, results.getInt(2));
		assertEquals("The Length is wrong", 0, results.getInt(3));
		assertFalse(results.next());		
	}

	public void testRoundFunction() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Byte,Short,Integer,Long,Float,Double,BigDecimal");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select ROUND(11.77) as R1, ROUND('11.77') as R2, ROUND(C2) as R3, round(C3/7.0) as R4 from numeric where ROUND(C5) = 3");
		assertTrue(results.next());
		assertEquals("R1 is wrong", 12, results.getInt(1));
		assertEquals("R2 is wrong", 12, results.getInt(2));
		assertEquals("R3 is wrong", -1010, results.getInt(3));
		assertEquals("R4 is wrong", 42871, results.getInt(4));
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

	public void testBadQuotechar() throws SQLException {
		Properties props = new Properties();
		props.put("quotechar", "()");

		try {
			DriverManager.getConnection("jdbc:relique:csv:"	+ filePath, props);
			fail("expected exception java.sql.SQLException");
		} catch (SQLException e) {
			assertEquals("Wrong exception text",
					"java.sql.SQLException: Invalid quotechar: ()", "" + e);
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

	public void testConnectionClosed() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		assertFalse(conn.isClosed());
		conn.close();
		assertTrue(conn.isClosed());

		/*
		 * Second close is ignored.
		 */
		conn.close();
		try {
			conn.createStatement();
			fail("expected exception java.sql.SQLException");			
		} catch (SQLException e) {
			assertEquals("wrong exception and/or exception text!",
				"java.sql.SQLException: Connection is already closed", "" + e);
		}
	}

	public void testStatementClosed() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);
		Statement stmt = conn.createStatement();
		assertFalse(stmt.isClosed());
		stmt.close();
		assertTrue(stmt.isClosed());

		/*
		 * Second close is ignored.
		 */
		stmt.close();

		try {
			stmt.executeQuery("SELECT * FROM sample");
			fail("expected exception java.sql.SQLException");			
		} catch (SQLException e) {
			assertEquals("wrong exception and/or exception text!",
				"java.sql.SQLException: Statement is already closed", "" + e);
		}
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

	public void testWithHeaderMissingColumns() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("defectiveHeaders", "True");
		
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM defectiveheader");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("Incorrect Column Name 1", "IDX", metadata.getColumnName(1));
		assertEquals("Incorrect Column Name 2", "COLUMN2", metadata.getColumnName(2));
		assertEquals("Incorrect Column Name 3", "COLUMN3", metadata.getColumnName(3));
		assertEquals("Incorrect Column Name 4", "COMMENT", metadata.getColumnName(4));
		assertEquals("Incorrect Column Name 5", "COLUMN5", metadata.getColumnName(5));

		assertTrue(results.next());
		assertEquals("1 is wrong", "178", results.getString(1));
		assertEquals("2 is wrong", "AAX", results.getString("COLUMN2"));
		assertEquals("3 is wrong", "ED+", results.getString(3));
		assertEquals("4 is wrong", "NONE", results.getString(4));
		assertEquals("5 is wrong", "T", results.getString("COLUMN5"));
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

	public void testFetchSize() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();
		stmt.setFetchSize(50);
		assertEquals("getFetchSize() incorrect", 50, stmt.getFetchSize());

		ResultSet results = stmt.executeQuery("SELECT * FROM sample5");
		assertEquals("getFetchSize() incorrect", 50, results.getFetchSize());
		results.setFetchSize(20);
		assertEquals("getFetchSize() incorrect", 20, results.getFetchSize());
	}

	public void testResultSet() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();
		// No query executed yet.
		assertNull(stmt.getResultSet());
		assertFalse(stmt.getMoreResults());
		assertEquals("Update count wrong", -1, stmt.getUpdateCount());

		ResultSet results1 = stmt.executeQuery("SELECT * FROM sample5");		
		ResultSet results2 = stmt.getResultSet();
		assertEquals("Result sets not equal", results1, results2);
		assertEquals("Update count wrong", -1, stmt.getUpdateCount());
		assertFalse(stmt.getMoreResults());
		assertNull("Result set not null", stmt.getResultSet());
		assertTrue("Result set was not closed", results1.isClosed());
	}

	public void testResultSetClosed() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results1 = stmt.executeQuery("SELECT * FROM sample5");
		ResultSet results2 = stmt.executeQuery("SELECT * FROM sample");
		assertTrue("First result set is not closed", results1.isClosed());
		assertFalse("Second result set is closed", results2.isClosed());
		try {
			results1.next();
			fail("Closed result set should throw SQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: ResultSet is already closed", "" + e);
		}
	}

	public void testHeaderlineWithMultipleTables() throws SQLException {
		Properties props = new Properties();
		// Define different headerline values for table banks and table transactions. 
		props.put("headerline.banks", "BLZ,BANK_NAME");
		props.put("headerline.transactions", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM banks where BANK_NAME like 'Sparkasse%'");

		// Check that column names from headerline are used for table banks.
		assertEquals("BLZ wrong", "BLZ", results.getMetaData().getColumnName(1));
		assertEquals("BANK_NAME wrong", "BANK_NAME", results.getMetaData().getColumnName(2));
		assertFalse(results.next());
		results.close();
		results = stmt.executeQuery("SELECT * FROM transactions");
		assertTrue(results.next());

		// Check that column names for table transactions are correct too.
		assertEquals("TRANS_DATE wrong", "19-10-2011", results.getString("TRANS_DATE"));
		assertEquals("FROM_ACCT wrong", "3670345", results.getString("FROM_ACCT"));
		assertEquals("AMOUNT wrong", "250.00", results.getString("AMOUNT"));
	}

	public void testWarnings() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT * FROM sample");
		assertTrue(results.next());
		assertNull("Warnings should be null", results.getWarnings());
		results.clearWarnings();
		assertNull("Warnings should still be null", results.getWarnings());
	}

	public void testStringWasNull() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT ID FROM sample");
		assertTrue(results.next());
		assertEquals("ID wrong", "Q123", results.getString(1));
		assertFalse(results.wasNull());
	}

	public void testObjectWasNull() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("SELECT ID FROM sample");
		assertTrue(results.next());
		assertEquals("ID wrong", "Q123", results.getObject(1));
		assertFalse(results.wasNull());
	}

	public void testTableReader() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * FROM airline where code='BA'");
		assertTrue(results.next());
		assertEquals("NAME wrong", "British Airways", results.getString("NAME"));
	}

	public void testTableReaderWithBadTable() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
		Statement stmt = conn.createStatement();
		try {
			stmt.executeQuery("SELECT * FROM X");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Table does not exist: X", "" + e);
		}
	}

	public void testTableReaderWithBadReader() throws SQLException {
		try {
			DriverManager.getConnection("jdbc:relique:csv:class:" + TestCsvDriver.class.getName());
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Class does not implement interface org.relique.io.TableReader: " + TestCsvDriver.class.getName(), "" + e);
		}
	}

	public void testTableReaderTables() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
			TableReaderTester.class.getName());
		ResultSet results = conn.getMetaData().getTables(null, null, "*", null);
		assertTrue(results.next());
		assertEquals("TABLE_NAME wrong", "AIRLINE", results.getString("TABLE_NAME"));
		assertTrue(results.next());
		assertEquals("TABLE_NAME wrong", "AIRPORT", results.getString("TABLE_NAME"));
		assertFalse(results.next());
	}
	
	public void testTableReaderMetadata() throws SQLException {
		String url = "jdbc:relique:csv:class:" + TableReaderTester.class.getName();
		Connection conn = DriverManager.getConnection(url);
		assertEquals("URL is wrong", url, conn.getMetaData().getURL());
	}

	public void testPropertiesInURL() throws SQLException {		
		Properties props = new Properties();
		
		/*
		 * Use same directory name logic as in CsvDriver.connect.
		 */
		String path = filePath;
		if (!path.endsWith(File.separator))
			path += File.separator;
		String url = "jdbc:relique:csv:" + path + "?suppressHeaders=true&headerline=BLZ,NAME&commentChar=%23&fileExtension=.txt";
		Connection conn = DriverManager.getConnection(url, props);
		Statement stmt = conn.createStatement();

		assertEquals("The URL is wrong", url, conn.getMetaData().getURL());

		ResultSet results = stmt.executeQuery("SELECT * FROM banks");
		assertTrue(results.next());
		assertEquals("The BLZ is wrong", "10000000", results.getString("BLZ"));
		assertEquals("The NAME is wrong", "Bundesbank (Berlin)", results.getString("NAME"));
	}
	
	public void testLiteralWithUnicode() throws SQLException {
		Properties props = new Properties();
		props.put("charset", "UTF-8");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select id from sample5 where name = '\u00C9rica Jeanine M\u00e9ndez M\u00e9ndez'");

		assertTrue(results.next());
		assertEquals("Incorrect ID Value", "08", results.getString(1));
		assertFalse(results.next());
	}
	
	public void testDistinct() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select distinct job from sample5");

		assertTrue(results.next());
		assertEquals("Incorrect distinct value 1", "Piloto", results.getString(1));
		assertTrue(results.next());
		assertEquals("Incorrect distinct value 2", "Project Manager", results.getString(1));
		assertTrue(results.next());
		assertEquals("Incorrect distinct value 3", "Finance Manager", results.getString(1));
		assertTrue(results.next());
		assertEquals("Incorrect distinct value 4", "Office Manager", results.getString(1));
		assertTrue(results.next());
		assertEquals("Incorrect distinct value 5", "Office Employee", results.getString(1));
		assertFalse(results.next());
	}
	
	public void testDistinctWithWhere() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select distinct TO_ACCT, TO_BLZ from transactions where AMOUNT<50");

		assertTrue(results.next());
		assertEquals("Incorrect distinct TO_ACCT value 1", 27234813, results.getInt(1));
		assertEquals("Incorrect distinct TO_BLZ value 1", 10020500, results.getInt(2));
		assertTrue(results.next());
		assertEquals("Incorrect distinct TO_ACCT value 2", 3670345, results.getInt(1));
		assertEquals("Incorrect distinct TO_BLZ value 2", 10010010, results.getInt(2));
		assertFalse(results.next());
	}
	
	public void testNoTable() throws SQLException, ParseException {

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT 'Hello', 'World' as W, 5+7");

		assertTrue(results.next());
		ResultSetMetaData metadata = results.getMetaData();
		assertEquals("column 1 type is incorrect", Types.VARCHAR, metadata.getColumnType(1));
		assertEquals("column 2 type is incorrect", Types.VARCHAR, metadata.getColumnType(2));
		assertEquals("column 3 type is incorrect", Types.INTEGER, metadata.getColumnType(3));

		assertEquals("column 2 name is incorrect", "W", metadata.getColumnName(2));

		assertEquals("column 1 value is incorrect", "Hello", results.getString(1));
		assertEquals("column 2 value is incorrect", "World", results.getString(2));
		assertEquals("column 3 value is incorrect", 12, results.getInt(3));
		assertFalse(results.next());
	}

	public void testExtraResultSetNext() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Integer,String,String,Timestamp,Time");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT ID FROM sample5 WHERE ID < 3");
		assertTrue(results.next());
		assertTrue(results.next());
		assertFalse(results.next());
		assertFalse(results.next());
		assertFalse(results.next());
	}
}
