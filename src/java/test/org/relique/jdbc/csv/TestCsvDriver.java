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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.sql.Types;

import junit.framework.TestCase;

/**
 * This class is used to test the CsvJdbc driver.
 * 
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Chetan Gupta
 * @author Mario Frasca
 * @version $Id: TestCsvDriver.java,v 1.15 2008/11/11 09:21:32 mfrasca Exp $
 */
public class TestCsvDriver extends TestCase {
	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;

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

	}

	public void testWithDefaultValues() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");

		results.next();
		assertTrue("Incorrect ID Value", results.getString("ID").equals("Q123"));
		assertTrue("Incorrect NAME Value", results.getString("NAME").equals(
				"\"S,\""));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("F"));

		assertTrue("Incorrect Column 1 Value", results.getString(1).equals(
				"\"S,\""));
		assertTrue("Incorrect Column 2 Value", results.getString(2).equals(
				"Q123"));
		assertTrue("Incorrect Column 3 Value", results.getString(3).equals("F"));

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
				"Susan, Peter and Dave"));
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
				"Peter \"peg leg\", Jimmy & Samantha \"Sam\""));
		assertTrue("Incorrect EXTRA_FIELD Value", results.getString(
				"EXTRA_FIELD").equals("G"));

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
		try{
			assertNull(results.getString("Job"));
			fail("Should not find the column 'Job'");
		} catch(SQLException e) {
		}
		assertTrue(!results.next());
	}

	/**
	 * fields in different order than in source file.
	 * 
	 * @throws SQLException
	 */
	public void testWhereMangled() throws SQLException {
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

		assertTrue("Incorrect Table Name", metadata.getTableName(0).equals(
				"sample3"));

		assertTrue("Incorrect Column Name 1", metadata.getColumnName(1).equals(
				"column 1"));
		assertTrue("Incorrect Column Name 2", metadata.getColumnName(2).equals(
				"column \"2\" two"));
		assertTrue("Incorrect Column Name 3", metadata.getColumnName(3).equals(
				"Column 3"));
		assertTrue("Incorrect Column Name 4", metadata.getColumnName(4).equals(
				"CoLuMn4"));
		assertTrue("Incorrect Column Name 5", metadata.getColumnName(5).equals(
				"COLumn5"));

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

		ResultSet results = stmt.executeQuery("SELECT id, name, job, start FROM sample5");

		ResultSetMetaData metadata = results.getMetaData();

		assertEquals("type of column 1 is incorrect", "Int", metadata.getColumnTypeName(1));
		assertEquals("type of column 2 is incorrect", "String", metadata.getColumnTypeName(2));
		assertEquals("type of column 3 is incorrect", "String", metadata.getColumnTypeName(3));
		assertEquals("type of column 4 is incorrect", "Timestamp", metadata.getColumnTypeName(4));

		assertEquals("type of column 1 is incorrect", Types.INTEGER, metadata.getColumnType(1));
		assertEquals("type of column 2 is incorrect", Types.CHAR, metadata.getColumnType(2));
		assertEquals("type of column 3 is incorrect", Types.CHAR, metadata.getColumnType(3));
		assertEquals("type of column 4 is incorrect", Types.TIMESTAMP, metadata.getColumnType(4));

		results.close();
		stmt.close();
		conn.close();
	}

	public void testColumnTypesUserSpecified() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Date");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");
		
		assertTrue(results.next());
		assertEquals("Integer column ID is wrong", new Integer(1), results.getObject("id"));
		assertEquals("Integer column 1 is wrong", new Integer(1), results.getObject(1));
		java.sql.Date shouldBe = java.sql.Date.valueOf("2001-01-02");
		assertEquals("Date column Start is wrong", shouldBe, results.getObject("start"));
		assertEquals("Date column 4 is wrong", shouldBe, results.getObject(4));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results.getObject("name"));
	}

	public void testColumnTypesUserSpecifiedTS() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");
		
		assertTrue(results.next());
		DateFormat dfp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		assertEquals("Integer column ID is wrong", new Integer(1), results.getObject("id"));
		assertEquals("Integer column 1 is wrong", new Integer(1), results.getObject(1));
		assertEquals("Date column Start is wrong", dfp.parse(results.getString("start")), results.getObject("start"));
		assertEquals("Date column 4 is wrong", dfp.parse(results.getString("start")), results.getObject(4));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results.getObject("name"));
	}
	
	/**
	 * TODO: this does not work yet!  first we must decide the behaviour!
	 * @throws SQLException
	 * @throws ParseException
	 */
	public void testColumnTypesInferFromData() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");
		
		assertTrue(results.next());
		DateFormat dfp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		assertEquals("Integer Column ID is wrong", new Integer(1), results.getObject("id"));
		assertEquals("Integer Column 1 is wrong", new Integer(1), results.getObject(1));
		assertEquals("Date Column start is wrong", dfp.parse(results.getString("start")), results.getObject("start"));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results.getObject("name"));
	}
	
	public void testColumnTypesDefaultBehaviour() throws SQLException, ParseException {
		Properties props = new Properties();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT ID, Name, Job, Start "
				+ "FROM sample5 WHERE Job = 'Project Manager'");
		
		assertTrue(results.next());
		assertEquals("the start time is wrong", "2001-01-02 12:30:00", results.getObject("start"));
		assertEquals("The ID is wrong", "01", results.getObject("id"));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results.getObject("name"));
	}
	
	public void testColumnTypesWithSelectStar() throws SQLException, ParseException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:"
				+ filePath, props);
		Statement stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery("SELECT * "
				+ "FROM sample5 WHERE Job = 'Project Manager'");
		
		assertTrue(results.next());
		assertEquals("the start time is wrong", "2001-01-02 12:30:00", results.getObject("start"));
		assertEquals("The ID is wrong", "01", results.getObject("id"));
		assertEquals("The Name is wrong", "Juan Pablo Morales", results.getObject("name"));
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
	 * TODO: an old patch has been made useless by Mario Frasca (that's me).  sorry...
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
				.executeQuery("SELECT * FROM sample4 WHERE id BETWEEN 2 AND 3");
				
		assertTrue(results.next());
		assertEquals("The ID is wrong", 2, results.getInt("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", 3, results.getInt("ID"));
		assertTrue(!results.next());
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
		try{
			assertNull(results.getString("id"));
			fail("Should not find the column 'id'");
		} catch(SQLException e) {
		}
		assertEquals("The name is wrong", "Felipe Grajales", results.getString("name"));
		assertTrue(!results.next());
	}

}