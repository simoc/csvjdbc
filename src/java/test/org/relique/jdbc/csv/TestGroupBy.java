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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.TestCase;

/**
 * Tests use of SQL GROUP BY clause.
 */
public class TestGroupBy extends TestCase {

	public static final String SAMPLE_FILES_LOCATION_PROPERTY = "sample.files.location";
	private String filePath;
	private DateFormat toUTC;

	public TestGroupBy(String name) {
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

	public void testGroupBySimple() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TO_BLZ from transactions group by TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertFalse(results.next());
	}
	
	public void testGroupByTwoColumns() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		props.put("dateFormat", "dd-mm-yyyy");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TRANS_DATE, TO_BLZ from transactions group by TRANS_DATE, TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-19"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-21"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-21"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-24"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-27"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-28"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertTrue(results.next());
		assertEquals("The TRANS_DATE is wrong", Date.valueOf("2011-10-31"), results.getDate("TRANS_DATE"));
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertFalse(results.next());
	}

	public void testGroupByNoResults() throws SQLException {
		Properties props = new Properties();
		props.put("fileExtension", ".txt");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select ID from empty-2 GROUP BY ID");
		assertFalse(results.next());
	}

	public void testGroupByAllDifferent() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select ID from sample4 GROUP BY ID");
		assertTrue(results.next());
		assertEquals("The ID is wrong", "01", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "02", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "03", results.getString("ID"));
		assertTrue(results.next());
		assertEquals("The ID is wrong", "04", results.getString("ID"));
		assertFalse(results.next());
	}
	
	public void testGroupByWhere() throws SQLException {
		Properties props = new Properties();
		props.put("columnTypes", "Int,String,String,Timestamp");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample5 WHERE ID >= 5 GROUP BY Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Piloto", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Employee", results.getString("Job"));
		assertFalse(results.next());
	}

	public void testGroupByOrderBy() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample4 GROUP BY Job ORDER BY Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertFalse(results.next());
	}
	
	public void testGroupByCount() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TO_BLZ, COUNT(TO_BLZ) AS N from transactions group by TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 5, results.getInt("N"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 2, results.getInt("N"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 1, results.getInt("N"));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertEquals("The COUNT is wrong", 1, results.getInt("N"));
		assertFalse(results.next());
	}

	/**
	 * Compare two values for near equality, allowing for floating point round-off.
	 */
	private boolean fuzzyEquals(double a, double b) {
		return (a == b || Math.round(a * 1000) == Math.round(b * 1000));
	}

	public void testGroupByMinMax() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select TO_BLZ, MIN(AMOUNT) AS MIN_AMOUNT, MAX(AMOUNT) AS MAX_AMOUNT from transactions group by TO_BLZ");
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020500, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(21.23, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(250.00, results.getDouble("MAX_AMOUNT")));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010424, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(460.00, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(999.00, results.getDouble("MAX_AMOUNT")));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10020400, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(1012.74, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(1012.74, results.getDouble("MAX_AMOUNT")));
		assertTrue(results.next());
		assertEquals("The TO_BLZ is wrong", 10010010, results.getInt("TO_BLZ"));
		assertTrue("The MIN_AMOUNT is wrong", fuzzyEquals(7.23, results.getDouble("MIN_AMOUNT")));
		assertTrue("The MAX_AMOUNT is wrong", fuzzyEquals(7.23, results.getDouble("MAX_AMOUNT")));
		assertFalse(results.next());
	}

	public void testGroupByOrderByCount() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job, COUNT(Job) C from sample4 GROUP BY Job ORDER BY C DESC");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertEquals("The COUNT is wrong", 3, results.getInt("C"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertEquals("The COUNT is wrong", 1, results.getInt("C"));
		assertFalse(results.next());
	}

	public void testGroupByColumnNumber() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample4 GROUP BY 1");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
	}

	public void testGroupByTableAlias() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job from sample4 T GROUP BY T.Job");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Finance Manager", results.getString("Job"));
		assertFalse(results.next());
	}

	public void testGroupByExpression() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		props.put("dateFormat", "dd-mm-yyyy");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select FROM_ACCT + '/' + FROM_BLZ as KEY from transactions group by KEY");
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "3670345/10010010", results.getString("KEY"));
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "97540210/10020500", results.getString("KEY"));
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "58340576/10010010", results.getString("KEY"));
		assertTrue(results.next());
		assertEquals("The KEY is wrong", "2340529/10020200", results.getString("KEY"));
		assertFalse(results.next());
	}

	public void testGroupByWithLiteral() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt.executeQuery("select Job + '/' C1, 7 C2 from sample4 GROUP BY Job");
		assertTrue(results.next());
		assertEquals("The C1 is wrong", "Project Manager/", results.getString("C1"));
		assertEquals("The C2 is wrong", "7", results.getString("C2"));
		assertTrue(results.next());
		assertEquals("The C1 is wrong", "Finance Manager/", results.getString("C1"));
		assertEquals("The C2 is wrong", "7", results.getString("C2"));
		assertFalse(results.next());
	}

	public void testGroupByWithBadColumnName() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample group by XXXX");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column name: XXXX", "" + e);
		}
	}

	public void testSelectUngroupedColumn() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample4 group by Job");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Column not included in GROUP BY: ID", "" + e);
		}
	}
	
	public void testOrderByUngroupedColumn() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Job FROM sample4 group by Job order by Id");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: ORDER BY column not included in GROUP BY: ID", "" + e);
		}
	}

	public void testHavingSimple() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "TRANS_DATE,FROM_ACCT,FROM_BLZ,TO_ACCT,TO_BLZ,AMOUNT");
		props.put("suppressHeaders", "true");
		props.put("fileExtension", ".txt");
		props.put("commentChar", "#");
		props.put("columnTypes", "Date,Integer,Integer,Integer,Integer,Double");
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select FROM_BLZ from transactions group by FROM_BLZ having FROM_BLZ > 10020000");
		assertTrue(results.next());
		assertEquals("The FROM_BLZ is wrong", 10020500, results.getInt("FROM_BLZ"));
		assertTrue(results.next());
		assertEquals("The FROM_BLZ is wrong", 10020200, results.getInt("FROM_BLZ"));
		assertFalse(results.next());
	}
	
	public void testHavingCount() throws SQLException {
		Properties props = new Properties();
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

		Statement stmt = conn.createStatement();

		ResultSet results = stmt
				.executeQuery("select Job, COUNT(Job) from sample5 group by Job having COUNT(Job) > 1");
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Project Manager", results.getString("Job"));
		assertEquals("The COUNT(Job) is wrong", 3, results.getInt(2));
		assertTrue(results.next());
		assertEquals("The Job is wrong", "Office Employee", results.getString("Job"));
		assertEquals("The COUNT(Job) is wrong", 4, results.getInt(2));
		assertFalse(results.next());
	}
	
	public void testHavingWithBadColumnName() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample group by Id HAVING XXXX = 1");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid column name: XXXX", "" + e);
		}
	}
	
	public void testHavingUngroupedColumn() throws SQLException {		
		try {
			Properties props = new Properties();
			Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath, props);

			Statement stmt = conn.createStatement();

			stmt.executeQuery("SELECT Id FROM sample group by Id HAVING Name = 'foo'");
			fail("Should raise a java.sqlSQLException");
		} catch (SQLException e) {
			assertEquals("java.sql.SQLException: Invalid HAVING column: NAME", "" + e);
		}
	}
}
