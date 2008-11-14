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

import java.sql.*;

import junit.framework.TestCase;

/**This class is used to test the CsvJdbc Scrollable driver.
*
* @author Chetan Gupta
* @version $Id: TestScrollableDriver.java,v 1.7 2008/11/14 14:20:28 mfrasca Exp $
*/
public class TestScrollableDriver extends TestCase
{ 
  public static final String SAMPLE_FILES_LOCATION_PROPERTY="sample.files.location";
  private String filePath;
  
	/**
	 * Create a test that will execute the method named on the parameter.
	 * This just wraps a call to the parent method.
	 */
	public TestScrollableDriver(String method) 
	{
		super(method);
	}
  
  protected void setUp()
  {
    filePath=System.getProperty(SAMPLE_FILES_LOCATION_PROPERTY);
    if (filePath == null)
    	filePath=RunTests.DEFAULT_FILEPATH;
    assertNotNull("Sample files location property not set !", filePath);

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

  public void testScroll() throws SQLException
  {

      // create a connection. The first command line parameter is assumed to
      //  be the directory in which the .csv files are held
      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

      // create a Statement object to execute the query with
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, 0);

      // Select the ID and NAME columns from sample.csv
      ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample");

      // dump out the results
      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"Q123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"\"S,\"");

      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Jonathan Ackerman");

      results.first();
      assertEquals("Incorrect ID Value",results.getString("ID"),"Q123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"\"S,\"");

      results.previous();
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"Q123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"\"S,\"");

      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Jonathan Ackerman");

      results.relative(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"C456");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Susan, Peter and Dave");

      results.relative(-3);
      assertEquals("Incorrect ID Value",results.getString("ID"),"Q123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"\"S,\"");

      results.relative(0);
      assertEquals("Incorrect ID Value",results.getString("ID"),"Q123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"\"S,\"");

      results.absolute(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Jonathan Ackerman");

      results.absolute(-2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"D789");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Amelia \"meals\" Maurice");

      results.last();
      assertEquals("Incorrect ID Value",results.getString("ID"),"X234");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Peter \"peg leg\", Jimmy & Samantha \"Sam\"");

      results.next();
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.previous();
      assertEquals("Incorrect ID Value",results.getString("ID"),"X234");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Peter \"peg leg\", Jimmy & Samantha \"Sam\"");

      results.relative(100);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.relative(-100);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.relative(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Jonathan Ackerman");

      results.absolute(7);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.absolute(-10);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.absolute(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Jonathan Ackerman");

      results.absolute(0);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.relative(0);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      // clean up
      results.close();
      stmt.close();
      conn.close();
  }
  
  /**
	 * TODO: probably broken by me, Mario Frasca. I'm not sure what to do here
	 * and I haven't enough time to investigate...
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
  public void testScrollWithMultiLineText() throws ClassNotFoundException, SQLException
  {
      // load the driver into memory
      Class.forName("org.relique.jdbc.csv.CsvDriver");

      // create a connection. The first command line parameter is assumed to
      //  be the directory in which the .csv files are held
      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

      // create a Statement object to execute the query with
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, 0);

      // Select the ID and NAME columns from sample.csv
      ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample2");

      // dump out the results
      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Aman");

      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"B223");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Binoy");

      results.first();
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Aman");

      results.previous();
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Aman");

      results.next();
      assertEquals("Incorrect ID Value",results.getString("ID"),"B223");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Binoy");

      results.relative(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"D456");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Dilip \"meals\" Maurice ~In New  LIne~ Done");

      results.relative(-3);
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Aman");

      results.relative(0);
      assertEquals("Incorrect ID Value",results.getString("ID"),"A123");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Aman");

      results.absolute(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"B223");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Binoy");

      results.absolute(-2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"E589");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"\"Elephant\"");

      results.last();
      assertEquals("Incorrect ID Value",results.getString("ID"),"F634");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Fandu \"\"peg leg\"\", Jimmy & Samantha ~In Another New  LIne~ \"\"Sam\"\"");

      results.next();
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.previous();
      assertEquals("Incorrect ID Value",results.getString("ID"),"F634");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Fandu \"\"peg leg\"\", Jimmy & Samantha ~In Another New  LIne~ \"\"Sam\"\"");

      results.relative(100);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.relative(-100);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.relative(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"B223");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Binoy");

      results.absolute(7);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.absolute(-10);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.absolute(2);
      assertEquals("Incorrect ID Value",results.getString("ID"),"B223");
      assertEquals("Incorrect NAME Value",results.getString("NAME"),"Binoy");

      results.absolute(0);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      results.relative(0);
      assertNull(results.getString("ID"));
      assertNull(results.getString("NAME"));

      // clean up
      results.close();
      stmt.close();
      conn.close();
  }

  /**
   * This checks for the scenario when due to where clause no rows are returned.
 * @throws SQLException 
   */
  public void testWhereNoResults() throws SQLException {
  	  Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
      // create a Statement object to execute the query with
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, 0);

      ResultSet results = stmt.executeQuery("SELECT ID,Name FROM sample4 WHERE ID=05");
      assertFalse("There are some junk records found", results.next());
      results.last();
      assertNull("Invalid Id", results.getString("ID"));
      assertNull("Invalid Name", results.getString("NAME"));
      assertTrue("Is not last", results.isLast());
      results.absolute(0);
      assertNull("Invalid Id", results.getString("ID"));
      assertNull("Invalid Name", results.getString("NAME"));
      assertTrue("Is not last", results.isLast());
      results.absolute(0);
      assertTrue("Is not before first", results.isBeforeFirst());
      results.previous();
      assertTrue("Is not before first", results.isBeforeFirst());
      assertTrue("Is not last", results.isLast());
      //Following throws exception
      //assertTrue("Is not before first", results.isAfterLast());

  }

  /**
   * This checks for the scenario when we have single record
   */
  public void testWhereSingleRecord() {
  	try {
  	  Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
      // create a Statement object to execute the query with
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, 0);

      ResultSet results = stmt.executeQuery("SELECT ID,Name FROM singlerecord");
      assertTrue(results.last());
      assertEquals("Invalid Id", "A123", results.getString("ID"));
      assertEquals("Invalid Name", "Jonathan Ackerman", results.getString("NAME"));
      results.absolute(1);
      assertEquals("Invalid Id", "A123", results.getString("ID"));
      assertEquals("Invalid Name", "Jonathan Ackerman", results.getString("NAME"));
      assertTrue("Is not last", results.isLast());
      assertTrue("Is not first", results.isFirst());
      results.absolute(0);
      assertTrue("Is not before first", results.isBeforeFirst());
      results.previous();
      assertTrue("Is not before first", results.isBeforeFirst());
      assertTrue(results.next());
      assertEquals("Invalid Id", "A123", results.getString("ID"));
      assertEquals("Invalid Name", "Jonathan Ackerman", results.getString("NAME"));
      results.relative(1);
      assertTrue("Is not after last", results.isAfterLast());
      results.previous();
      assertEquals("Invalid Id", "A123", results.getString("ID"));
      assertEquals("Invalid Name", "Jonathan Ackerman", results.getString("NAME"));
  	}
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }

  
  /**
   * This tests for the scenario with where clause.
   */
  public void testWhereMultipleResult() {
  	try {
  	  Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, 0);

      
      ResultSet results = stmt.executeQuery("SELECT ID, Name, Job FROM sample4 WHERE Job = 'Project Manager'");
      assertTrue(results.next());
      assertEquals("The ID is wrong","01",results.getString("ID"));
      assertTrue(results.next());
      assertEquals("The ID is wrong","02",results.getString("ID"));
      assertTrue(results.next());
      assertEquals("The ID is wrong","04",results.getString("ID"));
      assertTrue(results.first());
      assertEquals("The ID is wrong when using first","01",results.getString("ID"));

      assertTrue(results.absolute(3));
      assertEquals("The ID is wrong","04",results.getString("ID"));
     
      assertTrue(results.last());
      assertEquals("The ID is wrong","04",results.getString("ID"));
      assertFalse("It has records after last", results.next());
      assertTrue("Is not after Last", results.isAfterLast());
      assertTrue(results.previous());
      assertTrue(results.previous());
      assertEquals("The ID is wrong","02",results.getString("ID"));
      assertTrue(results.relative(0));
      assertEquals("The ID is wrong","02",results.getString("ID"));
      assertTrue(results.relative(1));
      assertEquals("The ID is wrong","04",results.getString("ID"));
      assertTrue("Is not last", results.isLast());
      assertTrue(results.relative(-2));
      assertEquals("The ID is wrong","01",results.getString("ID"));
      assertTrue("Is not first", results.isFirst());
      results.previous();
      assertTrue("Is not before first", results.isBeforeFirst());

  	}
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }


}
