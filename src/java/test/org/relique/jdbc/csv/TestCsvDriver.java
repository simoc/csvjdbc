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

import java.io.*;
import java.sql.*;
import java.util.Properties;
import junit.framework.*;

/**This class is used to test the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Chetan Gupta
 * @version $Id: TestCsvDriver.java,v 1.9 2005/05/13 02:19:32 gupta_chetan Exp $
 */
public class TestCsvDriver extends TestCase
{
  public static final String SAMPLE_FILES_LOCATION_PROPERTY="sample.files.location";
  private String filePath;

  public TestCsvDriver(String name)
  {
    super(name);
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

  public void testWithDefaultValues()
  {
    try
    {
      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath );

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("Q123"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("\"S,\""));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("F"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("A123"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Jonathan Ackerman"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("A"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("B234"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Grady O'Neil"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("B"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("C456"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Susan, Peter and Dave"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("C"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("D789"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Amelia \"meals\" Maurice"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("E"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("X234"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Peter \"peg leg\", Jimmy & Samantha \"Sam\""));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("G"));

      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }

  public void testWithProperties()
  {
    try
    {
      Properties props = new Properties();
      props.put("fileExtension",".txt");
      props.put("separator",";");

      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath,props);

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("Q123"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("\"S;\""));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("F"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("A123"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Jonathan Ackerman"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("A"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("B234"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Grady O'Neil"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("B"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("C456"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Susan; Peter and Dave"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("C"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("D789"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Amelia \"meals\" Maurice"));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("E"));

      results.next();
      assertTrue("Incorrect ID Value",results.getString("ID").equals("X234"));
      assertTrue("Incorrect NAME Value",results.getString("NAME").equals("Peter \"peg leg\"; Jimmy & Samantha \"Sam\""));
      assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("G"));

      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }

  public void testMetadata()
  {
   try
    {
      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT * FROM sample3");

      ResultSetMetaData metadata = results.getMetaData();

      assertTrue("Incorrect Table Name",metadata.getTableName(0).equals("sample3"));

      assertTrue("Incorrect Column Name 1",metadata.getColumnName(1).equals("column 1"));
      assertTrue("Incorrect Column Name 2",metadata.getColumnName(2).equals("column \"2\" two"));
      assertTrue("Incorrect Column Name 3",metadata.getColumnName(3).equals("Column 3"));     
			assertTrue("Incorrect Column Name 4",metadata.getColumnName(4).equals("CoLuMn4"));
			assertTrue("Incorrect Column Name 5",metadata.getColumnName(5).equals("COLumn5 "));

      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }

  public void testMetadataWithSupressedHeaders()
  {
    try
    {
      Properties props = new Properties();
      props.put("suppressHeaders","true");

      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath,props);

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT * FROM sample");

      ResultSetMetaData metadata = results.getMetaData();

      assertTrue("Incorrect Table Name",metadata.getTableName(0).equals("sample"));

      assertTrue("Incorrect Column Name 1",metadata.getColumnName(1).equals("COLUMN1"));
      assertTrue("Incorrect Column Name 2",metadata.getColumnName(2).equals("COLUMN2"));
      assertTrue("Incorrect Column Name 3",metadata.getColumnName(3).equals("COLUMN3"));

      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }


  public void testWithSuppressedHeaders()
  {
    try
    {
      Properties props = new Properties();
      props.put("suppressHeaders","true");

      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath,props );

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT * FROM sample");

      // header is now treated as normal data line
      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("ID"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("NAME"));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("EXTRA_FIELD"));

      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("Q123"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("\"S,\""));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("F"));

      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("A123"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("Jonathan Ackerman"));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("A"));

      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("B234"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("Grady O'Neil"));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("B"));

      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("C456"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("Susan, Peter and Dave"));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("C"));

      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("D789"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("Amelia \"meals\" Maurice"));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("E"));

      results.next();
      assertTrue("Incorrect COLUMN1 Value",results.getString("COLUMN1").equals("X234"));
      assertTrue("Incorrect COLUMN2 Value",results.getString("COLUMN2").equals("Peter \"peg leg\", Jimmy & Samantha \"Sam\""));
      assertTrue("Incorrect COLUMN3 Value",results.getString("COLUMN3").equals("G"));

      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      fail("Unexpected Exception: " + e);
    }
  }
  
	  public void testRelativePath()
		{
			try
			{
				// break up file path to test relative paths
				String parentPath = new File(filePath).getParent();
				String subPath = new File(filePath).getName();
				
				Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + parentPath );

				Statement stmt = conn.createStatement();

				ResultSet results = stmt.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM ." + 
				                                      File.separator + subPath + 
				                                      File.separator + "sample");

				results.next();
				assertTrue("Incorrect ID Value",results.getString("ID").equals("Q123"));
				assertTrue("Incorrect NAME Value",results.getString("NAME").equals("\"S,\""));
				assertTrue("Incorrect EXTRA_FIELD Value",results.getString("EXTRA_FIELD").equals("F"));
				
				results.close();
				stmt.close();
				conn.close();
			}
			catch(Exception e)
			{
				fail("Unexpected Exception: " + e);
			}
		}
		
	  /**
	   * This creates several sentectes with were and tests they work
	   */
	  public void testWhereSimple() {
	  	try {
	  	  Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
	
	      Statement stmt = conn.createStatement();
	
	      ResultSet results = stmt.executeQuery("SELECT ID,Name FROM sample4 WHERE ID=02");
	      assertTrue(results.next());
	      assertEquals("The name is wrong","Mauricio Hernandez",results.getString("Name"));
	      assertEquals("The job is wrong","Project Manager",results.getString("Job"));
	      assertTrue(!results.next());   
	  	}
	    catch(Exception e)
	    {
	      fail("Unexpected Exception: " + e);
	    }
	  }
	  
	  public void testWhereMultipleResult() {
	  	try {
	  	  Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
	
	      Statement stmt = conn.createStatement();
	
	      
	      ResultSet results = stmt.executeQuery("SELECT ID, Name, Job FROM sample4 WHERE Job = Project Manager");
	      assertTrue(results.next());
	      assertEquals("The ID is wrong","01",results.getString("ID"));
	      assertTrue(results.next());
	      assertEquals("The ID is wrong","02",results.getString("ID"));
	      assertTrue(results.next());
	      assertEquals("The ID is wrong","04",results.getString("ID"));
	      assertTrue(!results.next());
	  	}
	    catch(Exception e)
	    {
	      fail("Unexpected Exception: " + e);
	    }
	  }

	  /**
	   * This returns no results with where and tests if this still works
	   */
	  public void testWhereNoResults() {
	  	try {
	  	  Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);
	      Statement stmt = conn.createStatement();
	      ResultSet results = stmt.executeQuery("SELECT ID,Name FROM sample4 WHERE ID=05");
	      assertFalse(results.next());   
	  	}
	    catch(Exception e)
	    {
	      fail("Unexpected Exception: " + e);
	    }
	  }
	  
}