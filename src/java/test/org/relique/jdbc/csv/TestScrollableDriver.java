package test.org.relique.jdbc.csv;

import java.sql.*;
import junit.framework.TestCase;

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

  public void testScroll()
  {
    try
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
    catch(Exception e)
    {
      fail("Unexpected Exception:" + e);
    }
  }
  
  public void testScrollWithMultiLineText()
  {
    try
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
    catch(Exception e)
    {
      fail("Unexpected Exception:" + e);
    }
  }
}
