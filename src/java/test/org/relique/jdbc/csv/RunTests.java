package test.org.relique.jdbc.csv;

import junit.framework.*;
import java.io.*;
import java.sql.*;

public class RunTests
{
  public static String DEFAULT_FILEPATH="C:\\Downloads\\opensource\\sourceforge\\csvjdbc\\csvjdbc-r0-9\\src\\src\\testdata";
  
  public static Test suite()
  {
    TestSuite suite= new TestSuite();
    suite.addTestSuite(TestSqlParser.class);
    suite.addTestSuite(TestCsvDriver.class);
    suite.addTestSuite(TestScrollableDriver.class);
    return suite;
  }

  public static void main(String[] args)
  {
    String filePath = args[0];
    if (filePath == null)
    	filePath=DEFAULT_FILEPATH;
    // set the file location as a property so that it is easy to pass around
    System.setProperty(TestCsvDriver.SAMPLE_FILES_LOCATION_PROPERTY,filePath);

    // get the driver manager to log to sysout
    //DriverManager.setLogWriter( new PrintWriter(System.out));

    // kick off the tests. Note call main() instead of run() so that error codes
    // are returned so that they can be trapped by ant
    junit.textui.TestRunner.main(new String[]{"test.org.relique.jdbc.csv.RunTests"});
  }
}