package test.org.relique.jdbc.csv;

import junit.framework.*;
import java.io.*;
import java.sql.*;

public class RunTests
{
  public static Test suite()
  {
    TestSuite suite= new TestSuite();
    suite.addTestSuite(TestSqlParser.class);
    suite.addTestSuite(TestCsvDriver.class);
    return suite;
  }

  public static void main(String[] args)
  {
    // set the file location as a property so that it is easy to pass around
    System.setProperty(TestCsvDriver.SAMPLE_FILES_LOCATION_PROPERTY,args[0]);

    // get the driver manager to log to sysout
    //DriverManager.setLogWriter( new PrintWriter(System.out));

    // kick off the tests. Note call main() instead of run() so that error codes
    // are returned so that they can be trapped by ant
    junit.textui.TestRunner.main(new String[]{"test.org.relique.jdbc.csv.RunTests"});
  }
}