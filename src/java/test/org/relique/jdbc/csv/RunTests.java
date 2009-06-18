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

import junit.framework.*;

/**This class is used to test the CsvJdbc driver.
*
* @author Jonathan Ackerman
* @version $Id: RunTests.java,v 1.8 2009/06/18 14:14:04 mfrasca Exp $
*/
public class RunTests
{
  public static String DEFAULT_FILEPATH="/home/mario/workspace/csvjdbc/src/testdata";
  
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