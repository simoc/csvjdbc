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

/**This class is used to test the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @author JD Evora
 * @version $Id: TestCsvDriver.java,v 1.3 2001/12/02 00:25:05 jackerm Exp $
 */
public class TestCsvDriver
{
  public static void main(String[] args)
  {
    // check that path was passed to application
    if (args.length != 1)
    {
      System.out.println("Please specify path to sample folders !");
      System.exit(1);
    }

    // get the driver manager to log to sysout
    DriverManager.setLogWriter( new PrintWriter(System.out));

    // load CSV driver
    try
    {
      Class.forName("org.relique.jdbc.csv.CsvDriver");
    }
    catch (ClassNotFoundException e)
    {
      System.out.println("Oops, the driver is not in the CLASSPATH -> " + e);
      System.exit(1);
    }

    // do simple test
    defaultValues(args[0]);

    // do simple test with properties
    withProperties(args[0]);

    // do meta data test
    displayMetadata(args[0]);

    // test suppress headers test
    //testSupressHeaders(args[0]);
  }

  private static void defaultValues(String filePath)
  {
     System.out.println("------------------------------------------------------");
     System.out.println("TESTING THE DRIVER WITH THE DEFAULT VALUES");
     try
      {
        Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath );

        Statement stmt = conn.createStatement();

        ResultSet results = stmt.executeQuery("SELECT NAME,ID,EXTRA_FIELD FROM sample");

        while (results.next())
        {
          System.out.println("ID= " + results.getString("ID")
                             + "   NAME= " + results.getString("NAME")
                             + "   EXTRA_FIELD= " + results.getString("EXTRA_FIELD"));
        }

        results.close();
        stmt.close();
        conn.close();
      }
      catch(Exception e)
      {
        System.out.println("Oops -> " + e);
      }
  }

  private static void  withProperties(String filePath)
  {
    System.out.println("------------------------------------------------------");
    System.out.println("TESTING THE DRIVER WITH THE PARAMETERS SUPPLIED BY A PROPERTIES FILE");
  // Tests the .txt extension and the ; separator
    try
    {
      Properties pro = new Properties();
      pro.put("fileExtension",".txt");
      pro.put("separator",";");

      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath,pro);

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT ID,NAME,EXTRA_FIELD FROM sample");

      while (results.next())
      {
        System.out.println("ID= " + results.getString("ID")
                           + "   NAME= " + results.getString("NAME")
                           + "   EXTRA_FIELD= " + results.getString("EXTRA_FIELD"));
      }


      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      System.out.println("Oops -> " + e);
    }
  }

  private static void displayMetadata(String filePath)
  {
   System.out.println("------------------------------------------------------");
   System.out.println("TESTING THE METADATA");
   try
    {
      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + filePath);

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT * FROM sample");

      ResultSetMetaData rsm = results.getMetaData();

      System.out.println("The table '"+rsm.getTableName(0)+"' has the columns:");
      for(int i=0;i<rsm.getColumnCount ();i++)
      {
       System.out.println("\t"+rsm.getColumnName(i));
      }

      results.close();
      stmt.close();
      conn.close();
    }
    catch(Exception e)
    {
      System.out.println("Oops -> " + e);
      e.printStackTrace(System.out);
    }
  }


}