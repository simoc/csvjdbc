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

/**This class is used to test the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @version $Id: TestCsvDriver.java,v 1.1 2001/01/23 09:21:28 jackerm Exp $
 */
public class TestCsvDriver
{
  public static void main(String[] args)
  {
    try
    {
      DriverManager.setLogWriter( new PrintWriter(System.out));

      Class.forName("org.relique.jdbc.csv.CsvDriver");


      Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + args[0] );

      Statement stmt = conn.createStatement();

      ResultSet results = stmt.executeQuery("SELECT NAME,ID FROM sample");

      while (results.next())
      {
        System.out.println("ID= " + results.getString("ID") + "   NAME= " + results.getString("NAME"));
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
}