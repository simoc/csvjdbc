/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.sql.*;
import org.relique.jdbc.csv.CsvDriver;

public class Test
{
  public static void main(String[] args) throws Exception
  {
    // Load the driver.
    Class.forName("org.relique.jdbc.csv.CsvDriver");

    // Create a connection. The first command line parameter is
    // the directory containing the .csv files.
    // A single connection is thread-safe for use by several threads.
    //Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + args[0]);
    Connection conn = DriverManager.getConnection("jdbc:relique:csv:c:\\tmpbox\\CSV_relique2\\data\\");

    // Create a Statement object to execute the query with.
    // A Statement is not thread-safe.
    Statement stmt = conn.createStatement();

    // Select the ID and NAME columns from sample.csv
    //ResultSet results = stmt.executeQuery("SELECT nAme as NaMe FROM csv2000 group by nAme order by namE desc");// group by Name");
    ResultSet results = stmt.executeQuery("SELECT NAME FROM csv2000 group by nAme order by namE desc");// group by Name");
    // Dump out the results to a CSV file with the same format
    // using CsvJdbc helper function
    boolean append = true;
    CsvDriver.writeToCsv(results, System.out, append);

    // Clean up
    conn.close();
  }
}
