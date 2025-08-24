# ![CsvJdbc](CsvJdbcLogo.png)

![Java CI](https://github.com/simoc/csvjdbc/workflows/Java%20CI/badge.svg)

A Java database driver for reading comma-separated-value files.

## Introduction

CsvJdbc is a read-only JDBC driver that uses Comma Separated Value (CSV) files
or DBF files as database tables. It is ideal for writing data import programs
or analyzing log files.

The driver enables a directory or a ZIP file containing CSV or DBF files to be
accessed as though it were a database containing tables. However, as there is
no real database management system behind the scenes, not all JDBC
functionality is available.

## Usage

The CsvJdbc driver is used just like any other JDBC driver,
either in a database tool such as
[DBeaver](https://dbeaver.io/) or
[SQuirrel SQL Client](http://squirrel-sql.sourceforge.net/),
or programmatically in your own Java program using the steps below:

1. download `csvjdbc.jar` and add it to the Java CLASSPATH.
1. use `DriverManager` to connect to the database (the directory or ZIP file)
1. create a statement object
1. use the statement object to execute an SQL SELECT query
1. the result of the query is a `ResultSet`

The following example puts the above steps into practice.

```java
import java.sql.*;
import org.relique.jdbc.csv.CsvDriver;

public class DemoDriver
{
  public static void main(String[] args) throws Exception
  {
    // Create a connection to directory given as first command line
    // parameter. Driver properties are passed in URL format
    // (or alternatively in a java.utils.Properties object).
    //
    // A single connection is thread-safe for use by several threads.
    String url = "jdbc:relique:csv:" + args[0] + "?" +
      "separator=;" + "&" + "fileExtension=.txt";
    Class.forName("org.relique.jdbc.csv.CsvDriver");
    try (Connection conn = DriverManager.getConnection(url);

      // Create a Statement object to execute the query with.
      // A Statement is not thread-safe.
      Statement stmt = conn.createStatement();

      // Select the ID and NAME columns from sample.csv
      ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample"))
    {
      // Dump out the results to a CSV file with the same format
      // using CsvJdbc helper function
      boolean append = true;
      CsvDriver.writeToCsv(results, System.out, append);
    }
  }
}
```

## Documentation

Full documentation for CsvJdbc is found [here](doc.md).

## Developing

Read the instructions for [compiling](develop.md) CsvJdbc
and for [releasing](release.md) CsvJdbc.
