## Features

CsvJdbc accepts all types of CSV files defined by
[RFC 4180](https://tools.ietf.org/html/rfc4180).

CsvJdbc accepts only SQL SELECT queries from a single table and does
not support INSERT, UPDATE, DELETE or CREATE statements.

SQL sub-queries are permitted but
joins between tables in SQL SELECT queries are not yet supported.

SQL SELECT queries must be of the following format.

    SELECT [DISTINCT] [table-alias.]column [[AS] alias], ...
      FROM table [[AS] table-alias]
      WHERE [NOT] condition [AND | OR condition] ...
      GROUP BY column ... [HAVING condition ...]
      ORDER BY column [ASC | DESC] ...
      LIMIT n [OFFSET n]

Each column is either a named column,
`*`,
a constant value,
`NULL`,
`CURRENT_DATE`,
`CURRENT_TIME`,
a sub-query,
or an expression including functions, aggregate functions,
operations `+`, `-`,
`/`, `*`, `%` (modulo), `||` (string concatenation),
conditional `CASE` expressions
and parentheses.

Supported comparisons in the optional WHERE clause are
`<`,
`>`,
`<=`,
`>=`,
`=`,
`!=`,
`<>`,
`NOT`,
`BETWEEN`,
`LIKE`,
`IS NULL`,
`IN`,
`EXISTS`.

Use double quotes around table names or column names containing spaces
or other special characters.

Function             |Description
-----------          |-------------------
ABS(N)               |Returns absolute value of N
COALESCE(N1, N2, ...)|Returns first expression that is not NULL
TO_ARRAY([DISTINCT] N1, N2, ...) | Returns java.sql.Array containing (optionally distinct) values
DAYOFMONTH(D)        |Extracts day of month from date or timestamp D (first day of month is 1)
HOUROFDAY(T)         |Extracts hour of day from time or timestamp T
LENGTH(S)            |Returns length of string
LINE_NUMBER()        |Returns line number of row in CSV file (NULL if row not corresponding to a line)
LOWER(S)             |Converts string to lower case
LTRIM(S [, T])       |Removes leading characters from S that occur in T
MINUTE(T)            |Extracts minute of hour from time or timestamp T
MONTH(D)             |Extracts month from date or timestamp D (first month is 1)
NULLIF(X, Y)         |Returns NULL if X and Y are equal, otherwise X
RANDOM()             |Returns random number in the range 0 to 1
REPLACE(S, FROM, TO) |Replaces all occurrences of string FROM in S with TO
ROUND(N [, D])       |Rounds N to the specified number of decimal places D (`0` by default)
RTRIM(S, [, T])      |Removes trailing characters from S that occur in T
SECOND(T)            |Extracts seconds value from time or timestamp T
SUBSTRING(S, N [, L])|Extracts substring from S starting at index N (counting from 1) with length L
TRIM(S, [, T])       |Removes leading and trailing characters from S that occur in T
UPPER(S)             |Converts string to lower case
YEAR(D)              |Extracts year from date or timestamp D

Additional functions are defined from java methods using the
`function.NAME` driver property.

Aggregate Function|Description
------------------|-----------
AVG(N)            |Average of all values
COUNT(N)          |Count of all values
MAX(N)            |Maximum value
MIN(N)            |Minimum value
STRING_AGG(S, D)  |All values of S concatenated with delimiter D
ARRAY_AGG(S)      |All values of S into a java.sql.Array
SUM(N)            |Sum of all values

For queries containing `ORDER BY`, all records are read into memory and sorted.
For queries containing `GROUP BY` plus an aggregate function, all records are
read into memory and grouped. For queries that produce a scrollable result set,
all records up to the furthest accessed record are held into memory. For other
queries, CsvJdbc holds only one record at a time in memory.

Notes on functions returning an array:
* `ResultSet.getArray(...)` returns an object of type `org.relique.jdbc.csv.SqlArray`, that implements interface `java.sql.Array`
* Both `java.sql.Array.getArray(...)` and `getResultSet(...)` methods are implemented
* `SqlArray` tries to infer the type of the values for its `getBaseTypeName()` and `getBaseType()`
methods; an `IllegalStateException` is thrown if values of different types are detected

## Dependencies

CsvJdbc requires Java version 8, or later. For reading DBF files,
[DANS DBF Library](http://dans-dbf-lib.sourceforge.net/)
must be downloaded and included in the CLASSPATH.

## Advanced Usage

Like other databases, creating a scrollable statement enables scrolling
forwards and backwards through result sets. This is demonstrated in the
following example.

```java
import java.sql.*;

public class DemoDriver2
{
  public static void main(String[] args) throws Exception
  {
    try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + args[0]);

      // create a scrollable Statement so we can move forwards and backwards
      // through ResultSets
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
      ResultSet results = stmt.executeQuery("SELECT ID,NAME FROM sample"))
    {

      // dump out the last record in the result set, then the first record
      if (results.last())
      {
        System.out.println("ID= " + results.getString("ID") +
              "   NAME= " + results.getString("NAME"));
        if (results.first())
        {
          System.out.println("ID= " + results.getString("ID") +
              "   NAME= " + results.getString("NAME"));
        }
      }
    }
  }
}
```

To read several files (for example, daily log files) as a single table,
set the database connection property `indexedFiles`.
The following example demonstrates how to do this.

```java
import java.sql.*;
import java.util.Properties;

public class DemoDriver3
{
  public static void main(String[] args) throws Exception
  {
    Properties props = new Properties();
    props.put("fileExtension", ".txt");
    props.put("indexedFiles", "true");
    // We want to read test-001-20081112.txt, test-002-20081113.txt and many
    // other files matching this pattern.
    props.put("fileTailPattern", "-(\\d+)-(\\d+)");
    // Make the two groups in the regular expression available as
    // additional table columns.
    props.put("fileTailParts", "Seqnr,Logdatum");
    try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" +
        args[0], props);
      Statement stmt = conn.createStatement();
      ResultSet results = stmt.executeQuery("SELECT Datum, Station, " +
        "Seqnr, Logdatum FROM test"))
    {
      ResultSetMetaData meta = results.getMetaData();
      while (results.next())
      {
        for (int i = 0; i < meta.getColumnCount(); i++)
        {
          System.out.println(meta.getColumnName(i + 1) + " " +
            results.getString(i + 1));
        }
      }
    }
  }
}
```

Set the database connection property `columnTypes` to enable expressions
containing numeric, time and date data types to be used in SELECT statements
and to enable column values to be fetched using `ResultSet.getInt`,
`getDouble`, `getTime` and other `ResultSet.get` methods.

```java
import java.sql.*;
import java.util.Properties;

public class DemoDriver4
{
  public static void main(String[] args) throws Exception
  {
    Properties props = new Properties();
    // Define column names and column data types here.
    props.put("suppressHeaders", "true");
    props.put("headerline", "ID,ANGLE,MEASUREDATE");
    props.put("columnTypes", "Int,Double,Date");
    try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:" +
        args[0], props);
      Statement stmt = conn.createStatement();
      ResultSet results = stmt.executeQuery("SELECT Id, Angle * 180 / 3.1415 as A, " +
        "MeasureDate FROM t1 where Id > 1001"))
    {
      while (results.next())
      {
        // Fetch column values with methods that match the column data types.
        System.out.println(results.getInt(1));
        System.out.println(results.getDouble(2));
        System.out.println(results.getDate(3));
      }
    }
  }
}
```

To read the compressed files inside a ZIP file as database tables, make a
database connection to the ZIP file using the JDBC connection string format
`jdbc:relique:csv:zip:filename.zip`. This is demonstrated in the following
example.

```java
import java.sql.*;

public class DemoDriver5
{
  public static void main(String[] args) throws Exception
  {
    String zipFilename = args[0];
    try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:zip:" +
        zipFilename);
      Statement stmt = conn.createStatement();
      // Read from file mytable.csv inside the ZIP file
      ResultSet results = stmt.executeQuery("SELECT * FROM mytable"))
    {
      while (results.next())
      {
          System.out.println(results.getString("COUNTRY"));
      }
    }
  }
}
```

To read the resources from classpath as database tables, make a database connection
to the classpath path using the JDBC connection string format
`jdbc:relique:csv:classpath:path/to/resources`. The additional dependency
is required to scan the paths in the classpath.

```xml
...
<dependency>
  <groupId>io.github.classgraph</groupId>
  <artifactId>classgraph</artifactId>
  <version>X.Y.Z</version>
</dependency>
...
```

The following example demonstrates reading from the classpath.

```java
import java.sql.*;

public class DemoDriver6
{
  public static void main(String[] args) throws Exception
  {
    String path = args[0];
    try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:classpath:" +
            path);
      Statement stmt = conn.createStatement();
      // Read from the classpath resource mytable.csv
      ResultSet results = stmt.executeQuery("SELECT * FROM mytable"))
      {
        while (results.next())
        {
            System.out.println(results.getString("COUNTRY"));
        }
      }
  }
}
```

To read data that is either held inside some file storage or accessed remotely
(for example, using HTTP requests), create a Java class that implements the
interface `org.relique.io.TableReader` and give this class name in the
connection URL. CsvJdbc then creates an instance of this class and calls the
`getReader` method to obtain a `java.io.Reader` for each database table being
read. This is demonstrated in the following two Java classes.

```java
import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

import org.relique.io.TableReader;

public class MyHTTPReader implements TableReader
{
  public Reader getReader(Statement statement, String tableName) throws SQLException
  {
    try
    {
      URL url = new URL("http://csvjdbc.sourceforge.net/" + tableName + ".csv");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      InputStreamReader reader = new InputStreamReader(connection.getInputStream());
      return reader;
    }
    catch (Exception e)
    {
      throw new SQLException(e.getMessage());
    }
  }
  public List getTableNames(Connection connection)
  {
    // Return list of available table names
    Vector v = new Vector();
    v.add("sample");
    return v;
  }
}


import java.sql.*;
import org.relique.jdbc.csv.CsvDriver;

public class DemoDriver7
{
  public static void main(String []args) throws Exception
  {
    String sql = "SELECT * FROM sample";
    // Give name of Java class that provides database tables.
    try (Connection conn = DriverManager.getConnection("jdbc:relique:csv:class:" +
        MyHTTPReader.class.getName());
      Statement stmt = conn.createStatement();
      ResultSet results = stmt.executeQuery(sql))
    {
      CsvDriver.writeToCsv(results, System.out, true);
    }
  }
}
```

## Driver Properties

The driver also supports a number of parameters that change the
 default behaviour of the driver.

These properties are:

### charset

+ type: String
+ default: Java default
+ Defines the character set name of the files being read, such as `UTF-16`.
See the Java
[Charset](https://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html)
documentation for a list of available character set names.

### columnTypes

+ type: String
+ default: all Strings
+ A comma-separated list defining SQL data types for columns in tables. When
column values are fetched using `getObject` (as opposed to `getString`), the
driver will parse the value and return a correctly typed object. If fewer data
types are provided than the number of columns in the table, the last data type
is repeated for all remaining columns. If `columnTypes` is set to an empty
string then column types are inferred from the data. When working with multiple
tables with different column types, define properties named `columnTypes.CATS`
and `columnTypes.DOGS` to define different column types for tables `CATS` and
`DOGS`.

### commentChar

+ type: String
+ default: `null`
+ Lines before the header that start with the comment are skipped.
After the header has been read, all lines are interpreted as data.

### cryptoFilterClassName

+ type: Class
+ default: `null`
+ The full class name of a Java class that decrypts the file being read.
The class must implement interface `org.relique.io.CryptoFilter`. The class
`org.relique.io.XORFilter` included in CsvJdbc implements an XOR encryption
filter.

### cryptoFilterParameterTypes
+ type: String
+ default: `String`
+ Comma-separated list of data types to pass to the constructor of the
decryption class set in property `cryptoFilterClassName`.

### cryptoFilterParameters
+ type: String
+ default:
+ Comma-separated list of values to pass to the constructor of the
decryption class set in property `cryptoFilterClassName`.

### defectiveHeaders
+ type: Boolean
+ default: `False`
+ When true, column names that are an emtpy string or a case insensitive
duplicate of a previous column name are renamed to COLUMNx,
where x is the ordinal identifying the column.

### fileExtension
+ type: String
+ default: `.csv`
+ Specifies file extension of the CSV files. If the extension `.dbf`
is used then files are read as dBase format database files.

### fileTailParts
+ type: String
+ default: `null`
+ Comma-separated list of column names for the additional columns
generated by regular expression groups in the property `fileTailPattern`.

### fileTailPattern
+ type: String
+ default: `null`
+ Regular expression for matching filenames when property `indexedFiles` is
True.  If the regular expression contains groups (surrounded by parentheses)
then the value of each group in matching filenames is added as an extra column
to each line read from that file. For example, when querying table `test`, the
regular expression `-(\d+)-(\d+)` will match files `test-001-20081112.csv` and
`test-002-20081113.csv`. The column values `001` and `20081112` are added to
each line read from the first file and `002` and `20081113` are added to each
line read from the second file.

### fileTailPrepend
+ type: Boolean
+ default: `False`
+ when True, columns generated by regular expression groups in the
`fileTailPattern` property are prepended to the start of each line.
When False, the generated columns are appended after the columns
read for each line.

### fixedWidths
+ type: String
+ default: `null`
+ Defines character position ranges for each column in a fixed width file. When
set, column values are extracted from these ranges in each line instead of
separating the line by delimiters. Each column is a pair of character positions
separated by a minus sign, or a single character for columns with only a single
character. The position of the first character on each line is 1. Character
position ranges are separated by commas. For example, `1,2-9,16-19`.

### function.NAME
+ type: String
+ default: None
+ Defines a java method to use as the SQL function named `NAME` in SQL
statements. The property value is a public static java given as a java package,
class and method name followed by parameter list in parentheses. For example,
property `function.POW` with value `java.lang.Math.pow(double, double)` makes
`POW` available as an SQL function. Methods with variable length argument lists
are defined by appending ... after the last parameter. Each method parameter
must be a numeric type, `String`, or `Object`.

### headerline
+ type: string
+ default: None
+ Used in combination with the `suppressHeaders` property to specify a custom
header line for tables. `headerline` contains a list of column names for tables
separated by the `separator`. When working with multiple tables with different
headers, define properties named `headerline.CATS` and `headerline.DOGS` to
define different header lines for tables `CATS` and `DOGS`.

### ignoreNonParseableLines
+ type: Boolean
+ default: `False`
+ when True, lines that have too few or too many column values
will not cause an exception but will
be ignored. Each ignored line is logged. Call method
`java.sql.DriverManager.setLogWriter` before executing a query to capture a
list of ignored lines.

### indexedFiles
+ type: Boolean
+ default: `False`
+ when True, all files with a filename matching the table name plus the regular
expression given in property `fileTailPattern` are read as if they were a single
file.

### isHeaderFixedWidth
+ type: Boolean
+ default: `True`
+ Used in combination with the `fixedWidths` property when reading fixed
width files to specify whether the header line containing the column names
is also fixed width. If False, column names are separated by the `separator`.

### missingValue
+ type: String
+ default: `null`
+ When not null, lines with too few column values will use this value for
each missing column, instead of throwing an exception.

### quotechar
+ type: Character
+ default: `"`
+ Defines quote character. Column values surrounded with the quote character
are parsed with the quote characters removed. This is useful when values
contain the `separator` or line breaks. No more than one character is allowed.
An empty value disables quoting.

### quoteStyle
+ type: String
+ default: `SQL`
+ Defines how a quote character is interpreted inside a quoted value. When
`SQL`, a pair of quote characters together is interpreted as a single quote
character.  When `C`, a backslash followed by a quote character is interpreted
as a single quote character.

### locale
+ type: String
+ default: Java default
+ Defines locale to use when parsing timestamps. This is important when parsing
words such as `December` which vary depending on the locale. Call method
`java.util.Locale.toString()` to convert a locale to a string.

### maxDataLines
+ type: Integer
+ default: `0`
+ when non-zero, defines the maximum number of lines of data to read from the
file.  Using this property together with `skipLeadingDataLines` enables a
limited number of lines to be read from the middle of a very large file.

### randomSeed
+ type: Long
+ default: None
+ Defines seed value for random number generation. Using the same seed value will repeat the same sequence of random numbers.

### separator
+ type: String
+ default: `,`
+ Defines column separator. A separator longer than one character is permitted.
The separator `\t` is interpreted as a tab character.

### skipLeadingLines
+ type: Integer
+ default: `0`
+ after opening a file, skip this many lines before starting to interpret
the contents.

### skipLeadingDataLines
+ type: Integer
+ default: `0`
+ after reading the header from a file, skip this many lines before starting
to interpret lines as records.

### suppressHeaders
+ type: boolean
+ default: `False`
+ Used to specify that the file does not contain a column header with column
names. If `True` and `headerline` is not set, then columns are named
sequentially `COLUMN1`, `COLUMN2`, ... If `False`, the column header is read
from the first line of the file.

### timestampFormat, timeFormat, dateFormat
+ type: String
+ default: `yyyy-MM-dd HH:mm:ss`, `HH:mm:ss`, `yyyy-MM-dd`
+ Defines the format from which columns of type Timestamp, Time and Date are parsed. See the Java
[SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html)
documentation for date and timestamp patterns,
or
[DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html)
when property `useDateTimeFormatter` is set to `true`.

### timeZoneName
+ type: String
+ default: `UTC`
+ The time zone of Timestamp columns. To use the time zone of the computer,
set this to the value returned by the method
`java.util.TimeZone.getDefault().getID()`.

### trimHeaders
+ type: Boolean
+ default: `True`
+ If True, leading and trailing whitespace is trimmed from each column
name in the header line. Column names inside quotes are not trimmed.

### trimValues
+ type: Boolean
+ default: `False`
+ If True, leading and trailing whitespace is trimmed from each column
value in the file. Column values inside quotes are not trimmed.

### useDateTimeFormatter
+ type: Boolean
+ default: `False`
+ If True, Java class
[DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html)
(that was new in Java 8)
is used to parse and format timestamps, times and dates instead
of the older class
[SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html).

The following example code shows how some of these properties are used.

```java
...

  Properties props = new java.util.Properties();

  props.put("separator", "|");              // separator is a bar
  props.put("suppressHeaders", "true");     // first line contains data
  props.put("fileExtension", ".txt");       // file extension is .txt
  props.put("timeZoneName", "America/Los_Angeles"); // timestamps are Los Angeles time

  Connection conn1 = Drivermanager.getConnection("jdbc:relique:csv:" + args[0], props);

  ...

  // Connections using a URL string containing both directory and
  // properties are also accepted (class java.net.URLEncoder encodes
  // property values containing special characters).
  Connection conn2 = DriverManager.getConnection("jdbc:relique:csv:" + args[0] + "?" +
    "separator=" + URLEncoder.encode("|", "UTF-8") + "&" +
    "quotechar=" + URLEncoder.encode("'", "UTF-8") + "&" +
    "fileExtension=.txt" + "&" +
    "suppressHeaders=true");
```
