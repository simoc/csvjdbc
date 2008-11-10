/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.sql.*;
import java.util.Properties;
import java.io.File;

/**
 * This class implements the Driver interface for the CsvJdbc driver.
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @author     JD Evora
 * @author     Tomasz Skutnik
 * @author     Christoph Langer
 * @version    $Id: CsvDriver.java,v 1.10 2008/11/10 13:41:19 mfrasca Exp $
 */

public class CsvDriver implements Driver
{

  public static final String DEFAULT_EXTENSION = ".csv";
  public static final char DEFAULT_SEPARATOR = ',';
  public static final char DEFAULT_QUOTECHAR ='"';
  public static final String DEFAULT_HEADERLINE = null;
  public static final boolean DEFAULT_SUPPRESS = false;
  public static final boolean DEFAULT_TRIM_HEADERS = true;
  public static final String DEFAULT_COLUMN_TYPES = "String";

  public static final String FILE_EXTENSION="fileExtension";
  public static final String SEPARATOR="separator";
  public static final String QUOTECHAR="quotechar";
  public static final String HEADERLINE="headerline";
  public static final String SUPPRESS_HEADERS="suppressHeaders";
  public static final String TRIM_HEADERS="trimHeaders";
  public static final String CHARSET = "charset";
  public static final String COLUMN_TYPES = "columnTypes";
  private final static String URL_PREFIX = "jdbc:relique:csv:";
  /**
   *Gets the propertyInfo attribute of the CsvDriver object
   *
   * @param  url               Description of Parameter
   * @param  info              Description of Parameter
   * @return                   The propertyInfo value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
       throws SQLException
  {
    return new DriverPropertyInfo[0];
  }


  /**
   *Gets the majorVersion attribute of the CsvDriver object
   *
   * @return    The majorVersion value
   * @since
   */
  public int getMajorVersion()
  {
    return 1;
  }


  /**
   *Gets the minorVersion attribute of the CsvDriver object
   *
   * @return    The minorVersion value
   * @since
   */
  public int getMinorVersion()
  {
    return 0;
  }


  /**
   *Description of the Method
   *
   * @param  url               Description of Parameter
   * @param  info              Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public Connection connect(String url, Properties info) throws SQLException
  {
    DriverManager.println("CsvJdbc - CsvDriver:connect() - url=" + url);
    // check for correct url
    if (!url.startsWith(URL_PREFIX))
    {
      return null;
    }
    // get filepath from url
    String filePath = url.substring(URL_PREFIX.length());
    if (!filePath.endsWith(File.separator))
    {
      filePath += File.separator;
    }

    DriverManager.println("CsvJdbc - CsvDriver:connect() - filePath=" + filePath);

    // check if filepath is a correct path.
    File checkPath = new File(filePath);
    if (!checkPath.exists())
    {
      throw new SQLException("Specified path '" + filePath + "' not found !");
    }
    if (!checkPath.isDirectory())
    {
      throw new SQLException(
          "Specified path '" + filePath + "' is  not a directory !");
    }

    return new CsvConnection(filePath, info);
  }


  /**
   *Description of the Method
   *
   * @param  url               Description of Parameter
   * @return                   Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean acceptsURL(String url) throws SQLException
  {
    DriverManager.println("CsvJdbc - CsvDriver:accept() - url=" + url);
    return url.startsWith(URL_PREFIX);
  }


  /**
   *Description of the Method
   *
   * @return    Description of the Returned Value
   * @since
   */
  public boolean jdbcCompliant()
  {
    return false;
  }
  // This static block inits the driver when the class is loaded by the JVM.
  static
  {
    try
    {
      java.sql.DriverManager.registerDriver(new CsvDriver());
    }
    catch (SQLException e)
    {
      throw new RuntimeException(
          "FATAL ERROR: Could not initialise CSV driver ! Message was: "
           + e.getMessage());
    }
  }
}

