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

import java.io.*;
import java.util.*;

/**
 * This class is a helper class that handles the reading and parsing of data
 * from a .csv file.
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @created    25 November 2001
 * @version    $Id: CsvReader.java,v 1.3 2001/12/01 22:35:13 jackerm Exp $
 */

public class CsvReader
{
  private BufferedReader input;
  private String[] columnNames;
  private String[] columns;
  private java.lang.String buf = null;
  private char seperator = ',';
  private boolean suppressHeaders = false;


  /**
   *Constructor for the CsvReader object
   *
   * @param  fileName       Description of Parameter
   * @exception  Exception  Description of Exception
   * @since
   */
  public CsvReader(String fileName) throws Exception
  {
    this(fileName, ',', false);
  }


  /**
   * Insert the method's description here.
   *
   * Creation date: (6-11-2001 15:02:42)
   *
   * @param  fileName                 java.lang.String
   * @param  seperator                char
   * @param  suppressHeaders          boolean
   * @exception  java.lang.Exception  The exception description.
   * @since
   */
  public CsvReader(String fileName, char seperator, boolean suppressHeaders)
       throws java.lang.Exception
  {
    this.seperator = seperator;
    this.suppressHeaders = suppressHeaders;

    input = new BufferedReader(new FileReader(fileName));
    if (this.suppressHeaders)
    {
      // No column names available. Read first data line and determine number of colums.
      buf = input.readLine();
      String[] data = parseCsvLine(buf);
      columnNames = new String[data.length];
      for (int i = 0; i < data.length; i++)
      {
        columnNames[i] = "COLUMN" + String.valueOf(i);
      }
      data = null;
      // throw away.
    }
    else
    {
      String headerLine = input.readLine();
      columnNames = parseCsvLine(headerLine.toUpperCase());
    }
  }


  /**
   *Gets the columnNames attribute of the CsvReader object
   *
   * @return    The columnNames value
   * @since
   */
  public String[] getColumnNames()
  {
    return columnNames;
  }


  /**
   * Get the value of the column at the specified index.
   *
   * @param  columnIndex  Description of Parameter
   * @return              The column value
   * @since
   */

  public String getColumn(int columnIndex)
  {
    return columns[columnIndex];
  }


  /**
   * Get value from column at specified name.
   * If the column name is not found, throw an error.
   *
   * @param  columnName     Description of Parameter
   * @return                The column value
   * @exception  Exception  Description of Exception
   * @since
   */

  public String getColumn(String columnName) throws Exception
  {
    columnName = columnName.toUpperCase();
    for (int loop = 0; loop < columnNames.length; loop++)
    {
      if (columnName.equals(columnNames[loop]))
      {
        return getColumn(loop);
      }
    }
    throw new Exception("Column '" + columnName + "' not found.");
  }


  /**
   *Description of the Method
   *
   * @return                Description of the Returned Value
   * @exception  Exception  Description of Exception
   * @since
   */
  public boolean next() throws Exception
  {
    columns = new String[columnNames.length];
    String dataLine;
    if (suppressHeaders && (buf != null))
    {
      // The buffer is not empty yet, so use this first.
      dataLine = buf;
      buf = null;
    }
    else
    {
      // read new line of data from input.
      dataLine = input.readLine();
    }
    if (dataLine == null)
    {
      input.close();
      return false;
    }
    columns = parseCsvLine(dataLine);
    return true;
  }


  /**
   *Description of the Method
   *
   * @since
   */
  public void close()
  {
    try
    {
      input.close();
      buf = null;
    }
    catch (Exception e)
    {
    }
  }


  //  well it didn't work the first time, maybe this will :)
  /**
   *Description of the Method
   *
   * @param  line           Description of Parameter
   * @return                Description of the Returned Value
   * @exception  Exception  Description of Exception
   * @since
   */
  protected String[] parseCsvLine(String line) throws Exception
  {
    Vector values = new Vector();
    boolean inQuotedString = false;
    String value = "";
    int currentPos = 0;
    line += seperator;
    while (currentPos < line.length())
    {
      char currentChar = line.charAt(currentPos);
      if (value.length() == 0 && currentChar == '"' && !inQuotedString)
      {
        currentPos++;
        inQuotedString = true;
        continue;
      }
      if (currentChar == '"')
      {
        char nextChar = line.charAt(currentPos + 1);
        if (nextChar == '"')
        {
          value += currentChar;
          currentPos++;
        }
        else
        {
          if (!inQuotedString)
          {
            throw new Exception("Unexpected '\"' in position " + currentPos);
          }
          if (inQuotedString && nextChar != seperator)
          {
            throw new Exception("Expecting " + seperator + " in position " + (currentPos + 1));
          }
          values.add(value);
          value = "";
          inQuotedString = false;
          currentPos++;
        }
      }
      else
      {
        if (currentChar == seperator)
        {
          if (inQuotedString)
          {
            value += currentChar;
          }
          else
          {
            values.add(value);
            value = "";
          }
        }
        else
        {
          value += currentChar;
        }
      }
      currentPos++;
    }
    String[] retVal = new String[values.size()];
    values.copyInto(retVal);
    return retVal;
  }
  // This code is ugly and slow, but it does work ! (I hope)
}

