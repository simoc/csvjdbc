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
package org.relique.jdbc.csv;

import java.io.*;
import java.util.*;

/**This class is a helper class that handles the reading and parsing of data
 * from a .csv file.
 *
 * @author Jonathan Ackerman
 * @version $Id: CsvReader.java,v 1.2 2001/02/27 09:59:35 jackerm Exp $
 */
public class CsvReader
{
  private BufferedReader input;
  private String[] columnNames;
  private String[] columns;

  public CsvReader(String fileName) throws Exception
  {
    input = new BufferedReader(new FileReader(fileName));

    String headerLine = input.readLine();

    columnNames = parseCsvLine(headerLine.toUpperCase());
    columns = new String[columnNames.length];
  }


  // This code is ugly and slow, but it does work ! (I hope)
  //  well it didn't work the first time, maybe this will :)
  protected String[] parseCsvLine(String line) throws Exception
  {
    Vector values = new Vector();
    boolean inQuotedString=false;
    String value="";
    int currentPos =0;

    line+=",";



    while (currentPos < line.length())
    {
      char currentChar = line.charAt(currentPos);

      if (value.length() == 0 && currentChar == '"' && !inQuotedString)
      {
        currentPos++;
        inQuotedString=true;
        continue;
      }

      if (currentChar == '"')
      {
        char nextChar = line.charAt(currentPos+1);
        if (nextChar == '"')
        {
          value+=currentChar;
          currentPos++;
        }
        else
        {
          if (!inQuotedString)
            throw new Exception("Unexpected '\"' in position " + currentPos);

          if (inQuotedString && nextChar != ',')
            throw new Exception("Expecting ',' in position " + (currentPos+1));

          values.add(value);
          value="";
          inQuotedString=false;

          currentPos++;
        }
      }
      else
      {
        if (currentChar == ',')
        {
          if (inQuotedString)
          {
            value+=currentChar;
          }
          else
          {
            values.add(value);
            value="";
          }
        }
        else
        {
          value+=currentChar;
        }
      }

     currentPos++;
    }

    String[] retVal = new String[values.size()];
    values.copyInto(retVal);

    return retVal;
  }

  public String[] getColumnNames()
  {
    return columnNames;
  }

  public boolean next() throws Exception
  {
    columns = new String[columnNames.length];
    String dataLine = input.readLine();

    if (dataLine==null)
    {
      input.close();
      return false;
    }

    columns = parseCsvLine(dataLine);
    return true;
  }

  public String getColumn(int columnIndex)
  {
    return columns[columnIndex];
  }

  public String getColumn(String columnName) throws Exception
  {
    columnName = columnName.toUpperCase();

    for (int loop=0; loop < columnNames.length; loop++)
     if (columnName.equals(columnNames[loop]))
       return getColumn(loop);

    throw new Exception("Column '" +columnName + "' not found.");
  }

  public void close()
  {
    try
    {
      input.close();
    }
    catch(Exception e)
    {
    }
  }
}