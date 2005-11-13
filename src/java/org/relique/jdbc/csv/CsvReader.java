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
import java.sql.SQLException;

/**
 * This class is a helper class that handles the reading and parsing of data
 * from a .csv file.
 *
 * @author     Jonathan Ackerman
 * @author     Sander Brienen
 * @author     Stuart Mottram (fritto)
 * @author     Jason Bedell
 * @author     Tomasz Skutnik
 * @author     Christoph Langer
 * @author     Chetan Gupta
 * @created    25 November 2001
 * @version    $Id: CsvReader.java,v 1.15 2005/11/13 18:32:58 jackerm Exp $
 */

public class CsvReader extends CSVReaderAdapter
{

  /**
   *Constructor for the CsvReader object
   *
   * @param  fileName       Description of Parameter
   * @exception  Exception  Description of Exception
   * @since
   */
  public CsvReader(String fileName) throws Exception
  {
    this(fileName, ',', false, null, '"', "", CsvDriver.DEFAULT_EXTENSION, true);
  }

  /**
   * Insert the method's description here.
   *
   * Creation date: (6-11-2001 15:02:42)
   *
   * @param  fileName                 java.lang.String
   * @param  separator                char
   * @param  suppressHeaders          boolean
   * @param  quoteChar				  char
   *    * @exception  java.lang.Exception  The exception description.
   * @since
   */
  public CsvReader(String fileName, char separator, boolean suppressHeaders, String charset, char quoteChar, String headerLine, String extension, boolean trimHeaders)
       throws java.lang.Exception
  {
      super(fileName, separator, suppressHeaders, charset, quoteChar, headerLine, extension, trimHeaders);
  }
  
/**
   *Description of the Method
   *
   * @return                Description of the Returned Value
   * @exception  SQLException  Description of Exception
   * @since
   */
  public boolean next() throws SQLException {
    columns = new String[columnNames.length];
    String dataLine = null;
    try {
        if (suppressHeaders && (buf != null)) {
          // The buffer is not empty yet, so use this first.
          dataLine = buf;
          buf = null;
        } else {
          // read new line of data from input.
          dataLine = input.readLine();
        }
        if (dataLine == null) {
          input.close();
          return false;
        }
    } catch (IOException e) {
        throw new SQLException(e.toString());
    }
    columns = parseCsvLine(dataLine, false);
    return true;
  }


  /**
   *Description of the Method
   *
   * @since
   */
  public void close()
  {
    try {
      input.close();
      buf = null;
    } catch (Exception e) {}
  }
}

