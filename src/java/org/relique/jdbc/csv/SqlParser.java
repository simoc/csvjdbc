/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
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

import java.util.Vector;
import java.util.StringTokenizer;

/**
 *This is a very crude and SQL simple parser used by the Csv JDBC driver. It
 * only handles SELECT statements in the format "SELECT xxx,yyy,zzz FROM fffff WHERE xxx='123'"
 * The WHERE condition can only be a signle equal condition, on the form 
 * COLUMN=VALUE where column is a column from the resultset and value is a quota enclosed expression  
 * @author     Jonathan Ackerman
 * @author     Juan Pablo Morales
 * @created    25 November 2001
 * @version    $Id: SqlParser.java,v 1.3 2004/08/09 21:56:55 jackerm Exp $
 */
public class SqlParser
{
  /**
   * The name of the table
   *
   * @since
   */
  public String tableName;
  /**
   *Description of the Field
   *
   * @since
   */
  public String[] columnNames;
  /**
   * The index of the column that will be used for the where clause.
   */
  private int whereColumn;
  /**
   * The value that is sought on the where clause 
   */
  private String whereValue;
  /**
   *Gets the tableName attribute of the SqlParser object
   *
   * @return    The tableName value
   * @since
   */
  public String getTableName()
  {
    return tableName;
  }


  /**
   *Gets the columnNames attribute of the SqlParser object
   *
   * @return    The columnNames value
   * @since
   */
  public String[] getColumnNames()
  {
    return columnNames;
  }
  
  /**
   * Return the number of the column that is being used on the where clause
   * @return The zero based number of the column that is used on the where clause, -1 if there is no where clause 
   */
  public int getWhereColumn() {
  	return whereColumn;
  }
  /**
   * Return the value to use on the where column.
   * @return null if there is no where clause 
   */
  public String getWhereValue() {
  	return whereValue;
  }
  /**
   * Set the internal table name and column names.
   *
   * @param  sql            Description of Parameter
   * @exception  Exception  Description of Exception
   * @since
   */
  public void parse(String sql) throws Exception
  {
    tableName = null;
    columnNames = new String[0];

    String upperSql = sql.toUpperCase();

    if (!upperSql.startsWith("SELECT "))
    {
      throw new Exception("Malformed SQL. Missing SELECT statement.");
    }

    if (upperSql.lastIndexOf(" FROM ") == -1)
    {
      throw new Exception("Malformed SQL. Missing FROM statement.");
    }

    int fromPos = upperSql.lastIndexOf(" FROM ");
    
    int wherePos = upperSql.lastIndexOf(" WHERE ");
    /**
     * If we have a where clause then the table name is everything that sits between 
     * FROM and WHERE. If we don't then it's everything from the "FROM" up to the end
     * of the sentence
     */ 
    if(wherePos==-1) {
    	tableName = sql.substring(fromPos + 6).trim();
    } else {
    	tableName = sql.substring(fromPos + 6,wherePos).trim();
    }
    /* If we have a where clause fill the whereColumn and whereValue attributes
     */
    String whereColumnName = null;
    if(wherePos > -1) {
      int equalsPos = upperSql.lastIndexOf("=");
      if(equalsPos == -1) {
      	throw new Exception("Malformed SQL. No = sign on the WHERE clause.");
      }
      whereColumnName = upperSql.substring(wherePos+ 6,equalsPos).trim();
      whereValue = sql.substring(equalsPos + 1).trim();
      // If we have enclosing quotes take them away
      if(whereValue.startsWith("'")) {
      	whereValue = whereValue.substring(1,whereValue.length()-1);
      }
    } else {
    	whereValue = null;
    	whereColumn = -1;
    }
    Vector cols = new Vector();
    StringTokenizer tokenizer = new StringTokenizer(upperSql.substring(7, fromPos), ",");

    while (tokenizer.hasMoreTokens())
    {
      String currentColumn = tokenizer.nextToken().trim();      
      cols.add(currentColumn);
      //If the column's name is the same as the where column then put it      
      if(currentColumn.equals(whereColumnName)) {
      	whereColumn = cols.size()-1;
      }      
    }

    columnNames = new String[cols.size()];
    cols.copyInto(columnNames);

  }
}

