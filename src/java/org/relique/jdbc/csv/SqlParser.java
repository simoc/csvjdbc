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

import java.util.Map;
import java.util.Vector;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a very crude and simple SQL parser used by the Csv JDBC driver. It
 * only handles SELECT statements in the format
 * "SELECT xxx,yyy,zzz FROM fffff WHERE xxx='123'"
 * 
 * The WHERE condition can only be a single equal condition, on the form
 * COLUMN=VALUE where column is a column from the resultSet and value is a quote
 * enclosed expression
 * 
 * @author Jonathan Ackerman
 * @author Juan Pablo Morales
 * @author Mario Frasca
 * @created 25 November 2001
 * @version $Id: SqlParser.java,v 1.5 2008/11/10 13:41:19 mfrasca Exp $
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
  public Column[] columns;
  /**
   * The value that is sought on the where clause 
   */
  private String whereValue;
  /**
   * The name of the column that will be used for the where clause.
   */
  private String whereColumnName;
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
  public Column[] getColumns()
  {
    return columns;
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
  public void parse(String sql) throws Exception {
		tableName = null;
		columns = new Column[0];

		String upperSql = sql.toUpperCase();

		if (!upperSql.startsWith("SELECT ")) {
			throw new Exception("Malformed SQL. Missing SELECT statement.");
		}

		if (upperSql.lastIndexOf(" FROM ") == -1) {
			throw new Exception("Malformed SQL. Missing FROM statement.");
		}

		int fromPos = upperSql.lastIndexOf(" FROM ");

		int wherePos = upperSql.lastIndexOf(" WHERE ");
		/**
		 * If we have a where clause then the table name is everything that sits
		 * between FROM and WHERE. If we don't then it's everything from the
		 * "FROM" up to the end of the sentence
		 */
		if (wherePos == -1) {
			tableName = sql.substring(fromPos + 6).trim();
		} else {
			tableName = sql.substring(fromPos + 6, wherePos).trim();
		}
		/*
		 * If we have a where clause fill the whereColumn and whereValue
		 * attributes
		 */
		if (wherePos > -1) {
			int equalsPos = upperSql.lastIndexOf("=");
			if (equalsPos == -1) {
				throw new Exception(
						"Malformed SQL. No = sign on the WHERE clause.");
			}
			whereColumnName = upperSql.substring(wherePos + 6, equalsPos)
					.trim();
			whereValue = sql.substring(equalsPos + 1).trim();
			// If we have enclosing quotes take them away
			if (whereValue.startsWith("'")) {
				whereValue = whereValue.substring(1, whereValue.length() - 1);
			}
		} else {
			whereValue = null;
			whereColumnName = null;
		}
		Vector cols = new Vector();
		StringTokenizer tokenizer = new StringTokenizer(upperSql.substring(7,
				fromPos), ",");

		Pattern simple = Pattern.compile("[a-zA-Z0-9_]+");
		Pattern nameAlias = Pattern.compile("([a-zA-Z0-9_]+) +(?:[aA][sS] +)?([a-zA-Z0-9_]+)");
		Pattern literalAlias = Pattern.compile("('[^']*'|-?[0-9\\.]+) +(?:[aA][sS] +)?([a-zA-Z0-9_]+)");
		
		while (tokenizer.hasMoreTokens()) {
			String thisToken = tokenizer.nextToken().trim();
			/*
			 * we don't parse the token, we simply try to match it against a
			 * regular expressions describing...
			 * 
			 * name: new Column(name, -2, name);
			 * name plus alias: new Column(alias, -2, name);
			 * literal plus alias: new Column(alias, -1, literal);
			 */
			Column currentColumn = null;
			Matcher m = simple.matcher(thisToken);
			if (m.matches()){
				currentColumn = new Column(thisToken, -2, thisToken);
			}
			m = nameAlias.matcher(thisToken);
			if(m.matches()){
				currentColumn = new Column(m.group(2), -2, m.group(1));
			}
			m = literalAlias.matcher(thisToken);
			if(m.matches()){
				currentColumn = new Column(m.group(2), -1, m.group(1));
			}
			if (thisToken.equals("*")){
				currentColumn = new Column(thisToken, -2, thisToken);
			}

			cols.add(currentColumn);
		}

		columns = new Column[cols.size()];
		cols.copyInto(columns);
	}


public String[] getColumnNames() {
	String[] result = new String[columns.length];
	for (int i=0; i<columns.length; i++){
		result[i] = columns[i].getName();
	}
    return result;
}


public Map getWhereClause() {
	// TODO Auto-generated method stub
	return null;
}


public String getWhereColumnName() {
	return whereColumnName;
}
}

