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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
 * @version $Id: SqlParser.java,v 1.15 2011/10/14 13:47:14 mfrasca Exp $
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
  public String[] columns;
  private ExpressionParser whereClause;
  public List environment;
  
  public void setPlaceholdersValues(Object[] values)
  {
	  if (whereClause != null)
		  whereClause.setPlaceholdersValues(values);
  }
  
  public int getPlaceholdersCount()
  {
	  if (whereClause != null)
		  return whereClause.getPlaceholdersCount();
	  else
		  return 0;
  }
  
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
  public List getColumns()
  {
    return environment;
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
		columns = new String[0];

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

		// if we have a "WHERE" parse the expression
		if (wherePos > -1) {
			whereClause = new ExpressionParser(new StringReader(sql.substring(wherePos + 6)));
			whereClause.parseLogicalExpression();
		} else {
			whereClause = null;
		}
		StringTokenizer tokenizer = new StringTokenizer(sql.substring(7,
				fromPos), ",");

		environment = new ArrayList();

		// parse the column specifications
		while (tokenizer.hasMoreTokens()) {
			String thisToken = tokenizer.nextToken().trim();
			ExpressionParser cs = new ExpressionParser(new StringReader(thisToken));
			cs.parseQueryEnvEntry();
			if (cs.content != null) {
				QueryEnvEntry cc = (QueryEnvEntry)cs.content.content;
				environment.add(new Object[]{cc.key, cc.expression});
			}
		}
	}


public String[] getColumnNames() {
	String[] result = new String[environment.size()];
	for (int i=0; i<environment.size(); i++){
		Object[] entry = (Object[]) environment.get(i);
		result[i] = (String) entry[0];
	}
    return result;
}

public ExpressionParser getWhereClause() {
	return whereClause;
}


public String getAlias(int i) {
	Object[] o = (Object[]) environment.get(i);
	return (String) o[0];
}

public Expression getExpression(int i) {
	Object[] o = (Object[]) environment.get(i);
	return (Expression) o[1];
}

}

