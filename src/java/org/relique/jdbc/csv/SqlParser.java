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
import java.util.Iterator;
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
 * @version $Id: SqlParser.java,v 1.18 2011/10/30 16:44:08 simoc Exp $
 */
public class SqlParser
{
  /**
   * The name of the table
   *
   * @since
   */
  private String tableName;
  private String tableAlias;
  /**
   *Description of the Field
   *
   * @since
   */
  private ParsedExpression whereClause;
  private List environment;
  private List orderByColumns;

  private boolean isDistinct;

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

  public String getTableAlias()
  {
    return tableAlias;
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
   * Get last index of keyword in SQL string.
   * @param sql SQL statement.
   * @param keyword keyword being searched for.
   * @return index, or -1 if keyword not found.
   */
  private int getLastIndexOfKeyword(String sql, String keyword) {
	boolean found = false;
	char before, after;
	int index = sql.lastIndexOf(keyword);
	while ((!found) && index >= 0) {
		if (index == 0)
			before = ' ';
		else
			before = sql.charAt(index - 1);
		if (index + keyword.length() == sql.length())
			after = ' ';
		else
			after = sql.charAt(index + keyword.length());
		if (Character.isWhitespace(before) && Character.isWhitespace(after)) {
			found = true;
		} else {
			sql = sql.substring(0, index);
			index = sql.lastIndexOf(keyword);
		}
	}
    return index;
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
	  tableAlias = null;

	  sql = sql.trim();
	  String upperSql = sql.toUpperCase();

	  int orderByPos = getLastIndexOfKeyword(upperSql, "ORDER BY");

	  // if we have a "ORDER BY" parse the list of column names.
	  if (orderByPos >= 0) {
		  orderByColumns = new ArrayList();
		  String orderBy = sql.substring(orderByPos + 8).trim();
		  StringTokenizer tokenizer = new StringTokenizer(orderBy, ",");
		  while (tokenizer.hasMoreTokens()) {
			  String token = tokenizer.nextToken().trim();
			  ExpressionParser cs = new ExpressionParser(new StringReader(token));
			  cs.parseOrderByEntry();
			  if (cs.content != null) {
				  OrderByEntry cc = (OrderByEntry)cs.content.content;
				  int direction = cc.order.equalsIgnoreCase("ASC") ? 1 : -1;
				  orderByColumns.add(new Object[]{Integer.valueOf(direction), cc.expression});
			  }
		  }
		  if (orderByColumns.isEmpty())
			  throw new Exception("Malformed SQL. Missing ORDER BY columns");
	  }

	  environment = new ArrayList();

	  // parse the column specifications
	  String truncated = sql;
	  if (orderByPos >= 0)
		  truncated = sql.substring(0, orderByPos);

	  ExpressionParser cs2 = new ExpressionParser(new StringReader(truncated));
	  cs2.parseSelectStatement();
	  
	  this.isDistinct = cs2.isDistinct;
	  this.tableName = cs2.tableName;
	  this.tableAlias = cs2.tableAlias;
	  this.whereClause = cs2.content;

	  Iterator it = cs2.queryEntries.iterator();
	  while (it.hasNext()) {
		  ParsedExpression parsedExpression = (ParsedExpression)it.next();
		  if (parsedExpression != null) {
			QueryEnvEntry cc = (QueryEnvEntry)parsedExpression.content;
			String key = cc.key;
			if (tableAlias != null && key.startsWith(tableAlias + "."))
				key = key.substring(tableAlias.length() + 1);
			environment.add(new Object[]{key, cc.expression});
		  }
	  }

	  if (environment.isEmpty())
		  throw new Exception("Malformed SQL. No columns");
  }

  public String[] getColumnNames() {
	  String[] result = new String[environment.size()];
	  for (int i=0; i<environment.size(); i++){
		  Object[] entry = (Object[]) environment.get(i);
		  result[i] = (String) entry[0];
	  }
	  return result;
  }

  public Expression getWhereClause() {
	  return whereClause;
  }

  public List getOrderByColumns() {
		return orderByColumns;
  }

  public String getAlias(int i) {
	  Object[] o = (Object[]) environment.get(i);
	  return (String) o[0];
  }

  public Expression getExpression(int i) {
	  Object[] o = (Object[]) environment.get(i);
	  return (Expression) o[1];
  }

  public boolean isDistinct() {
	  return this.isDistinct;
  }
}
