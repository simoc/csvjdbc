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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SQL parser using JavaCC syntax definition file where.jj.
 * 
 * @author Jonathan Ackerman
 * @author Juan Pablo Morales
 * @author Mario Frasca
 * @created 25 November 2001
 */
public class SqlParser
{
	/**
	 * The name of the table
	 */
	private String tableName;
	private String tableAlias;

	/**
	 * Description of the Field
	 */
	private ParsedExpression whereClause;
	private List<Object[]> environment;
	private List<Expression> groupByColumns;
	private ParsedExpression havingClause;
	private List<Object[]> orderByColumns;

	private int limit;
	private int offset;

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
	 * Gets the tableName attribute of the SqlParser object
	 * 
	 * @return The tableName value
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
	 * Gets the columnNames attribute of the SqlParser object
	 * 
	 * @return The columnNames value
	 */
	public List<Object[]> getColumns()
	{
		return environment;
	}

	/**
	 * Parses SQL statement
	 */
	public void parse(String sql) throws SQLException, ParseException
	{
		tableName = null;
		tableAlias = null;

		// Ensure last line of SQL statement ends with newline so we can
		// correctly skip single-line comments.
		sql = sql + "\n";


		// parse the SQL statement
		ExpressionParser cs2 = new ExpressionParser(new StringReader(sql));
		ParsedStatement parsedStatement = cs2.parseSingleStatement();
		setParsedStatement(parsedStatement);
	}

	public void setParsedStatement(ParsedStatement parsedStatement) throws SQLException
	{
		this.isDistinct = parsedStatement.isDistinct;
		this.tableName = parsedStatement.tableName;
		this.tableAlias = parsedStatement.tableAlias;
		this.whereClause = parsedStatement.whereClause;
		this.limit = parsedStatement.limit;
		this.offset = parsedStatement.offset;

		this.environment = new ArrayList<Object[]>();

		Iterator<ParsedExpression> it = parsedStatement.queryEntries.iterator();
		while (it.hasNext())
		{
			ParsedExpression parsedExpression = it.next();
			if (parsedExpression != null)
			{
				QueryEnvEntry cc = (QueryEnvEntry) parsedExpression.content;
				String key = cc.key;
				if (tableAlias != null && key.startsWith(tableAlias + "."))
					key = key.substring(tableAlias.length() + 1);
				environment.add(new Object[]{ key, cc.expression });
			}
		}

		if (environment.isEmpty())
			throw new SQLException(CsvResources.getString("noColumnsSelected"));

		Iterator<ParsedExpression> it2 = parsedStatement.groupByEntries.iterator();
		if (it2.hasNext())
			groupByColumns = new ArrayList<Expression>();
		while (it2.hasNext())
		{
			ParsedExpression cc = it2.next();
			groupByColumns.add(cc.content);
		}
		this.havingClause = parsedStatement.havingClause;

		Iterator<ParsedExpression> it3 = parsedStatement.orderByEntries.iterator();
		if (it3.hasNext())
			orderByColumns = new ArrayList<Object[]>();
		while (it3.hasNext())
		{
			ParsedExpression cc = it3.next();
			OrderByEntry entry = (OrderByEntry) cc.content;
			int direction = entry.order.equalsIgnoreCase("ASC") ? 1 : -1;
			orderByColumns.add(new Object[]{Integer.valueOf(direction), entry.expression});
		}
	}

	public String[] getColumnNames()
	{
		String[] result = new String[environment.size()];
		for (int i = 0; i < environment.size(); i++)
		{
			Object[] entry = environment.get(i);
			result[i] = (String) entry[0];
		}
		return result;
	}

	public LogicalExpression getWhereClause()
	{
		return whereClause;
	}

	public List<Expression> getGroupByColumns()
	{
		return groupByColumns;
	}

	public LogicalExpression getHavingClause()
	{
		return havingClause;
	}

	public List<Object[]> getOrderByColumns()
	{
		return orderByColumns;
	}

	public int getLimit()
	{
		return limit;
	}

	public int getOffset()
	{
		return offset;
	}

	public String getAlias(int i)
	{
		Object[] o = environment.get(i);
		return (String) o[0];
	}

	public Expression getExpression(int i)
	{
		Object[] o = environment.get(i);
		return (Expression) o[1];
	}

	public boolean isDistinct()
	{
		return this.isDistinct;
	}
}
