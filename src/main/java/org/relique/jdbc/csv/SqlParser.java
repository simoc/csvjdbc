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
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
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
 */
public class SqlParser
{
	/**
	 * The name of the table
	 */
	private List<String> tableNames;
	private List<String> tableAliases;

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
	public List<String> getTableNames()
	{
		return tableNames;
	}

	public List<String> getTableAliases()
	{
		return tableAliases;
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
	 * Parses SQL statement.
	 * @param sql SQL statement to parse.
	 * @throws ParseException if SQL statement cannot be parsed.
	 * @throws SQLException if SQL statement is not valid.
	 */
	public void parse(String sql) throws SQLException, ParseException
	{
		tableNames = new ArrayList<String>();
		tableAliases = new ArrayList<String>();

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

		if (parsedStatement.whereClause != null)
		{
			/*
			 * WHERE clause must be logical expression such as A > 5 and not a
			 * simple expression such as A + 5.
			 */
			if (!(parsedStatement.whereClause.content instanceof LogicalExpression &&
				parsedStatement.whereClause.content.isValid()))
			{
				throw new SQLException(CsvResources.getString("whereNotLogical"));
			}
		}

		this.whereClause = parsedStatement.whereClause;
		this.limit = parsedStatement.limit;
		this.offset = parsedStatement.offset;

		this.tableNames = new ArrayList<String>();
		this.tableAliases = new ArrayList<String>();
		for (ParsedTable parsedTable : parsedStatement.tableEntries)
		{
			if (parsedTable.isDerivedTable())
			{
				/*
				 * Queries from derived tables not supported yet.
				 */
				throw new SQLException(CsvResources.getString("derivedTableNotSupported"));
			}
			tableNames.add(parsedTable.getTableName());
			tableAliases.add(parsedTable.getTableAlias());
		}

		this.environment = new ArrayList<Object[]>();

		Iterator<ParsedExpression> it = parsedStatement.queryEntries.iterator();
		while (it.hasNext())
		{
			ParsedExpression parsedExpression = it.next();
			if (parsedExpression != null)
			{
				QueryEnvEntry cc = (QueryEnvEntry) parsedExpression.content;

				/*
				 * A logical expression such as A > 5 is not allowed as a query expression.
				 */
				if ((cc.getExpression() instanceof LogicalExpression) ||
					(cc.getExpression().isValid() == false))
				{
					throw new SQLException("invalidQueryExpression");
				}

				String key = cc.getKey();
				for (int i = 0; i < tableNames.size(); i++)
				{
					if (tableAliases.get(i) != null && key.startsWith(tableAliases.get(i) + "."))
					{
						key = key.substring(tableAliases.get(i).length() + 1);
						break;
					}
					if (tableNames.get(i) != null && key.startsWith(tableNames.get(i).toUpperCase() + "."))
					{
						key = key.substring(tableNames.get(i).length() + 1);
						break;
					}
				}
				environment.add(new Object[]{ key, cc.getExpression() });
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

		if (parsedStatement.havingClause != null)
		{
			if (!(parsedStatement.havingClause.content instanceof LogicalExpression) &&
				parsedStatement.havingClause.content.isValid())
			{
				throw new SQLException(CsvResources.getString("havingNotLogical"));
			}
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
