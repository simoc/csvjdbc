/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2014 Simon Chenery
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SubQueryExpression extends Expression
{
	private ParsedStatement parsedStatement;

	public SubQueryExpression(ParsedStatement parsedStatement)
	{
		this.parsedStatement = parsedStatement;
	}

	public Object eval(Map<String, Object> env) throws SQLException
	{
		/*
		 * Evaluate sub-query that returns a single value.
		 */
		SubQueryEqualsRowMatcher rowMatcher = new SubQueryEqualsRowMatcher();
		evalList(env, rowMatcher);
		List<Object> rowMatcherValues = rowMatcher.getValues();
		int nRows = rowMatcherValues.size();
		if (nRows == 0)
			return null;
		if (nRows > 1)
			throw new SQLException(CsvResources.getString("subqueryOneRow"));
		return rowMatcherValues.get(0);
	}

	public boolean evalList(Map<String, Object> env, SubQueryRowMatcher rowMatcher) throws SQLException
	{
		/*
		 * Evaluate sub-query that matches against a single value or list of values for:
		 * SELECT X1, (SELECT X2 FROM ... ) FROM ...
		 * SELECT ... WHERE X1 = (SELECT X2 FROM ... )
		 * SELECT ... WHERE X1 IN (SELECT X2 FROM ... )
		 * SELECT ... WHERE EXISTS (SELECT X2 FROM ... )
		 */
		boolean matches = false;
		CsvStatement statement = null;
		ResultSet resultSet = null;

		try
		{
			/*
			 * Clear query expressions so that any aggregate functions are calculated
			 * independently each time this SQL statement is executed.
			 */
			SqlParser sqlParser = new SqlParser();
			for (ParsedExpression parsedExpr : parsedStatement.queryEntries)
			{
				parsedExpr.resetAggregateFunctions();
			}

			sqlParser.setParsedStatement(parsedStatement);

			Expression expr = new ColumnName(CsvStatement.STATEMENT_COLUMN_NAME);
			statement = (CsvStatement) expr.eval(env);

			resultSet = statement.executeParsedQuery(sqlParser, env);
			if (resultSet.getMetaData().getColumnCount() != 1)
				throw new SQLException(CsvResources.getString("subqueryOneColumn"));

			/*
			 * Go through sub-query ResultSet sequentially until we find a row
			 * that causes outer/parent SQL statement to be evaluated to true or false.
			 */
			while (matches == false && resultSet.next())
			{
				Object o = resultSet.getObject(1);
				matches = rowMatcher.matches(o);
			}
		}
		finally
		{
			if (resultSet != null)
				resultSet.close();
		}
		return matches;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		sb.append(parsedStatement.toString());
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> retval = new LinkedList<String>();
		List<String> usedColumns = parsedStatement.usedColumns(availableColumns);
		for (String column : usedColumns)
		{
			/*
			 * Only return columns from parent SQL table, not those
			 * from the sub-query table.
			 */
			if (availableColumns.contains(column))
				retval.add(column);
		}
		return retval;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		/*
		 * Aggregate functions are internal to this sub-query SQL statement,
		 * and not the parent SQL statement, so do not return them.
		 */
		return new LinkedList<AggregateFunction>();
	}
}
