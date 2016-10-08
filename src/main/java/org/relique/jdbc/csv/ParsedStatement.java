/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2008  Mario Frasca
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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

class ParsedStatement
{
	List<ParsedExpression> queryEntries;
	boolean isDistinct;
	List<ParsedTable> tableEntries;
	ParsedExpression whereClause;
	List<ParsedExpression> groupByEntries;
	ParsedExpression havingClause;
	List<ParsedExpression> orderByEntries;
	int limit, offset;

	public ParsedStatement(List<ParsedExpression> queryEntries, boolean isDistinct,
		List<ParsedTable> tableEntries,
		ParsedExpression whereClause,
		List<ParsedExpression> groupByEntries,
		ParsedExpression havingClause,
		List<ParsedExpression> orderByEntries,
		int limit, int offset)
	{
		this.queryEntries = queryEntries;
		this.isDistinct = isDistinct;
		this.tableEntries = tableEntries;
		this.whereClause = whereClause;
		this.groupByEntries = groupByEntries;
		this.havingClause = havingClause;
		this.orderByEntries = orderByEntries;
		this.limit = limit;
		this.offset = offset;
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT");
		if (isDistinct)
			sb.append(" DISTINCT");
		String separator = " ";
		for (ParsedExpression expr : queryEntries)
		{
			sb.append(separator);
			separator = ", ";
			QueryEnvEntry queryEnvEntry = (QueryEnvEntry)(expr.content);
			sb.append(queryEnvEntry.getExpression().toString());
		}
		if (tableEntries != null)
		{
			int tableCounter = 0;
			for (ParsedTable parsedTable : tableEntries)
			{
				if (tableCounter == 0)
				{
					sb.append(" FROM");
				}
				else
				{
					JoinType joinType = parsedTable.getJoinType();
					if (joinType == JoinType.CROSS)
						sb.append(" CROSS JOIN");
					else if (joinType == JoinType.INNER)
						sb.append(" INNER JOIN");
					else if (joinType == JoinType.LEFT_OUTER)
						sb.append(" LEFT OUTER JOIN");
					else if (joinType == JoinType.RIGHT_OUTER)
						sb.append(" RIGHT OUTER JOIN");
					else if (joinType == JoinType.FULL_OUTER)
						sb.append(" FULL OUTER JOIN");
				}
				sb.append(" ");
				if (parsedTable.isDerivedTable())
				{
					sb.append("(");
					sb.append(parsedTable.getDerivedTableStatement().toString());
					sb.append(")");
				}
				else
				{
					sb.append(parsedTable.getTableName());
				}
				String tableAlias = parsedTable.getTableAlias();
				if (tableAlias != null)
				{
					sb.append(" ");
					sb.append(tableAlias);
				}

				if (tableCounter > 0)
				{
					if (parsedTable.getJoinType() != JoinType.CROSS)
					{
						sb.append(" ON ");
						sb.append(parsedTable.getJoinClause().toString());
					}
				}
				tableCounter++;
			}

			if (whereClause != null)
				sb.append(" WHERE ").append(whereClause.toString());

			if (groupByEntries != null && groupByEntries.size() > 0)
			{
				sb.append(" GROUP BY");
				separator = " ";
				for (ParsedExpression expr : groupByEntries)
				{
					sb.append(separator);
					separator = ", ";
					sb.append(expr.toString());
				}
				if (havingClause != null)
				{
					sb.append(" HAVING ");
					sb.append(havingClause.toString());
				}
			}

			if (orderByEntries != null && orderByEntries.size() > 0)
			{
				sb.append(" ORDER BY");
				separator = " ";
				for (ParsedExpression expr : orderByEntries)
				{
					sb.append(separator);
					separator = ", ";
					sb.append(expr.toString());
				}
			}
		}

		if (limit >= 0)
		{
			sb.append(" LIMIT ").append(limit);
		}
		if (offset > 0)
		{
			sb.append(" OFFSET ").append(offset);
		}
		return sb.toString();
	}

	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		if (queryEntries != null)
		{
			for (ParsedExpression parsedExpr : queryEntries)
			{
				QueryEnvEntry queryEnvEntry = (QueryEnvEntry)(parsedExpr.content);
				List<String> columns = queryEnvEntry.getExpression().usedColumns(availableColumns);
				result.addAll(columns);
			}
		}

		if (whereClause != null)
			result.addAll(whereClause.usedColumns(availableColumns));

		if (groupByEntries != null)
		{
			for (ParsedExpression groupByExpr : groupByEntries)
			{
				result.addAll(groupByExpr.usedColumns(availableColumns));
			}
		}

		if (havingClause != null)
			result.addAll(havingClause.usedColumns(availableColumns));

		if (orderByEntries != null)
		{
			for (ParsedExpression orderByExpr : orderByEntries)
			{
				result.addAll(orderByExpr.usedColumns(availableColumns));
			}
		}

		return result;
	}

	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		if (queryEntries != null)
		{
			for (ParsedExpression parsedExpr : queryEntries)
			{
				QueryEnvEntry queryEnvEntry = (QueryEnvEntry)(parsedExpr.content);
				List<AggregateFunction> functions = queryEnvEntry.getExpression().aggregateFunctions();
				result.addAll(functions);
			}
		}

		if (whereClause != null)
			result.addAll(whereClause.aggregateFunctions());

		if (groupByEntries != null)
		{
			for (ParsedExpression groupByExpr : groupByEntries)
			{
				result.addAll(groupByExpr.aggregateFunctions());
			}
		}

		if (havingClause != null)
			result.addAll(havingClause.aggregateFunctions());

		if (orderByEntries != null)
		{
			for (ParsedExpression orderByExpr : orderByEntries)
			{
				result.addAll(orderByExpr.aggregateFunctions());
			}
		}

		return result;
	}
}