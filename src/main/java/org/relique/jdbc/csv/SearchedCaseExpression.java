/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2015 Simon Chenery
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

import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SearchedCaseExpression extends Expression
{
	private List<Expression> conditions;
	private List<Expression> values;
	private Expression elseExpression;

	public SearchedCaseExpression(List<Expression> conditions,
		List<Expression> values, Expression elseExpression)
	{
		this.conditions = conditions;
		this.values = values;
		this.elseExpression = elseExpression;
	}

	public Object eval(Map<String, Object> env) throws SQLException
	{
		for (int i = 0; i < conditions.size(); i++)
		{
			Expression condition = conditions.get(i);
			if (!(condition instanceof LogicalExpression && condition.isValid()))
			{
				throw new SQLException(CsvResources.getString("caseNotLogical"));
			}
			Boolean b = ((LogicalExpression)condition).isTrue(env);
			if (b != null && b.booleanValue())
				return values.get(i).eval(env);
		}
		if (elseExpression != null)
			return elseExpression.eval(env);

		return null;
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder("CASE");
		for (int i = 0; i < conditions.size(); i++)
		{
			Expression condition = conditions.get(i);
			sb.append(" WHEN ").append(condition.toString()).append(" THEN ").append(values.get(i));
		}
		if (elseExpression != null)
			sb.append(" ELSE ").append(elseExpression.toString());
		sb.append(" END");
		return sb.toString();
	}

	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		Iterator<Expression> it = conditions.iterator();
		while (it.hasNext())
		{
			result.addAll(it.next().usedColumns(availableColumns));
		}
		it = values.iterator();
		while (it.hasNext())
		{
			result.addAll(it.next().usedColumns(availableColumns));
		}
		if (elseExpression != null)
			result.addAll(elseExpression.usedColumns(availableColumns));
		return result;
	}
	
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		Iterator<Expression> it = conditions.iterator();
		while (it.hasNext())
		{
			result.addAll(it.next().aggregateFunctions());
		}
		it = values.iterator();
		while (it.hasNext())
		{
			result.addAll(it.next().aggregateFunctions());
		}
		if (elseExpression != null)
			result.addAll(elseExpression.aggregateFunctions());
		return result;
	}
}