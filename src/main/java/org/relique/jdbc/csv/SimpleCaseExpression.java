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

class SimpleCaseExpression extends Expression
{
	private Expression caseExpression;
	private List<Expression> switches;
	private List<Expression> values;
	private Expression elseExpression;

	public SimpleCaseExpression(Expression caseExpression,
		List<Expression> switches, List<Expression> values, Expression elseExpression)
	{
		this.caseExpression = caseExpression;
		this.switches = switches;
		this.values = values;
		this.elseExpression = elseExpression;
	}

	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object o1 = caseExpression.eval(env);
		for (int i = 0; i < switches.size(); i++)
		{
			Expression expr = switches.get(i);
			Object o2 = expr.eval(env);
			Integer comparison = RelopExpression.compare((Comparable)o1, (Comparable)o2, env);
			if (comparison.intValue() == 0)
				return values.get(i).eval(env);
		}
		if (elseExpression != null)
			return elseExpression.eval(env);

		return null;
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder("CASE ");
		sb.append(caseExpression.toString());
		for (int i = 0; i < switches.size(); i++)
		{
			Expression expr = switches.get(i);
			sb.append(" WHEN ").append(expr.toString()).append(" THEN ").append(values.get(i));
		}
		if (elseExpression != null)
			sb.append(" ELSE ").append(elseExpression.toString());
		sb.append(" END");
		return sb.toString();
	}

	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(caseExpression.usedColumns(availableColumns));
		Iterator<Expression> it = switches.iterator();
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
		Iterator<Expression> it = switches.iterator();
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