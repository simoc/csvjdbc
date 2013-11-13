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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class SQLCountFunction extends AggregateFunction
{
	HashSet<Object> distinctValues;
	Expression expression;
	int counter = 0;
	public SQLCountFunction(boolean isDistinct, Expression expression)
	{
		if (isDistinct)
			this.distinctValues = new HashSet<Object>();
		this.expression = expression;
	}
	public Object eval(Map<String, Object> env)
	{
		Integer retval;
		Object o = env.get("@GROUPROWS");
		if (o != null)
		{
			/*
			 * The count is the number of rows grouped together
			 * by the GROUP BY clause.
			 */
			List groupRows = (List)o;
			if (this.distinctValues != null)
			{
				HashSet<Object> unique = new HashSet<Object>();
				for (int i = 0; i < groupRows.size(); i++)
				{
					o = expression.eval((Map)groupRows.get(i));
					if (o != null)
						unique.add(o);
				}
				retval = Integer.valueOf(unique.size());
			}
			else
			{
				retval = Integer.valueOf(groupRows.size());
			}
		}
		else
		{
			if (this.distinctValues != null)
				retval = Integer.valueOf(this.distinctValues.size());
			else
				retval = Integer.valueOf(counter);
		}
		return retval;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder("COUNT(");
		if (distinctValues != null)
			sb.append("DISTINCT ");
		sb.append(expression);
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns()
	{
		return new LinkedList<String>();
	}
	public List<String> aggregateColumns()
	{
		List<String> result = new LinkedList<String>();
		if (!(expression instanceof AsteriskExpression))
			result.addAll(expression.usedColumns());
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.add(this);
		return result;
	}
	public void processRow(Map<String, Object> env)
	{
		if (expression instanceof AsteriskExpression)
		{
			counter++;
		}
		else
		{
			/*
			 * Only count non-null values.
			 */
			Object o = expression.eval(env);
			if (o != null)
			{
				counter++;
				if (distinctValues != null)
				{
					/*
					 * We want a count of DISTINCT values, so we have
					 * to keep a list of unique values.
					 */
					distinctValues.add(o);
				}
			}
		}
	}
}
