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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class SQLMinFunction extends AggregateFunction
{
	boolean isDistinct;
	Expression expression;
	Object min = null;
	public SQLMinFunction(boolean isDistinct, Expression expression)
	{
		this.isDistinct = isDistinct;
		this.expression = expression;
	}
	public Object eval(Map<String, Object> env)
	{
		Object o = env.get("@GROUPROWS");
		if (o != null)
		{
			/*
			 * Find the minimum from the rows grouped together
			 * by the GROUP BY clause.
			 */
			List groupRows = (List)o;
			Object minInGroup = null;
			for (int i = 0; i < groupRows.size(); i++)
			{
				o = expression.eval((Map)groupRows.get(i));
				if (o != null)
				{
					if (minInGroup == null || ((Comparable)minInGroup).compareTo(o) > 0)
						minInGroup = o;
				}
			}
			return minInGroup;
		}
		return min;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder("MIN(");
		if (isDistinct)
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
		/*
		 * Only consider non-null values.
		 */
		Object o = expression.eval(env);
		if (o != null)
		{
			if (min == null || ((Comparable)min).compareTo(o) > 0)
				min = o;
		}
	}
}
