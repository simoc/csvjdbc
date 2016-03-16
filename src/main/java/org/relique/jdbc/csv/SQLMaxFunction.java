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

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SQLMaxFunction extends AggregateFunction
{
	boolean isDistinct;
	Expression expression;
	Object max = null;
	public SQLMaxFunction(boolean isDistinct, Expression expression)
	{
		this.isDistinct = isDistinct;
		this.expression = expression;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object o = env.get(GROUPING_COLUMN_NAME);
		if (o != null)
		{
			/*
			 * Find the maximum from the rows grouped together
			 * by the GROUP BY clause.
			 */
			List groupRows = (List)o;
			Object maxInGroup = null;
			for (int i = 0; i < groupRows.size(); i++)
			{
				o = expression.eval((Map)groupRows.get(i));
				if (o != null)
				{
					if (maxInGroup == null || ((Comparable)maxInGroup).compareTo(o) < 0)
						maxInGroup = o;
				}
			}
			return maxInGroup;
		}
		return max;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder("MAX(");
		if (isDistinct)
			sb.append("DISTINCT ");
		sb.append(expression);
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return new LinkedList<String>();
	}
	public List<String> aggregateColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(expression.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.add(this);
		return result;
	}
	public void resetAggregateFunctions()
	{
		this.max = null;
	}
	public void processRow(Map<String, Object> env) throws SQLException
	{
		/*
		 * Only consider non-null values.
		 */
		Object o = expression.eval(env);
		if (o != null)
		{
			if (max == null || ((Comparable)max).compareTo(o) < 0)
				max = o;
		}
	}
}
