/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2023  Peter Fokkinga

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
package org.relique.jdbc.csv;

import java.sql.SQLException;
import java.util.*;

public class SQLArrayAggFunction extends AggregateFunction
{
	final boolean isDistinct;
	final Expression expression;
	final ArrayList<Object> aggregateValues = new ArrayList<>();
	final LinkedHashSet<Object> distinctAggregateValues = new LinkedHashSet<>();

	public SQLArrayAggFunction(boolean isDistinct, Expression expression)
	{
		this.isDistinct = isDistinct;
		this.expression = expression;
	}

	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		List<Object> values = new ArrayList<>();
		Object o = env.get(GROUPING_COLUMN_NAME);
		if (o != null)
		{
			List groupRows = (List)o;
			for (int i = 0; i < groupRows.size(); i++)
			{
				o = expression.eval((Map)groupRows.get(i));
				if (o != null)
				{
					values.add(o);
				}
			}
		}
		else
		{
			Iterator<Object> it = aggregateValues.iterator();
			while (it.hasNext())
			{
				values.add(it.next());
			}
			it = distinctAggregateValues.iterator();
			while (it.hasNext())
			{
				values.add(it.next());
			}
		}
		StringConverter converter = (StringConverter)env.get(StringConverter.COLUMN_NAME);
		CsvStatement statement = (CsvStatement)env.get(CsvStatement.STATEMENT_COLUMN_NAME);
		return new SqlArray(values, converter, statement.getConnection());

	}

	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return List.of();
	}

	@Override
	public List<String> aggregateColumns(Set<String> availableColumns)
	{
		return List.copyOf(expression.usedColumns(availableColumns));
	}

	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		return List.of(this);
	}

	@Override
	public void resetAggregateFunctions()
	{
		distinctAggregateValues.clear();
		aggregateValues.clear();
	}

	@Override
	public void processRow(Map<String, Object> env) throws SQLException
	{
		/*
		 * Only consider non-null values.
		 */
		Object o = expression.eval(env);
		if (o != null)
		{
			if (isDistinct)
				distinctAggregateValues.add(o);
			else
				aggregateValues.add(o);
		}
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("ARRAY_AGG(");
		if (isDistinct)
			sb.append("DISTINCT ");
		sb.append(expression);
		sb.append(")");
		return sb.toString();
	}
}
