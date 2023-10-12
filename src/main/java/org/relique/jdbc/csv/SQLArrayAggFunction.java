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
		return new SqlArray(values, converter);

	}

	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return Collections.emptyList();
	}

	@Override
	public List<String> aggregateColumns(Set<String> availableColumns)
	{
		return new LinkedList<>(expression.usedColumns(availableColumns));
	}

	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<>();
		result.add(this);
		return result;
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
