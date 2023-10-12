package org.relique.jdbc.csv;

import java.sql.SQLException;
import java.util.*;

public class SQLToArrayFunction extends SQLCoalesceFunction
{

	private HashSet<Object> distinctValues;

	public SQLToArrayFunction(boolean isDistinct, List<Expression> expressions)
	{
		super(expressions);

		if (isDistinct)
			this.distinctValues = new HashSet<>();
	}

	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		List<Object> values = new ArrayList<>(expressions.size());
		if (distinctValues != null)
			distinctValues.clear();
		for (Expression expr : expressions)
        {
			Object val = expr.eval(env);
			if (distinctValues == null)
			{
				values.add(val);
			}
			else if (!distinctValues.contains(val))
			{
				values.add(val);
				distinctValues.add(val);
			}
		}
		StringConverter converter = (StringConverter)env.get(StringConverter.COLUMN_NAME);
		return new SqlArray(values, converter);
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("TO_ARRAY(");
		if (distinctValues != null)
			sb.append("DISTINCT ");

		String delimiter = "";
		for (Expression expression : expressions)
		{
			sb.append(delimiter);
			sb.append(expression.toString());
			delimiter = ",";
		}
		sb.append(")");
		return sb.toString();
	}
}
