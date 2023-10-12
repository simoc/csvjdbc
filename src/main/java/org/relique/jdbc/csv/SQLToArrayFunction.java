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
