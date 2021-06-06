/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2015  Simon Chenery
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

class SQLAbsFunction extends Expression
{
	Expression expression;
	public SQLAbsFunction(Expression expression)
	{
		this.expression = expression;
	}
	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;
		Object o = expression.eval(env);
		if (o != null)
		{
			if (!(o instanceof Number))
			{
				try
				{
					o = Double.valueOf(o.toString());
				}
				catch(NumberFormatException e)
				{
					o = null;
				}
			}

			if (o != null)
			{
				if (o instanceof Short)
				{
					int i = ((Short)o).intValue();
					if (i < 0)
						i = -i;
					retval = Integer.valueOf(i);
				}
				else if (o instanceof Integer)
				{
					int i = ((Integer)o).intValue();
					if (i < 0)
						i = -i;
					retval = Integer.valueOf(i);
				}
				else if (o instanceof Long)
				{
					long l = ((Long)o).intValue();
					if (l < 0)
						l = -l;
					retval = Long.valueOf(l);
				}
				else
				{
					double d = ((Number)o).doubleValue();
					if (d < 0)
						d = -d;
					retval = Double.valueOf(d);
				}
			}
		}
		return retval;
	}
	@Override
	public String toString()
	{
		return "ABS("+expression+")";
	}
	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<>();
		result.addAll(expression.usedColumns(availableColumns));
		return result;
	}
	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<>();
		result.addAll(expression.aggregateFunctions());
		return result;
	}
}
