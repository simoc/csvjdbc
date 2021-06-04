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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SQLRoundFunction extends Expression
{
	Expression expression;
	Expression decimals;

	public SQLRoundFunction(Expression expression, Expression decimals)
	{
		this.expression = expression;
		this.decimals = decimals;
	}

	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = expression.eval(env);
		if (retval != null)
		{
			if (!(retval instanceof Number))
			{
				try
				{
					retval = Double.valueOf(retval.toString());
				}
				catch(NumberFormatException e)
				{
					retval = null;
				}
			}
			if (retval != null)
			{
				if (retval instanceof Short)
				{
					retval = Integer.valueOf(((Short) retval).intValue());
				}
				else if (!(retval instanceof Integer || retval instanceof Long))
				{
					double d = ((Number) retval).doubleValue();
					int newScale = 0;
					if (decimals != null)
					{
						Object decimalsObj = decimals.eval(env);
						if (decimalsObj != null)
						{
							if (decimalsObj instanceof Number)
							{
								newScale = ((Number) decimalsObj).intValue();
							}
							else
							{
								try
								{
									newScale = Integer.parseInt(decimalsObj.toString());
								}
								catch (NumberFormatException e)
								{
									newScale = 0;
								}
							}
						}
					}
					BigDecimal rounded = BigDecimal.valueOf(d).setScale(newScale, RoundingMode.HALF_UP);
					if (newScale > 0 || d < Integer.MIN_VALUE || d > Integer.MAX_VALUE)
					{
						retval = Double.valueOf(rounded.doubleValue());
					}
					else
					{
						retval = Integer.valueOf(rounded.intValue());
					}
				}
			}
		}
		return retval;
	}

	@Override
	public String toString()
	{
		StringBuilder stringBuilder = new StringBuilder("ROUND(");
		stringBuilder.append(expression);
		stringBuilder.append(")");
		if (decimals != null)
		{
			stringBuilder.append(",");
			stringBuilder.append(decimals);
		}
		return stringBuilder.toString();
	}

	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<>();
		result.addAll(expression.usedColumns(availableColumns));
		if (decimals != null)
		{
			result.addAll(decimals.usedColumns(availableColumns));
		}
		return result;
	}

	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<>();
		result.addAll(expression.aggregateFunctions());
		if (decimals != null)
		{
			result.addAll(decimals.aggregateFunctions());
		}
		return result;
	}
}
