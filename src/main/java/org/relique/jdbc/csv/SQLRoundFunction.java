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

class SQLRoundFunction extends Expression
{
	Expression expression;
	public SQLRoundFunction(Expression expression)
	{
		this.expression = expression;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = expression.eval(env);
		if (retval != null)
		{
			if (!(retval instanceof Number))
			{
				try
				{
					retval = new Double(retval.toString());
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
					retval = new Integer(((Short)retval).intValue());
				}
				else if (!(retval instanceof Integer || retval instanceof Long))
				{
					double d = ((Number)retval).doubleValue();
					if (d < Integer.MIN_VALUE || d > Integer.MAX_VALUE)
						retval = new Double(Math.round(d));
					else
						retval = new Integer((int)Math.round(d));
				}
			}
		}
		return retval;
	}
	public String toString()
	{
		return "ROUND("+expression+")";
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(expression.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(expression.aggregateFunctions());
		return result;
	}
}
