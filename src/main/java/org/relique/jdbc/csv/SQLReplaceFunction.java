/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2022  Simon Chenery
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
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

class SQLReplaceFunction extends Expression
{
	Expression expr;
	Expression from;
	Expression to;

	public SQLReplaceFunction(Expression expr, Expression from, Expression to)
	{
		this.expr = expr;
		this.from = from;
		this.to = to;
	}

	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;

		Object str = expr.eval(env);
		if (str != null)
		{
			Object fromObj = from.eval(env);
			if (fromObj != null)
			{
				Object toObj = to.eval(env);
				if (toObj != null)
				{
					String fromStr = fromObj.toString();
					if (fromStr.isEmpty())
					{
						// Return original string if nothing to replace from.
						retval = str.toString();
					}
					else
					{
						retval = str.toString().replace(fromStr, toObj.toString());
					}
				}
			}
		}

		return retval;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("REPLACE(");
		sb.append(expr);
		sb.append(",");
		sb.append(from);
		sb.append(",");
		sb.append(to);
		return sb.toString();
	}
	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<>();
		result.addAll(expr.usedColumns(availableColumns));
		result.addAll(from.usedColumns(availableColumns));
		result.addAll(to.usedColumns(availableColumns));
		return result;
	}
	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<>();
		result.addAll(expr.aggregateFunctions());
		result.addAll(from.aggregateFunctions());
		result.addAll(to.aggregateFunctions());
		return result;
	}
}
