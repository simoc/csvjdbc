/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2024  Simon Chenery
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

public class SQLToNumberFunction extends Expression
{
	private Expression expr;
	private Expression pattern;

	public SQLToNumberFunction(Expression expr, Expression pattern)
	{
		this.expr = expr;
		this.pattern = pattern;
	}

	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;

		Object exprObj = expr.eval(env);
		if (exprObj != null)
		{
			Object patternObj = pattern.eval(env);
			if (patternObj != null)
			{
				Expression stringConverter = new ColumnName(StringConverter.COLUMN_NAME);
				StringConverter sc = (StringConverter)stringConverter.eval(env);

				String exprStr = exprObj.toString();
				String patternStr = patternObj.toString();
				retval = sc.parseNumberPattern(exprStr, patternStr);
			}
		}
		
		return retval;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("TO_NUMBER(");
		sb.append(expr);
		sb.append(",");
		sb.append(pattern);
		sb.append(")");
		return sb.toString();
	}
	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<>();
		result.addAll(expr.usedColumns(availableColumns));
		result.addAll(pattern.usedColumns(availableColumns));
		return result;
	}
	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<>();
		result.addAll(expr.aggregateFunctions());
		result.addAll(pattern.aggregateFunctions());
		return result;
	}
}
