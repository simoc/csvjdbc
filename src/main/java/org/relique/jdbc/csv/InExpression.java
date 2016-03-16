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

class InExpression extends LogicalExpression
{
	Expression obj;
	List<Expression> inList = null;
	SubQueryExpression subQuery = null;

	public InExpression(Expression obj, List<Expression> inList)
	{
		this.obj = obj;
		this.inList = inList;
	}
	public InExpression(Expression obj, SubQueryExpression subQuery)
	{
		this.obj = obj;
		this.subQuery = subQuery;
	}
	public Boolean isTrue(Map<String, Object> env) throws SQLException
	{
		Comparable objValue = (Comparable)obj.eval(env);
		if (objValue == null)
			return null;
		if (inList != null)
		{
			for (Expression expr: inList)
			{
				Comparable exprValue = (Comparable)expr.eval(env);
				Integer compared = RelopExpression.compare(objValue, exprValue, env);
				if (compared != null && compared.intValue() == 0)
					return Boolean.TRUE;
			}
		}
		else
		{
			boolean matches = subQuery.evalList(env, new InExpressionSubQueryRowMatcher(env, objValue));
			return Boolean.valueOf(matches);
		}
		return Boolean.FALSE;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("IN ");
		sb.append(obj.toString());

		if (inList != null)
		{
			sb.append(" (");
			String delimiter = "";
			for (Expression expr: inList)
			{
				sb.append(delimiter);
				sb.append(expr.toString());
				delimiter = ", ";
			}
			sb.append(")");
		}
		else
		{
			sb.append(subQuery.toString());
		}
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(obj.usedColumns(availableColumns));
		if (inList != null)
		{
			for (Expression expr: inList)
			{
				result.addAll(expr.usedColumns(availableColumns));
			}
		}
		if (subQuery != null)
		{
			result.addAll(subQuery.usedColumns(availableColumns));
		}
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		if (inList != null)
		{
			for (Expression expr: inList)
			{
				result.addAll(expr.aggregateFunctions());
			}
		}
		if (subQuery != null)
		{
			result.addAll(subQuery.aggregateFunctions());
		}
		return result;
	}
}
