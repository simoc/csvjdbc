/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2014 Simon Chenery
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class SQLCoalesceFunction extends Expression
{
	List<Expression> expressions;
	public SQLCoalesceFunction(List<Expression> expressions)
	{
		this.expressions = expressions;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;

		/*
		 * Find first expression that does not evaluate to NULL.
		 */
		Iterator<Expression> it = expressions.iterator();
		while (retval == null && it.hasNext())
		{
			Expression expr = it.next();
			retval = expr.eval(env);
		}
		return retval;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder("COALESCE(");
		String delimiter = "";
		Iterator<Expression> it = expressions.iterator();
		while (it.hasNext())
		{
			sb.append(delimiter);
			sb.append(it.next().toString());
			delimiter = ",";
		}
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		Iterator<Expression> it = expressions.iterator();
		while (it.hasNext())
		{
			result.addAll(it.next().usedColumns(availableColumns));
		}
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		Iterator<Expression> it = expressions.iterator();
		while (it.hasNext())
		{
			result.addAll(it.next().aggregateFunctions());
		}
		return result;
	}
}
