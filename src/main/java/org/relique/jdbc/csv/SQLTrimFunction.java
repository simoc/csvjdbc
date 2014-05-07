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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class SQLTrimFunction extends Expression
{
	Expression expression;

	public SQLTrimFunction(Expression expression)
	{
		this.expression = expression;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = expression.eval(env);
		if (retval != null)
			retval = retval.toString().trim();
		return retval;
	}
	public String toString()
	{
		return "TRIM("+expression+")";
	}
	public List<String> usedColumns()
	{
		List<String> result = new LinkedList<String>();
		result.addAll(expression.usedColumns());
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(expression.aggregateFunctions());
		return result;
	}
}
