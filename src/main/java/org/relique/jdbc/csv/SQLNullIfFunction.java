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

class SQLNullIfFunction extends Expression
{
	Expression expression1;
	Expression expression2;
	public SQLNullIfFunction(Expression expression1, Expression expression2)
	{
		this.expression1 = expression1;
		this.expression2 = expression2;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval;
		Comparable value1 = (Comparable)expression1.eval(env);
		Comparable value2 = (Comparable)expression2.eval(env);
		Integer compared = RelopExpression.compare(value1, value2, env);

		if (compared != null && compared.intValue() == 0)
			retval = null;
		else
			retval = value1;
		return retval;
	}
	public String toString()
	{
		return "NULLIF("+expression1+","+expression2+")";
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(expression1.usedColumns(availableColumns));
		result.addAll(expression2.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(expression1.aggregateFunctions());
		result.addAll(expression2.aggregateFunctions());
		return result;
	}
}
