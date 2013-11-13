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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class IsNullExpression extends LogicalExpression
{
	Expression arg;
	public IsNullExpression(Expression arg)
	{
		this.arg = arg;
	}
	public boolean isTrue(Map<String, Object> env)
	{
		Object o = arg.eval(env);
		return (o == null);
	}
	public String toString()
	{
		return "N "+arg;
	}
	public List<String> usedColumns()
	{
		List<String> result = new LinkedList<String>();
		result.addAll(arg.usedColumns());
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(arg.aggregateFunctions());
		return result;
	}
}
