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

class LikeExpression extends LogicalExpression
{
	Expression arg1, arg2;
	public LikeExpression(Expression arg1, Expression arg2)
	{
		this.arg1 = arg1;
		this.arg2 = arg2;
	}
	public boolean isTrue(Map<String, Object> env)
	{
		Object left = arg1.eval(env);
		Object right = arg2.eval(env);
		boolean result = false;
		if (left != null && right != null)
			result = LikePattern.matches(right.toString(), left.toString());
		return result;
	}
	public String toString()
	{
		return "L "+arg1+" "+arg2;
	}
	public List<String> usedColumns()
	{
		List<String> result = new LinkedList<String>();
		result.addAll(arg1.usedColumns());
		result.addAll(arg2.usedColumns());
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(arg1.aggregateFunctions());
		result.addAll(arg2.aggregateFunctions());
		return result;
	}
}
