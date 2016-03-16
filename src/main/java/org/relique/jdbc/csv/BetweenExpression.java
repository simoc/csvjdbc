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

class BetweenExpression extends LogicalExpression
{
	Expression obj, left, right;
	public BetweenExpression(Expression obj, Expression left, Expression right)
	{
		this.obj = obj;
		this.left = left;
		this.right = right;
	}
	public Boolean isTrue(Map<String, Object> env) throws SQLException
	{
		Comparable leftValue = (Comparable)left.eval(env);
		Comparable rightValue = (Comparable)right.eval(env);
		Comparable objValue = (Comparable)obj.eval(env);
		Integer comparedLeft = RelopExpression.compare(leftValue, objValue, env);
		if (comparedLeft == null)
			return null;
		if (comparedLeft.intValue() <= 0)
		{
			Integer comparedRight = RelopExpression.compare(rightValue, objValue, env);
			if (comparedRight == null)
				return null;
			if (comparedRight.intValue() >= 0)
			{
				return Boolean.TRUE;
			}
		}
		return Boolean.FALSE;
	}
	public String toString()
	{
		return "B "+obj+" "+left+" "+right;
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(obj.usedColumns(availableColumns));
		result.addAll(left.usedColumns(availableColumns));
		result.addAll(right.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(obj.aggregateFunctions());
		result.addAll(left.aggregateFunctions());
		result.addAll(right.aggregateFunctions());
		return result;
	}
}
