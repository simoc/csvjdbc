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

class OrExpression extends LogicalExpression
{
	LogicalExpression left, right;
	boolean isValid;
	public OrExpression(Expression left, Expression right)
	{
		isValid = (left instanceof LogicalExpression && right instanceof LogicalExpression);
		if (isValid)
		{
			this.left = (LogicalExpression)left;
			this.right = (LogicalExpression)right;
			isValid = left.isValid() && right.isValid();
		}
	}
	public boolean isValid()
	{
		return isValid;
	}
	public Boolean isTrue(Map<String, Object> env) throws SQLException
	{
		Boolean leftIsTrue = left.isTrue(env);
		if (leftIsTrue != null && leftIsTrue.booleanValue())
		{
			return Boolean.TRUE;
		}
		else
		{
			Boolean rightIsTrue = right.isTrue(env);
			if (rightIsTrue != null && rightIsTrue.booleanValue())
				return Boolean.TRUE;

			if (leftIsTrue == null && rightIsTrue == null)
				return null;
			else
				return Boolean.FALSE;
		}
	}
	public String toString()
	{
		return "OR "+left+" "+right;
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(left.usedColumns(availableColumns));
		result.addAll(right.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(left.aggregateFunctions());
		result.addAll(right.aggregateFunctions());
		return result;
	}
}
