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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class RelopExpression extends LogicalExpression
{
	String op;
	Expression left, right;
	public RelopExpression(String op, Expression left, Expression right)
	{
		this.op = op;
		this.left = left;
		this.right = right;
	}
	public boolean isTrue(Map<String, Object> env)
	{
		boolean result = false;
		Comparable leftValue = (Comparable)left.eval(env);
		Comparable rightValue = (Comparable)right.eval(env);
		Integer leftComparedToRightObj = compare(leftValue, rightValue,  env);
		if (leftComparedToRightObj != null)
		{
			int leftComparedToRight = leftComparedToRightObj.intValue();
			if (leftValue != null && rightValue != null)
			{
				if (op.equals("="))
				{
					result = leftComparedToRight == 0;
				}
				else if (op.equals("<>") || op.equals("!="))
				{
					result = leftComparedToRight != 0;
				}
				else if (op.equals(">"))
				{
					result = leftComparedToRight>0;
				}
				else if (op.equals("<"))
				{
					result = leftComparedToRight<0;
				}
				else if (op.equals("<=") || op.equals("=<"))
				{
					result = leftComparedToRight <= 0;
				}
				else if (op.equals(">=") || op.equals("=>"))
				{
					result = leftComparedToRight >= 0;
				}
			}
		}
		return result;
	}
	public static Integer compare(Comparable leftValue,
		Comparable rightValue, Map<String, Object> env)
	{
		Integer leftComparedToRightObj = null;
		try
		{
			if (leftValue != null && rightValue != null)
				leftComparedToRightObj = new Integer(leftValue.compareTo(rightValue));
		}
		catch (ClassCastException e)
		{
		}
		try
		{
			if (leftComparedToRightObj == null)
			{
				if (leftValue == null || rightValue == null)
				{
					/*
					 * Do nothing.  Anything compared with NULL is false.
					 */
				}
				else if (leftValue instanceof Date)
				{
					Expression stringConverter = new ColumnName("@StringConverter");
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					Date date = sc.parseDate(rightValue.toString());
					if (date != null)
						leftComparedToRightObj = new Integer(leftValue.compareTo(date));
				}
				else if (rightValue instanceof Date)
				{
					Expression stringConverter = new ColumnName("@StringConverter");
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					Date date = sc.parseDate(leftValue.toString());
					if (date != null)
						leftComparedToRightObj = new Integer(date.compareTo((Date)rightValue));
				}
				else if (leftValue instanceof Time)
				{
					Expression stringConverter = new ColumnName("@StringConverter");
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					Time time = sc.parseTime(rightValue.toString());
					if (time != null)
						leftComparedToRightObj = new Integer(leftValue.compareTo(time));
				}
				else if (rightValue instanceof Time)
				{
					Expression stringConverter = new ColumnName("@StringConverter");
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					Time time = sc.parseTime(leftValue.toString());
					if (time != null)
						leftComparedToRightObj = new Integer(time.compareTo((Time)rightValue));
				}
				else if (leftValue instanceof Timestamp)
				{
					Expression stringConverter = new ColumnName("@StringConverter");
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					Timestamp timestamp = sc.parseTimestamp(rightValue.toString());
					if (timestamp != null)
						leftComparedToRightObj = new Integer(leftValue.compareTo(timestamp));
				}
				else if (rightValue instanceof Timestamp)
				{
					Expression stringConverter = new ColumnName("@StringConverter");
					StringConverter sc = (StringConverter) stringConverter.eval(env);
					Timestamp timestamp = sc.parseTimestamp(leftValue.toString());
					if (timestamp != null)
						leftComparedToRightObj = new Integer(timestamp.compareTo((Timestamp)rightValue));
				}
				else
				{
					Double leftDouble = new Double(((Number)leftValue).toString());
					Double rightDouble = new Double(((Number)rightValue).toString());
					leftComparedToRightObj = new Integer(leftDouble.compareTo(rightDouble));
				}
			}
		}
    	catch (ClassCastException e)
		{
		}
		catch (NumberFormatException e)
		{
		}
		return leftComparedToRightObj;
	}
	public String toString()
	{
		return op+" "+left+" "+right;
	}
	public List<String> usedColumns()
	{
		List<String> result = new LinkedList<String>();
		result.addAll(left.usedColumns());
		result.addAll(right.usedColumns());
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
