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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SQLSumFunction extends AggregateFunction
{
	HashSet<Object> distinctValues;
	Expression expression;
	BigDecimal sum = null;
	int counter = 0;
	public SQLSumFunction(boolean isDistinct, Expression expression)
	{
		if (isDistinct)
			this.distinctValues = new HashSet<Object>();
		this.expression = expression;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;
		Object o = env.get(GROUPING_COLUMN_NAME);
		if (o != null)
		{
			/*
			 * Find the sum of rows grouped together
			 * by the GROUP BY clause.
			 */
			List groupRows = (List)o;
			BigDecimal groupSum = null;
			counter = 0;
			if (this.distinctValues != null)
			{
				HashSet<Object> unique = new HashSet<Object>();
				for (int i = 0; i < groupRows.size(); i++)
				{
					o = expression.eval((Map)groupRows.get(i));
					if (o != null)
						unique.add(o);
				}
				for (Object obj: unique)
				{
					if (groupSum == null)
						groupSum = new BigDecimal(obj.toString());
					else
						groupSum = groupSum.add(new BigDecimal(obj.toString()));
					counter++;
				}
			}
			else
			{
				for (int i = 0; i < groupRows.size(); i++)
				{
					o = expression.eval((Map)groupRows.get(i));
					if (o != null)
					{
						try
						{
							if (groupSum == null)
								groupSum = new BigDecimal(o.toString());
							else
								groupSum = groupSum.add(new BigDecimal(o.toString()));
							counter++;
						}
						catch (NumberFormatException e)
						{
						}
					}
				}
			}
			try
			{
				if (groupSum != null)
					retval = Long.valueOf(groupSum.longValueExact());
			}
			catch (ArithmeticException e)
			{
				retval = groupSum.doubleValue();
			}
			return retval;
		}

		try
		{
			if (this.distinctValues != null)
			{
				BigDecimal groupSum = null;
				for (Object obj: this.distinctValues)
				{
					if (groupSum == null)
						groupSum = new BigDecimal(obj.toString());
					else
						groupSum = groupSum.add(new BigDecimal(obj.toString()));
				}
				counter = this.distinctValues.size();
				try
				{
					if (groupSum != null)
						retval = Long.valueOf(groupSum.longValueExact());
				}
				catch (ArithmeticException e)
				{
					retval = groupSum.doubleValue();
				}
			}
			else
			{
				if (sum != null)
					retval = Long.valueOf(sum.longValueExact());
			}
		}
		catch (ArithmeticException e)
		{
			retval = sum.doubleValue();
		}
		return retval;
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder("SUM(");
		if (distinctValues != null)
			sb.append("DISTINCT ");
		sb.append(expression);
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return new LinkedList<String>();
	}
	public List<String> aggregateColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(expression.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.add(this);
		return result;
	}
	public void resetAggregateFunctions()
	{
		if (this.distinctValues != null)
			this.distinctValues.clear();
		this.sum = null;
		this.counter = 0;
	}
	public void processRow(Map<String, Object> env) throws SQLException
	{
		/*
		 * Only consider non-null values.
		 */
		Object o = expression.eval(env);
		if (o != null)
		{
			try
			{
				if (sum == null)
					sum = new BigDecimal(o.toString());
				else
					sum = sum.add(new BigDecimal(o.toString()));
				counter++;
			}
			catch (NumberFormatException e)
			{
			}
			if (distinctValues != null)
			{
				/*
				 * We want the sum of DISTINCT values, so we have
				 * to keep a list of unique values.
				 */
				distinctValues.add(o);
			}
		}
	}
}
