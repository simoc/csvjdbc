/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2015  Simon Chenery
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
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

class SQLSubstringFunction extends Expression
{
	private static final Integer MAX_LENGTH = Integer.valueOf(Integer.MAX_VALUE);

	Expression expr;
	Expression startIndex;
	Expression len;

	public SQLSubstringFunction(Expression expr, Expression startIndex, Expression len)
	{
		this.expr = expr;
		this.startIndex = startIndex;
		this.len = len;
	}

	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;

		Object str = expr.eval(env);
		if (str != null)
		{
			Object startIndexObj = startIndex.eval(env);
			if (startIndexObj != null)
			{
				if (len != null)
				{
					Object lenObj = len.eval(env);
					if (lenObj != null)
						retval = substring(str, startIndexObj, lenObj);
				}
				else
				{
					retval = substring(str, startIndexObj, MAX_LENGTH);
				}
			}
		}

		return retval;
	}

	private Object substring(Object str, Object startIndex, Object len) throws SQLException
	{
		int start = 0;
		long nChars = 0;
		Object retval = null;
		boolean parseIntFailed = false;

		if (startIndex instanceof Number)
		{
			start = ((Number)startIndex).intValue();
		}
		else
		{
			try
			{
				start = Integer.parseInt(startIndex.toString());
			}
			catch(NumberFormatException e)
			{
				parseIntFailed = true;
			}
		}

		if (len instanceof Number)
		{
			nChars = ((Number)len).intValue();
		}
		else
		{
			try
			{
				nChars = Integer.parseInt(len.toString());
			}
			catch(NumberFormatException e)
			{
				parseIntFailed = true;
			}
		}

		if (!parseIntFailed)
		{
			/*
			 * Use Java zero-based indexing.
			 */
			start--;

			if (start < 0)
			{
				/*
				 * Enable substring("hello", -2, 4) => "h", like PostgreSQL.
				 */
				nChars += start;
				start = 0;
			}
			if (nChars < 0)
				nChars = 0;

			String s2 = str.toString();
			
			if (start >= s2.length())
			{
				retval = "";
			}
			else
			{
				long endIndex = start + nChars;
				if (endIndex > s2.length())
				{
					endIndex = s2.length();
				}
				retval = s2.substring(start, (int)endIndex);
			}
		}

		return retval;
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder("SUBSTRING(");
		sb.append(expr);
		sb.append(",");
		sb.append(startIndex);
		if (len != null)
		{
			sb.append(",");
			sb.append(len);
		}
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		result.addAll(expr.usedColumns(availableColumns));
		result.addAll(startIndex.usedColumns(availableColumns));
		if (len != null)
			result.addAll(len.usedColumns(availableColumns));
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		result.addAll(expr.aggregateFunctions());
		result.addAll(startIndex.aggregateFunctions());
		if (len != null)
			result.addAll(len.aggregateFunctions());
		return result;
	}
}
