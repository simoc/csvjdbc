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
	Expression trimChars;

	public SQLTrimFunction(Expression expression, Expression trimChars)
	{
		this.expression = expression;
		this.trimChars = trimChars;
	}
	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = expression.eval(env);
		if (retval != null)
		{
			String str = retval.toString();
			if (trimChars != null)
			{
				Object o = trimChars.eval(env);
				if (o != null)
				{
					String trim = o.toString();
					int startIndex = 0;
					while (startIndex < str.length() && trim.indexOf(str.charAt(startIndex)) >= 0)
							startIndex++;

					int endIndex = str.length() - 1;
					while (endIndex >= startIndex && trim.indexOf(str.charAt(endIndex)) >= 0)
						endIndex--;

					if (endIndex >= startIndex)
						retval = str.substring(startIndex, endIndex + 1);
					else
						retval = "";
				}
				else
				{
					retval = null;
				}
			}
			else
			{
				/*
				 * Trim whitespace by default.
				 */
				retval = str.trim();
			}
		}
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
