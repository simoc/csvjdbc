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
import java.util.List;
import java.util.Map;
import java.util.Set;

class NotExpression extends LogicalExpression
{
	LogicalExpression content;
	boolean isValid;
	public NotExpression(Expression arg)
	{
		isValid = (arg instanceof LogicalExpression);
		if (isValid)
		{
			this.content = (LogicalExpression)arg;
			isValid = this.content.isValid();
		}
	}
	public boolean isValid()
	{
		return isValid;
	}
	public Boolean isTrue(Map<String, Object> env) throws SQLException
	{
		Boolean b = content.isTrue(env);
		if (b == null)
			return null;
		else if (b.booleanValue())
			return Boolean.FALSE;
		else
			return Boolean.TRUE;
	}
	public String toString()
	{
		return "NOT "+content;
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return content.usedColumns(availableColumns);
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		return content.aggregateFunctions();
	}
}
