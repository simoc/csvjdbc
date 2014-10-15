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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
	public boolean isTrue(Map<String, Object> env) throws SQLException
	{
		return !content.isTrue(env);
	}
	public String toString()
	{
		return "NOT "+content;
	}
	public List<String> usedColumns()
	{
		return content.usedColumns();
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		return content.aggregateFunctions();
	}
}
