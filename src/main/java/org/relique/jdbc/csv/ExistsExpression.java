/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2015  Simon Chenery
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

class ExistsExpression extends LogicalExpression
{
	SubQueryExpression subQuery = null;

	public ExistsExpression(SubQueryExpression subQuery)
	{
		this.subQuery = subQuery;
	}
	public Boolean isTrue(Map<String, Object> env) throws SQLException
	{
		boolean matches = subQuery.evalList(env, new ExistsExpressionSubQueryRowMatcher());
		return Boolean.valueOf(matches);
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("EXISTS ");
		sb.append(subQuery.toString());
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return subQuery.usedColumns(availableColumns);
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		return subQuery.aggregateFunctions();
	}
}