/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2023 Simon Chenery
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

class SQLLineNumberFunction extends Expression
{
	/*
	 * Key for column name in database rows for accessing line number in CSV file.
	 */
	public static final String LINE_NUMBER_COLUMN_NAME = "@LINE_NUMBER";

	private Expression expression = new ColumnName(LINE_NUMBER_COLUMN_NAME);

	public SQLLineNumberFunction()
	{
	}
	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		//return new ColumnName(CsvStatement.LINE_NUMBER_COLUMN_NAME).eval(env);
		return expression.eval(env);
	}
	@Override
	public String toString()
	{
		return "LINE_NUMBER()";
	}
	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return new LinkedList<>();
	}
	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		return new LinkedList<>();
	}
}
