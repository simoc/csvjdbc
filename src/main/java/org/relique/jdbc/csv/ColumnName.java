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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ColumnName extends Expression
{
	private String columnName;

	public ColumnName(String columnName)
	{
		this.columnName = columnName;
	}

	public String getColumnName()
	{
		return columnName;
	}

	@Override
	public Object eval(Map<String, Object> env)
	{
		return env.get(columnName);
	}

	@Override
	public String toString()
	{
		return "["+columnName+"]";
	}

	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<>();
		result.add(columnName);
		return result;
	}
}
