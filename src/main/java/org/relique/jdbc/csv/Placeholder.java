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

/**
 * A place holder for a parameter in a prepared statement.
 */
class Placeholder extends Expression
{
	public static int nextIndex = 1;
	private int index;
	public Placeholder()
	{
		index = nextIndex;
		nextIndex++;
	}
	public Object eval(Map<String, Object> env)
	{
		return env.get("?" + index);
	}
	public String toString()
	{
		return "?";
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return new LinkedList<String>();
	}
}
