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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A place holder for a parameter in a prepared statement.
 * Each parameter is identified by its position in the prepared statement,
 * counting from 1.
 */
class Placeholder extends Expression
{
	/** position of this place holder, counting from 1. */
	private int position;

	/**
	 * create a new placeholder, with the next available position number.
	 */
	public Placeholder(int position)
	{
		this.position = position;
	}

	@Override
	public Object eval(Map<String, Object> env)
	{
		return env.get("?" + position);
	}
	@Override
	public String toString()
	{
		return "?";
	}
	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return List.of();
	}
}
