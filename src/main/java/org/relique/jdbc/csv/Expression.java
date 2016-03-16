/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2008 Mario Frasca
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

public abstract class Expression
{
	public Object eval(Map<String, Object> env) throws SQLException
	{
		return null;
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return null;
	}

	public List<AggregateFunction> aggregateFunctions()
	{
		return new LinkedList<AggregateFunction>();
	}

	/**
	 * Is this a valid expression such as A &gt; 5 or an invalid
	 * mix of logical and arithmetic such as (A &gt; 5) + 1 that we
	 * cannot detect during parsing.
	 * @return true if valid.
	 */
	public boolean isValid()
	{
		return true;
	}

	/**
	 * Reset aggregate function each time it is used in a subquery.
	 */
	public void resetAggregateFunctions()
	{
	}
}