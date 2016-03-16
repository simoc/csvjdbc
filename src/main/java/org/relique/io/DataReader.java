/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
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
package org.relique.io;

import java.sql.SQLException;
import java.util.Map;

public abstract class DataReader
{
	public static final int DEFAULT_COLUMN_SIZE = 20;

	public DataReader()
	{
		super();
	}

	abstract public boolean next() throws SQLException;

	abstract public String[] getColumnNames() throws SQLException;

	abstract public void close() throws SQLException;

	abstract public Map<String, Object> getEnvironment() throws SQLException;
	
	abstract public String[] getColumnTypes() throws SQLException;

	abstract public int[] getColumnSizes() throws SQLException;

	abstract public String getTableAlias();
}