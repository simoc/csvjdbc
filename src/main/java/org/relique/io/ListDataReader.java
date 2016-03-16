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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A reader from a list, enabling database metadata functions to return JDBC ResultSet objects
 * containing lists of tables, schemas and other metadata. 
 */
public class ListDataReader extends DataReader
{
	private String []columnNames;
	private String []columnTypes;
	private List<Object []> columnValues;
	private int rowIndex;

	public ListDataReader(String []columnNames, String []columnTypes, List<Object []> columnValues)
	{
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.columnValues = columnValues;
		rowIndex = -1;
	}

	@Override
	public boolean next() throws SQLException
	{
		rowIndex++;
		boolean retval = (rowIndex < columnValues.size());
		return retval;
	}

	@Override
	public String[] getColumnNames() throws SQLException
	{
		return columnNames;
	}

	@Override
	public void close() throws SQLException
	{
	}

	@Override
	public Map<String, Object> getEnvironment() throws SQLException
	{
		HashMap<String, Object> retval = new HashMap<String, Object>();
		Object []o = columnValues.get(rowIndex);
		for (int i = 0; i < columnNames.length; i++)
		{
			retval.put(columnNames[i], o[i]);
		}
		return retval;
	}

	@Override
	public String[] getColumnTypes() throws SQLException
	{
		return columnTypes;
	}

	@Override
	public int[] getColumnSizes() throws SQLException
	{
		int []columnSizes = new int[columnTypes.length];
		Arrays.fill(columnSizes, DEFAULT_COLUMN_SIZE);
		return columnSizes;
	}

	@Override
	public String getTableAlias()
	{
		return null;
	}
}
