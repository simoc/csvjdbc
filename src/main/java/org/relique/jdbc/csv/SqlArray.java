/*
CsvJdbc - a JDBC driver for CSV files
Copyright (C) 2023  Peter Fokkinga

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
package org.relique.jdbc.csv;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.relique.io.ListDataReader;


public class SqlArray implements Array 
{
	private final List<Object> values;
	private final String baseTypeName;
	private final int baseType;
	private boolean freed;
	Connection createdByConnection;
	CsvStatement internalStatement;

	public SqlArray(List<Object> values, StringConverter converter, Connection connection)
	{
		String[] inferredTypes = converter.inferColumnTypes(values.toArray());
		baseTypeName = getBaseTypeNameImpl(values, Arrays.asList(inferredTypes));
		baseType = getBaseTypeImpl(baseTypeName);

		this.values = values.stream()
				.map(o -> converter.convert(baseTypeName, o.toString()))
				.collect(Collectors.toList());
		this.createdByConnection = connection;
	}

	protected void checkFreed() throws SQLException
	{
		if (freed)
			throw new SQLException(CsvResources.getString("freedArray"));
	}

	@Override
	public String getBaseTypeName() throws SQLException 
	{
		return baseTypeName;
	}

	private String getBaseTypeNameImpl(List<Object> values, List<String> inferredTypes) 
	{
		int index = 0;
		String result = null;
		for (Object value : values) 
		{
			if (value == null || value.toString().isEmpty()) 
			{
				index++;
			} 
			else if (result == null) 
			{
				result = inferredTypes.get(index++);
			} 
			else if (!result.equals(inferredTypes.get(index++))) 
			{
				throw new IllegalStateException("type of element at index " + index
						+ "(1 based) does not match the element(s) in front");
			}
		}
		return result;
	}

	@Override
	public int getBaseType() throws SQLException 
	{
		return baseType;
	}

	private int getBaseTypeImpl(String typeName) 
	{
		return "Int".equals(typeName) // inconsistency in StringConverter.getTypeInfo()
				? Types.INTEGER
				: StringConverter.getTypeInfo().stream()
				.filter(oa -> oa[0].equals(typeName))
				.findFirst()
				.map(o -> (Integer) o[1])
				.orElse(Types.NULL);
	}

	@Override
	public Object getArray() throws SQLException 
	{
		checkFreed();

		return values.toArray();
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException 
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
				": SqlArray.getArray(Map)");
	}

	@Override
	public Object getArray(long index, int count) throws SQLException 
	{
		checkFreed();

		if (index < 1 || index > values.size() || count < 0)
		{
			throw new SQLException(CsvResources.getString("arraySubListOutOfBounds") + ": " +
				index + ": " + count);
		}
		int toIndex = Math.min(values.size(),  (int) index - 1 + count);
		return values.subList((int) index - 1, toIndex).toArray();
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException 
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
				": SqlArray.getArray(long, int, Map)");
	}

	@Override
	public ResultSet getResultSet() throws SQLException 
	{
		return getResultSet(1, values.size());
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException 
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
				": SqlArray.getResultSet(Map)");
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException 
	{
		checkFreed();

		if (index < 1 || index > values.size() || count < 0)
		{
			throw new SQLException(CsvResources.getString("arraySubListOutOfBounds") + ": " +
				index + ": " + count);
		}

		String[] columnNames = {"INDEX", "VALUE"};
		String[] columnTypes = {"Integer", getBaseTypeName()};
		String joinedColumnTypes = columnTypes[0] + "," + columnTypes[1];
		ArrayList<Object[]> columnValues = new ArrayList<>();
		for (int i = 0; i < count; i++)
		{
			// The first array element has index 1.
			Object[] o = {Integer.valueOf(i + (int)index), values.get(i + (int)index - 1)};
			columnValues.add(o);
		}

		ListDataReader reader = new ListDataReader(columnNames,
			columnTypes, columnValues);
		ArrayList<Object[]> queryEnvironment = new ArrayList<>();
		queryEnvironment.add(new Object[]{"*", new AsteriskExpression("*")});
		ResultSet retval = null;

		try
		{
			if (internalStatement == null)
			{
				internalStatement = (CsvStatement)createdByConnection.createStatement();
			}

			retval = new CsvResultSet(internalStatement,
				reader,
				"",
				queryEnvironment,
				false,
				ResultSet.TYPE_FORWARD_ONLY,
				null,
				null,
				null,
				null,
				-1,
				0,
				joinedColumnTypes,
				0,
				0,
				new HashMap<>());
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e.getMessage());
		}
		return retval;
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException 
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
				": SqlArray.getResultSet(long, int, Map)");
	}

	@Override
	public void free() throws SQLException 
	{
		values.clear();
		if (internalStatement != null)
		{
			internalStatement.close();
			internalStatement = null;
		}
		freed = true;
	}
}
