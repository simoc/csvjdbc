package org.relique.jdbc.csv;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SqlArray implements Array 
{
	private final List<Object> values;
	private final String baseTypeName;
	private final int baseType;

	public SqlArray(List<Object> values, StringConverter converter) 
	{
		String[] inferredTypes = converter.inferColumnTypes(values.toArray());
		baseTypeName = getBaseTypeNameImpl(values, Arrays.asList(inferredTypes));
		baseType = getBaseTypeImpl(baseTypeName);

		this.values = values.stream()
				.map(o -> converter.convert(baseTypeName, o.toString()))
				.collect(Collectors.toList());
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
		if (index > values.size())
			throw new SQLException("index " + index + " is out of bounds for array with size " + values.size());
		int toIndex = Math.min(values.size(),  (int) index - 1 + count);
		return values.subList((int) index - 1, toIndex);
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
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
				": SqlArray.getResultSet()");
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
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
				": SqlArray.getResultSet(long, int)");
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
	}
}