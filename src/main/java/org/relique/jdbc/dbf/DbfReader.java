/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2011  Mario Frasca
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
package org.relique.jdbc.dbf;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.relique.io.DataReader;
import org.relique.jdbc.csv.CsvResources;
import org.relique.jdbc.csv.MinimumMemoryMap;

public class DbfReader extends DataReader
{
	private Object table = null;
	private List fields;
	private Integer recordCount;
	private Object record;
	private int rowNo;
	private Class<?> fieldClass;
	private Class<?> recordClass;
	private Class<?> tableClass;
	private Method tableOpenMethod;
	private Method tableGetFieldsMethod;
	private Method tableCloseMethod;
	private Method fieldGetNameMethod;
	private Method tableGetRecordCountMethod;
	private Method tableGetRecordAtMethod;
	private Method recordGetTypedValueMethod;
	private Method fieldGetTypeMethod;
	private Method fieldGetLengthMethod;
	private Map<String, String> dbfTypeToSQLType;
	private String upperTableName;
	private String tableAlias;
	private String[] columnNames = null;

	public DbfReader(String path, String tableName, String tableAlias, String charset) throws SQLException
	{
		super();
		try
		{
			fieldClass = Class.forName("nl.knaw.dans.common.dbflib.Field");
			recordClass = Class.forName("nl.knaw.dans.common.dbflib.Record");
			tableClass = Class.forName("nl.knaw.dans.common.dbflib.Table");
		} 
		catch (ClassNotFoundException e)
		{
			throw new DbfClassNotFoundException(CsvResources.getString("noDansDbf") + ": " + e);
		}
		try
		{
			tableOpenMethod = tableClass.getMethod("open");
			tableCloseMethod = tableClass.getMethod("close");
			tableGetFieldsMethod = tableClass.getMethod("getFields");
			fieldGetNameMethod = fieldClass.getMethod("getName");
			tableGetRecordCountMethod = tableClass.getMethod("getRecordCount");
			tableGetRecordAtMethod = tableClass.getMethod("getRecordAt", Integer.TYPE);
			recordGetTypedValueMethod = recordClass.getMethod("getTypedValue", String.class);
			fieldGetTypeMethod = fieldClass.getMethod("getType");
			fieldGetLengthMethod = fieldClass.getMethod("getLength");
		}
		catch (Exception e)
		{
			throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
		}
		try
		{
			Constructor<?> tableConstructor;
			if (charset != null)
			{
				tableConstructor = tableClass.getConstructor(File.class, String.class);
				table = tableConstructor.newInstance(new File(path), charset);
			}
			else
			{
				tableConstructor = tableClass.getConstructor(File.class);
				table = tableConstructor.newInstance(new File(path));
			}
			tableOpenMethod.invoke(table);
			fields = (List) tableGetFieldsMethod.invoke(table, new Object[] {});
			recordCount = (Integer)tableGetRecordCountMethod.invoke(table, new Object[] {});
			record = null;
			rowNo = -1;
			this.upperTableName = tableName.toUpperCase();
			this.tableAlias = tableAlias;
		}
		catch (Exception e)
		{
			throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
		}
		dbfTypeToSQLType = new HashMap<>();
		dbfTypeToSQLType.put("CHARACTER", "String");
		dbfTypeToSQLType.put("NUMBER", "Double");
		dbfTypeToSQLType.put("LOGICAL", "Boolean");
		dbfTypeToSQLType.put("DATE", "Date");
		dbfTypeToSQLType.put("MEMO", "String");
		dbfTypeToSQLType.put("FLOAT", "Double");
	}

	@Override
	public void close() throws SQLException
	{
		if (table != null)
		{
			try
			{
				tableCloseMethod.invoke(table);
			}
			catch (Exception e)
			{
				throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
			}
		}
		table = null;
	}

	@Override
	public String[] getColumnNames() throws SQLException
	{
		if (columnNames == null)
		{
			int columnCount = fields.size();
			String[] result = new String[columnCount];
			for (int i = 0; i < columnCount; i++)
			{
				Object field = fields.get(i);
				try
				{
					result[i] = (String) fieldGetNameMethod.invoke(field, new Object[] {});
				}
				catch (Exception e)
				{
					throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
				}
			}
			columnNames = result;
		}
		return columnNames;
	}

	private Object getField(int i) throws SQLException
	{
		try
		{
			String []fieldNames = getColumnNames();
			String fieldName = fieldNames[i];
			Object result = recordGetTypedValueMethod.invoke(record, fieldName);
			if(result instanceof String)
				result = ((String) result).trim();
			else if (result instanceof java.util.Date)
				result = new java.sql.Date(((java.util.Date)result).getTime());
			return result;
		}
		catch (Exception e)
		{
			throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
		}
	}

	@Override
	public boolean next() throws SQLException
	{
		rowNo++;

		if (rowNo >= recordCount.intValue())
			return false;

		try
		{
			record = tableGetRecordAtMethod.invoke(table, Integer.valueOf(rowNo));
		}
		catch (InvocationTargetException e)
		{
			/*
			 * Re-throw the exception from inside the dans library.
			 */
			throw new SQLException(e.getCause());
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String[] getColumnTypes() throws SQLException
	{
		String[] result = new String[fields.size()];
		for (int i = 0; i < fields.size(); i++)
		{
			String dbfType = "";
			try
			{
				dbfType = fieldGetTypeMethod.invoke(fields.get(i)).toString();
			}
			catch (Exception e)
			{
				throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
			}
			result[i] = dbfTypeToSQLType.get(dbfType);
			if (result[i] == null)
				throw new SQLException(CsvResources.getString("dbfTypeNotSupported") + ": " + dbfType);
		}
		return result;
	}

	@Override
	public int[] getColumnSizes() throws SQLException
	{
		int[] result = new int[fields.size()];
		for (int i = 0; i < fields.size(); i++)
		{
			try
			{
				Object fieldLength = fieldGetLengthMethod.invoke(fields.get(i));
				result[i] = ((Number)fieldLength).intValue();
			}
			catch (Exception e)
			{
				throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
			}
		}
		return result;
	}

	@Override
	public Map<String, Object> getEnvironment() throws SQLException
	{
		int initialSize = fields.size() * 2;
		if (tableAlias != null)
			initialSize += fields.size();
		if (initialSize == 0)
			initialSize = 1;

		Map<String, Object> result = new MinimumMemoryMap<>(initialSize);
		for (int i = 0; i < fields.size(); i++)
		{
			Object field = fields.get(i);
			try
			{
				String fieldName = (String) fieldGetNameMethod.invoke(field, new Object[] {});
				Object o = getField(i);

				/*
				 * Convert column names to upper case because
				 * that is what query environment uses.
				 */
				fieldName = fieldName.toUpperCase();
				result.put(fieldName, o);
				result.put(upperTableName + "." + fieldName, o);
				if (tableAlias != null)
				{
					/*
					 * Also allow field value to be accessed as S.ID  if table alias S is set.
					 */
					result.put(tableAlias + "." + fieldName, o);
				}
			}
			catch (Exception e)
			{
				throw new SQLException(CsvResources.getString("dansDbfError") + ": " + e);
			}
		}
		return result;
	}

	@Override
	public String getTableAlias()
	{
		return tableAlias;
	}
}
