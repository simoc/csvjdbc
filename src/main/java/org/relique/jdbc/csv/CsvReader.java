/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.relique.jdbc.csv;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.relique.io.DataReader;

public class CsvReader extends DataReader
{
	private String headerline;
	private CsvRawReader rawReader;
	private int transposedLines;
	private int transposedFieldsToSkip;
	private String[] columnNames;
	private String[] aliasedColumnNames;
	private String[] columnTypes;
	private Vector<String[]> firstTable;
	private int joiningValueNo;
	private int valuesToJoin;
	private String[] joiningValues;
	private StringConverter converter;
	private String[] fieldValues;

	public CsvReader(CsvRawReader rawReader, int transposedLines,
		int transposedFieldsToSkip, String headerline) throws SQLException
	{
		super();

		this.rawReader = rawReader;
		this.transposedLines = transposedLines;
		this.transposedFieldsToSkip = transposedFieldsToSkip;
		this.headerline = headerline;
		this.columnNames = rawReader.parseLine(headerline, true);
		this.firstTable = null;
		columnTypes = null;

		if (!this.isPlainReader())
		{
			firstTable = new Vector<String[]>();
			joiningValueNo = 0;
			joiningValues = null;
			try
			{
				String[] values = null;
				for (int i = 0; i < transposedLines; i++)
				{
					String line;
					line = rawReader.getNextDataLine();
					values = rawReader.parseLine(line, false);
					firstTable.add(values);
				}
				valuesToJoin = values.length;
				fieldValues = new String[columnNames.length];
			}
			catch (IOException e)
			{
				e.printStackTrace();
				throw new SQLException("" + e);
			}
		}
	}

	public void setConverter(StringConverter converter)
	{
		this.converter = converter;
	}

	public int getTransposedFieldsToSkip()
	{
		return transposedFieldsToSkip;
	}

	public int getTransposedLines()
	{
		return transposedLines;
	}

	public String getHeaderline()
	{
		return headerline;
	}

	boolean isPlainReader()
	{
		return transposedLines == 0 && transposedFieldsToSkip == 0;
	}

	@Override
	public boolean next() throws SQLException
	{
		if (this.isPlainReader())
		{
			boolean result = rawReader.next();
			fieldValues = rawReader.getFieldValues();
			return result;
		}
		else
		{
			if (joiningValues == null ||
				joiningValueNo + getTransposedFieldsToSkip() == valuesToJoin)
			{
				String line;
				try
				{
					line = rawReader.getNextDataLine();
				}
				catch (IOException e)
				{
					throw new SQLException("" + e);
				}
				if (line == null)
					return false;
				joiningValues = rawReader.parseLine(line, false);
				joiningValueNo = 0;
			}
			for (int i = 0; i < transposedLines; i++)
			{
				fieldValues[i] = firstTable.get(i)[joiningValueNo
					+ getTransposedFieldsToSkip()];
			}
			for (int i = transposedLines; i < columnNames.length - 1; i++)
			{
				fieldValues[i] = joiningValues[i - transposedLines];
			}
			fieldValues[columnNames.length - 1] =
				joiningValues[columnNames.length - transposedLines - 1 + joiningValueNo];
			joiningValueNo++;
			if (columnTypes == null)
				getColumnTypes();
			return true;
		}
	}

	@Override
	public String[] getColumnNames()
	{
		if (isPlainReader())
			return rawReader.getColumnNames();
		else
			return columnNames;
	}

	public String[] getAliasedColumnNames()
	{
		if (this.aliasedColumnNames == null)
		{
			String tableAlias = rawReader.getTableAlias();
			if (tableAlias != null)
			{
				/*
				 * Create array of "T.ID" column aliases that we can use for
				 * every row.
				 */
				String[] columnNames = getColumnNames();
				this.aliasedColumnNames = new String[columnNames.length];
				for (int i = 0; i < columnNames.length; i++)
				{
					this.aliasedColumnNames[i] = tableAlias + "." +
						columnNames[i].toUpperCase();
				}
			}
		}
		return this.aliasedColumnNames;
	}

	@Override
	public Object getField(int i) throws SQLException
	{
		if (isPlainReader())
			return rawReader.getField(i);
		else
			return null;
	}

	@Override
	public void close()
	{
		rawReader.close();
	}

	@Override
	public Map<String, Object> getEnvironment() throws SQLException
	{

		if (fieldValues.length != getColumnNames().length)
		{
			throw new SQLException(CsvResources.getString("wrongColumnCount") + ": " +
				fieldValues.length + " " + getColumnNames().length);
		}
		if (columnTypes == null)
			getColumnTypes();
		String[] columnNames = getColumnNames();
		String[] columnAliases = getAliasedColumnNames();

		int nKeys = 1 + columnNames.length;
		if (columnAliases != null)
			nKeys += columnAliases.length;
		Map<String, Object> result = new HashMap<String, Object>(nKeys, 1);
		result.put("@STRINGCONVERTER", converter);

		for (int i = 0; i < columnNames.length; i++)
		{
			String key = columnNames[i].toUpperCase();
			Object value = converter.convert(columnTypes[i], fieldValues[i]);
			result.put(key, value);
			if (columnAliases != null)
			{
				/*
				 * Also allow column value to be accessed as S.ID if table alias
				 * S is set.
				 */
				result.put(columnAliases[i], value);
			}

		}
		return result;
	}

	public void setColumnTypes(String line) throws SQLException
	{
		String[] typeNamesLoc = line.split(",");
		if (typeNamesLoc.length == 0)
			throw new SQLException(CsvResources.getString("invalidColumnType") + ": " + line);
		columnTypes = new String[getColumnNames().length];
		for (int i = 0; i < Math.min(typeNamesLoc.length, columnTypes.length); i++)
		{
			String typeName = typeNamesLoc[i].trim();
			if (converter.forSQLName(typeName) == null)
				throw new SQLException(CsvResources.getString("invalidColumnType") + ": " + typeName);
			columnTypes[i] = typeName;
		}

		/*
		 * Use last column type for any remaining columns.
		 */
		for (int i = typeNamesLoc.length; i < columnTypes.length; i++)
		{
			columnTypes[i] = typeNamesLoc[typeNamesLoc.length - 1].trim();
		}
	}

	@Override
	public String[] getColumnTypes() throws SQLException
	{
		if (columnTypes == null)
		{
			inferColumnTypes();
		}
		return columnTypes;
	}

	private void inferColumnTypes() throws SQLException
	{
		if (fieldValues == null)
			throw new SQLException(CsvResources.getString("cannotInferColumns"));

		columnTypes = new String[fieldValues.length];
		for (int i = 0; i < fieldValues.length; i++)
		{
			try
			{
				String typeName = "String";
				String value = getField(i).toString();
				if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))
				{
					typeName = "Boolean";
				}
				else if (value.equals(("" + converter.parseInt(value))))
				{
					typeName = "Int";
				}
				else if (value.equals(("" + converter.parseLong(value))))
				{
					typeName = "Long";
				}
				else if (value.equals(("" + converter.parseDouble(value))))
				{
					typeName = "Double";
				}
				else if (value.equals(("" + converter.parseBytes(value))))
				{
					typeName = "Bytes";
				}
				else if (value.equals(("" + converter.parseBigDecimal(value))))
				{
					typeName = "BigDecimal";
				}
				else if (converter.parseTimestamp(value) != null)
				{
					typeName = "Timestamp";
				}
				else if (value.equals(("" + converter.parseDate(value) + "          ").substring(0, 10)))
				{
					typeName = "Date";
				}
				else if (value.equals(("" + converter.parseTime(value) + "        ").substring(0, 8)))
				{
					typeName = "Time";
				}
				else if (value.equals(("" + converter.parseAsciiStream(value))))
				{
					typeName = "AsciiStream";
				}
				columnTypes[i] = typeName;
			}
			catch (SQLException e)
			{
			}
		}
	}

	@Override
	public int[] getColumnSizes()
	{
		return rawReader.getColumnSizes();
	}

	@Override
	public String getTableAlias()
	{
		return rawReader.getTableAlias();
	}
}
