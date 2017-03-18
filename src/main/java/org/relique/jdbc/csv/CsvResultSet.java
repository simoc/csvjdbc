/*
 *	CsvJdbc - a JDBC driver for CSV files
 *	Copyright (C) 2001	Jonathan Ackerman
 *
 *	This library is free software; you can redistribute it and/or
 *	modify it under the terms of the GNU Lesser General Public
 *	License as published by the Free Software Foundation; either
 *	version 2.1 of the License, or (at your option) any later version.
 *
 *	This library is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *	Lesser General Public License for more details.
 *
 *	You should have received a copy of the GNU Lesser General Public
 *	License along with this library; if not, write to the Free Software
 *	Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.relique.jdbc.csv;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.relique.io.DataReader;

/**
 * This class implements the java.sql.ResultSet JDBC interface for the
 * CsvJdbc driver.
 *
 * @author	   Jonathan Ackerman
 * @author	   Michael Maraya
 * @author	   Tomasz Skutnik
 * @author	   Chetan Gupta
 */
public class CsvResultSet implements ResultSet
{
	/** Metadata for this ResultSet */
	private ResultSetMetaData resultSetMetaData;

	/** Statement that produced this ResultSet */
	private CsvStatement statement;

	private int resultSetType = ResultSet.TYPE_SCROLL_SENSITIVE;
	
	/** Helper class that performs the actual file reads */
	private DataReader reader;

	/** Table referenced by the Statement */
	private String tableName;

	/** Last column name index read */
	private int lastIndexRead = -1;

	private LogicalExpression whereClause;

	private List<Expression> groupByColumns;
	
	private List<Expression> distinctColumns;

	private LogicalExpression havingClause;

	private List<Object []> orderByColumns;

	private List<Object []> queryEnvironment;

	private List<AggregateFunction> aggregateFunctions;

	private Set<ArrayList<Object>> distinctValues;

	private Map<String, Object> parentObjectEnvironment;

	private Map<String, Object> recordEnvironment;

	private List<String> usedColumns;

	private StringConverter converter;

	private ArrayList<Map<String, Object>> bufferedRecordEnvironments = null;

	private int currentRow;

	private boolean hitTail = false;

	/** Result of last call to next() */
	private boolean nextResult = true;

	private int maxRows;
	
	private int fetchSize;

	private int fetchDirection;

	private int limit;

	private boolean isClosed = false;

	/**
	 * Wrapper for any exception thrown by OrderByComparator.
	 */
	public class OrderByException extends RuntimeException
	{	
		public OrderByException(String message)
		{
			super(message);
		}
	}

	/**
	 * Compares SQL ORDER BY expressions for two records.
	 */
	public class OrderByComparator implements Comparator<Map<String, Object>>
	{
		public int compare(Map<String, Object> recordEnvironment1, Map<String, Object> recordEnvironment2)
		{
			int retval = 0;
			
			try
			{
				int i = 0;
				while (i < orderByColumns.size() && retval == 0)
				{
					Object []o = orderByColumns.get(i);
					Integer direction = (Integer)o[0];
					Expression expr = (Expression)o[1];
					recordEnvironment = recordEnvironment1;
					Map<String, Object> objectEnvironment1 = updateRecordEnvironment(true);
					if (converter != null)
						objectEnvironment1.put(StringConverter.COLUMN_NAME, converter);
					Comparable<Object> result1 = (Comparable<Object>)expr.eval(objectEnvironment1);
					recordEnvironment = recordEnvironment2;
					Map<String, Object> objectEnvironment2 = updateRecordEnvironment(true);
					if (converter != null)
						objectEnvironment2.put(StringConverter.COLUMN_NAME, converter);
					Comparable<Object> result2 = (Comparable<Object>)expr.eval(objectEnvironment2);
					if (result1 == null)
					{
						if (result2 == null)
							retval = 0;
						else
							retval = -1;
					}
					else if (result2 == null)
					{
						retval = 1;
					}
					else
					{
						retval = result1.compareTo(result2);
					}
					if (direction.intValue() < 0)
						retval = -retval;
					i++;
				}
			}
			catch (SQLException e)
			{
				throw new OrderByException(e.getMessage());
			}
			return retval;
		}
	}

	/**
	 * Constructor for the CsvResultSet object 
	 *
	 * @param statement Statement that produced this ResultSet
	 * @param reader Helper class that performs the actual file reads
	 * @param tableName Table referenced by the Statement
	 * @param queryEnvironment each query expression in the Statement.
	 * @param isDistinct true if Statement contains DISTINCT keyword.
	 * @param resultSetType indicates whether ResultSet be scrollable.
	 * @param whereClause expression for the SQL where clause.
	 * @param groupByColumns expressions for SQL GROUP BY clause.
	 * @param havingClause expression for SQL HAVING clause.s
	 * @param orderByColumns expressions for SQL ORDER BY clause.
	 * @param sqlLimit maximum number of rows set with SQL LIMIT clause.
	 * @param sqlOffset number of rows to skip with SQL OFFSET clause.
	 * @param columnTypes A comma-separated string specifying the type of the i-th column of the database table (not of the result).
	 * @param skipLeadingLines number of lines to skip at start of file.
	 * @param parentObjectEnvironment environment for current row in parent of sub-queries.
	 * @throws ClassNotFoundException in case the typed columns fail.
	 * @throws SQLException if executing the SQL statement fails.
	 */
	protected CsvResultSet(CsvStatement statement,
			DataReader reader,
			String tableName,
			List<Object []> queryEnvironment,
			boolean isDistinct,
			int resultSetType, 
			LogicalExpression whereClause,
			List<Expression> groupByColumns,
			LogicalExpression havingClause,
			List<Object []> orderByColumns,
			int sqlLimit,
			int sqlOffset,
			String columnTypes,
			int skipLeadingLines,
			Map<String, Object> parentObjectEnvironment) throws ClassNotFoundException, SQLException
	{
		this.statement = statement;
		maxRows = statement.getMaxRows();
		fetchSize = statement.getFetchSize();
		fetchDirection = statement.getFetchDirection();
		this.limit = sqlLimit;
		this.resultSetType = resultSetType;
		this.reader = reader;
		this.tableName = tableName;
		this.queryEnvironment = new ArrayList<Object []>(queryEnvironment);
		this.aggregateFunctions = new ArrayList<AggregateFunction>();
		this.whereClause = whereClause;
		if (groupByColumns != null)
			this.groupByColumns = new ArrayList<Expression>(groupByColumns);
		else
			this.groupByColumns = null;
		this.havingClause = havingClause;
		if (orderByColumns != null)
			this.orderByColumns = new ArrayList<Object []>(orderByColumns);
		else
			this.orderByColumns = null;
		if (isDistinct)
			this.distinctValues = new HashSet<ArrayList<Object>>();
		this.parentObjectEnvironment = parentObjectEnvironment;

		String timeFormat = ((CsvConnection)statement.getConnection()).getTimeFormat();
		String dateFormat = ((CsvConnection)statement.getConnection()).getDateFormat();
		String timestampFormat = ((CsvConnection)statement.getConnection()).getTimestampFormat();
		String timeZone = ((CsvConnection)statement.getConnection()).getTimeZoneName();
		Locale locale = ((CsvConnection)statement.getConnection()).getLocale();
		this.converter = new StringConverter(dateFormat, timeFormat, timestampFormat, timeZone, locale);
		if (reader instanceof CsvReader)
		{
			((CsvReader) reader).setConverter(converter);
			if(!"".equals(columnTypes))
				((CsvReader) reader).setColumnTypes(columnTypes);
		}

		String[] columnNames = reader.getColumnNames();

		String tableAlias = reader.getTableAlias();
		HashSet<String> allReaderColumns = new HashSet<String>();
		for (int i = 0; i < columnNames.length; i++)
		{
			String columnName = columnNames[i].toUpperCase();
			allReaderColumns.add(columnName);
			if (tableName != null)
				allReaderColumns.add(tableName.toUpperCase() + "." + columnName);
			if (tableAlias != null)
				allReaderColumns.add(tableAlias + "." + columnName);
		}

		if (whereClause!= null)
			this.usedColumns = new LinkedList<String>(whereClause.usedColumns(allReaderColumns));
		else
			this.usedColumns = new LinkedList<String>();

		if (!(this.resultSetType == TYPE_FORWARD_ONLY || this.resultSetType == TYPE_SCROLL_INSENSITIVE ||
			this.resultSetType == TYPE_SCROLL_SENSITIVE))
		{
			throw new SQLException(CsvResources.getString("invalidResultSetType") + ": " + this.resultSetType);
		}

		/*
		 * Replace any "select *" with the list of column names in that table.
		 */
		for (int i = 0; i < this.queryEnvironment.size(); i++)
		{
			Object[] o = this.queryEnvironment.get(i);
			if (o[1] instanceof AsteriskExpression)
			{
				AsteriskExpression asteriskExpression = (AsteriskExpression)o[1];
				
				/*
				 * Check that any table alias is valid.
				 */
				String asterisk = asteriskExpression.toString();
				if (!(asterisk.equals("*") || (tableAlias != null && asterisk.equalsIgnoreCase(tableAlias + ".*"))))
					throw new SQLException(CsvResources.getString("invalidColumnName") + ": " + asterisk);
				this.queryEnvironment.remove(i);
				for (int j = 0; j < columnNames.length; j++)
				{
					this.queryEnvironment.add(i + j, new Object[]{columnNames[j], new ColumnName(columnNames[j])});
				}
			}
		}

		/*
		 * Replace any "group by 2" with the 2nd column in the query list.
		 */
		if (this.groupByColumns != null)
		{
			for (int i = 0; i < this.groupByColumns.size(); i++)
			{
				Expression expression = this.groupByColumns.get(i);
				if (expression instanceof NumericConstant)
				{
					NumericConstant n = (NumericConstant)expression;
					if (!(n.value instanceof Integer))
						throw new SQLException(CsvResources.getString("invalidGroupBy") + ": " + n);
					int index = n.value.intValue();

					/*
					 * Column numbering in SQL starts at 1, not 0.
					 */
					index--;

					if (index < 0 || index >= this.queryEnvironment.size())
					{
						throw new SQLException(CsvResources.getString("invalidGroupBy") + ": " + (index + 1));
					}
					Object[] q = this.queryEnvironment.get(index);
					this.groupByColumns.set(i, (Expression)q[1]);
				}
			}
		}

		if (this.groupByColumns != null)
		{
			for (Expression expr : this.groupByColumns)
			{
				this.usedColumns.addAll(expr.usedColumns(allReaderColumns));
			}
			if (havingClause!= null)
				this.usedColumns.addAll(havingClause.usedColumns(allReaderColumns));
		}

		/*
		 * Replace any "order by 2" with the 2nd column in the query list.
		 */
		if (this.orderByColumns != null)
		{
			for (Object []o : this.orderByColumns)
			{
				Expression expression = (Expression)o[1];
				if (expression instanceof NumericConstant)
				{
					NumericConstant n = (NumericConstant)expression;
					if (!(n.value instanceof Integer))
						throw new SQLException(CsvResources.getString("invalidOrderBy") + ": " + n);
					int index = n.value.intValue();
					
					/*
					 * Column numbering in SQL starts at 1, not 0.
					 */
					index--;

					if (index < 0 || index >= this.queryEnvironment.size())
					{
						throw new SQLException(CsvResources.getString("invalidOrderBy") + ": " + (index + 1));
					}
					Object[] q = this.queryEnvironment.get(index);
					o[1] = q[1];
				}
			}
		}

		if (this.orderByColumns != null)
		{
			for (Object []o : this.orderByColumns)
			{
				Expression expr = (Expression)o[1];
				this.usedColumns.addAll(expr.usedColumns(allReaderColumns));
			}
		}

		/*
		 * Find any SQL aggregate functions so they can be evaluated separately.
		 */
		for (int i = 0; i < this.queryEnvironment.size(); i++)
		{
			Object[] o = this.queryEnvironment.get(i);
			Expression expr = (Expression)o[1];

			/*
			 * Exclude aggregate functions in subquery as they do not affect this query.
			 */
			if (!(expr instanceof SubQueryExpression))
			{
				List<AggregateFunction> exprAggregateFunctions = expr.aggregateFunctions();
				this.aggregateFunctions.addAll(exprAggregateFunctions);
				for (AggregateFunction aggregateFunction : exprAggregateFunctions)
				{
					this.usedColumns.addAll(aggregateFunction.aggregateColumns(allReaderColumns));
				}
			}
		}

		if (aggregateFunctions.size() > 0 && this.groupByColumns == null)
		{
			/*
			 * Check there is no mix of query columns and aggregate functions.
			 */
			List<String> allUsedColumns = new LinkedList<String>();
			for (int i = 0; i < this.queryEnvironment.size(); i++)
			{
				Object[] o = this.queryEnvironment.get(i);
				if (o[1] != null)
				{
					allUsedColumns.addAll(((Expression)o[1]).usedColumns(allReaderColumns));
				}
			}
			if (allUsedColumns.size() > 0 && aggregateFunctions.size() > 0)
				throw new SQLException(CsvResources.getString("columnsWithAggregateFunctions"));
		}
		if (whereClause != null && whereClause.aggregateFunctions().size() > 0)
			throw new SQLException(CsvResources.getString("noAggregateFunctions"));

		if (!((CsvConnection)statement.getConnection()).isIndexedFiles())
		{
			//TODO no check when indexedFiles=true because unit test TestCsvDriver.testFromNonExistingIndexedTable then fails.
			/*
			 * Check that each selected expression is valid, using only column names contained in the table.
			 */
			for (int i = 0; i < this.queryEnvironment.size(); i++)
			{
				Object[] o = this.queryEnvironment.get(i);
				if (o[1] != null)
				{
					Expression expr = (Expression)o[1];
					if (expr instanceof SubQueryExpression)
					{
						/*
						 * Any invalid column names will be caught in the sub-query.
						 */
					}
					else
					{
						List<String> exprUsedColumns = expr.usedColumns(allReaderColumns);
						for (Object usedColumn : exprUsedColumns)
						{
							if (!allReaderColumns.contains(usedColumn))
								throw new SQLException(CsvResources.getString("invalidColumnName") + ": " + usedColumn);
						}
					}
				}
				//TODO selected column aliases are allowed in WHERE clause (although this is invalid SQL) and unit tested in TestCsvDriver.testFieldAsAlias so add all aliases to list too.
				allReaderColumns.add(o[0].toString());
			}
		}
		
		/*
		 * Check that all columns used in the WHERE, GROUP BY, HAVING
		 * and ORDER BY clauses do exist in the table.
		 */
		if (!((CsvConnection)statement.getConnection()).isIndexedFiles())
		{
			for (String usedColumn : this.usedColumns)
			{
				if (allReaderColumns.contains(usedColumn) == false &&
						parentObjectEnvironment.containsKey(usedColumn) == false)
				{
					throw new SQLException(CsvResources.getString("invalidColumnName") + ": " + usedColumn);
				}
			}

			checkGroupBy(allReaderColumns);

			if (this.orderByColumns != null)
			{
				for (Object []o : this.orderByColumns)
				{
					Expression expr = (Expression)o[1];
					List<String> exprUsedColumns = new LinkedList<String>(expr.usedColumns(allReaderColumns));
					if (expr instanceof SubQueryExpression)
					{
						/*
						 * Differentiate which columns are from this table and which
						 * columns are from the subquery table.
						 * 
						 * Go through all the columns and only add the ones that are defined
						 * in this table.
						 */
						List<String> copy = new LinkedList<String>();
						for (String usedColumn : exprUsedColumns)
						{
							if (allReaderColumns.contains(usedColumn.toUpperCase()))
								copy.add(usedColumn);
						}
						exprUsedColumns = copy;
					}

					for (String usedColumn : exprUsedColumns)
					{
						if (allReaderColumns.contains(usedColumn) == false &&
							parentObjectEnvironment.containsKey(usedColumn) == false)
						{
							throw new SQLException(CsvResources.getString("invalidColumnName") + ": " + usedColumn);
						}
					}

					if (exprUsedColumns.isEmpty())
					{
						/*
						 * Must order by something that contains at least one column, not 'foo' or 1+1.
						 */
						throw new SQLException(CsvResources.getString("invalidOrderBy") + ": " + expr.toString());
					}
				}
			}
		}

		if (this.distinctValues != null)
		{
			for (int i = 0; i < this.queryEnvironment.size(); i++)
			{
				Object[] o = this.queryEnvironment.get(i);
				Expression expr = (Expression)o[1];
				this.usedColumns.addAll(expr.usedColumns(allReaderColumns));
			}
		}

		if (this.groupByColumns != null ||
		this.orderByColumns != null || this.aggregateFunctions.size() > 0 ||
			isScrollable())
		{
			bufferedRecordEnvironments = new ArrayList<Map<String, Object>>();
			currentRow = 0;
		}

		if (this.groupByColumns != null)
		{
			/*
			 * Read all rows and group them together based on GROUP BY expressions.
			 */
			int savedMaxRows = maxRows;
			int savedLimit = limit;
			maxRows = 0;
			limit = -1;
			ArrayList<ArrayList<Object>> groupOrder = new ArrayList<ArrayList<Object>>();
			Map<ArrayList<Object>, ArrayList<Map<String, Object>>> groups = new MinimumMemoryMap<ArrayList<Object>, ArrayList<Map<String, Object>>>();
			try
			{
				while (next())
				{
					Map<String, Object> objectEnvironment = updateRecordEnvironment(true);
					if (converter != null)
						objectEnvironment.put(StringConverter.COLUMN_NAME, converter);
					ArrayList<Object> groupByKeys = new ArrayList<Object>(this.groupByColumns.size());
					for (Expression expr : this.groupByColumns)
					{
						groupByKeys.add(expr.eval(objectEnvironment));
					}
					ArrayList<Map<String, Object>> groupByValues = groups.get(groupByKeys);
					if (groupByValues == null)
					{
						groupByValues = new ArrayList<Map<String, Object>>();
						groups.put(groupByKeys, groupByValues);
						groupOrder.add(groupByKeys);
					}
					groupByValues.add(recordEnvironment);
				}
				bufferedRecordEnvironments.clear();
				for (ArrayList<Object> groupByKey : groupOrder)
				{
					ArrayList<Map<String, Object>> values = groups.get(groupByKey);

					/*
					 * Create a row in the ResultSet for each group with a
					 * reference to all the rows in that group so we can
					 * later calculate any aggregate functions for each group.
					 */
					Map<String, Object> firstRow = new MinimumMemoryMap<String, Object>(values.get(0));
					firstRow.put(AggregateFunction.GROUPING_COLUMN_NAME, values);

					if (this.havingClause == null || Boolean.TRUE.equals(this.havingClause.isTrue(firstRow)))
						bufferedRecordEnvironments.add(firstRow);
				}

				if (this.orderByColumns != null)
				{
					sortRows(sqlOffset);
				}
			}
			finally
			{
				maxRows = savedMaxRows;
				limit = savedLimit;
			}

			/*
			 * Rewind back to before the row so we can read it.
			 */
			currentRow = 0;
			nextResult = true;
			recordEnvironment = null;
			updateRecordEnvironment(false);
			hitTail = true;

		}
		else if (this.aggregateFunctions.size() > 0)
		{
			/*
			 * Read all rows, evaluating the aggregate functions for each row to
			 * produce a single row result.
			 */
			int savedMaxRows = maxRows;
			int savedLimit = limit;
			maxRows = 0;
			limit = -1;
			try
			{
				while (next())
				{
					for (Object o : this.aggregateFunctions)
					{
						AggregateFunction func = (AggregateFunction)o;
						func.processRow(recordEnvironment);
					}
				}

				/*
				 * Create a single row ResultSet from the aggregate functions.
				 */
				bufferedRecordEnvironments.clear();
				if ((savedLimit < 0 || savedLimit > 0) && sqlOffset == 0)
					bufferedRecordEnvironments.add(new HashMap<String, Object>());
			}
			finally
			{
				maxRows = savedMaxRows;
				limit = savedLimit;
			}

			/*
			 * Rewind back to before the row so we can read it.
			 */
			currentRow = 0;
			nextResult = true;
			recordEnvironment = null;
			updateRecordEnvironment(false);
			hitTail = true;

		}
		else if (this.orderByColumns != null)
		{
			/*
			 * Read all rows into memory and sort them based on SQL ORDER BY expressions.
			 */
			int savedMaxRows = maxRows;
			int savedLimit = limit;
			maxRows = 0;
			limit = -1;
			try
			{
				while (next())
					;
			}
			finally
			{
				maxRows = savedMaxRows;
				limit = savedLimit;
			}
			sortRows(sqlOffset);

			/*
			 * Rewind back to before first row so we can now read them in sorted order.
			 */
			currentRow = 0;
			nextResult = true;
			recordEnvironment = null;
			updateRecordEnvironment(false);
		}
		else if (sqlOffset > 0)
		{
			int savedMaxRows = maxRows;
			int savedLimit = limit;
			maxRows = 0;
			limit = -1;

			/*
			 * Skip the first n rows.
			 */
			try
			{
				while (sqlOffset > 0)
				{
					if (!next())
						break;
					sqlOffset--;
				}
			}
			finally
			{
				maxRows = savedMaxRows;
				limit = savedLimit;
				currentRow = 0;
				nextResult = true;
				if (bufferedRecordEnvironments != null)
					bufferedRecordEnvironments.clear();
			}
		}
	}

	private boolean isScrollable()
	{
		return (this.resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE ||
			this.resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE);
	}

	/**
	 * Check that all selected and ORDER BY columns also appear in any GROUP BY clause. 
	 * @throws SQLException
	 */
	private void checkGroupBy(Set<String> allReaderColumns) throws SQLException
	{
		if (this.groupByColumns != null)
		{
			for (Expression expr : this.groupByColumns)
			{
				List<String> exprUsedColumns = expr.usedColumns(allReaderColumns);
				if (exprUsedColumns.isEmpty())
				{
					/*
					 * Must group by something that contains at least one column, not 'foo' or 1+1.
					 */
					throw new SQLException(CsvResources.getString("invalidGroupBy") + ": " + expr.toString());
				}
			}
			ArrayList<String> groupingColumns = new ArrayList<String>();
			for (Expression expr : this.groupByColumns)
			{
				groupingColumns.addAll(expr.usedColumns(allReaderColumns));
			}
			ArrayList<String> queryEnvironmentColumns = new ArrayList<String>();
			for (int i = 0; i < this.queryEnvironment.size(); i++)
			{
				Object[] o = this.queryEnvironment.get(i);
				queryEnvironmentColumns.add(o[0].toString());
				if (o[1] != null)
				{
					Expression expr = (Expression)o[1];
					for (Object o2 : expr.usedColumns(allReaderColumns))
					{
						queryEnvironmentColumns.add(o2.toString());
					}
				}
			}
			for (int i = 0; i < this.queryEnvironment.size(); i++)
			{
				Object[] o = this.queryEnvironment.get(i);
				if (!groupingColumns.contains(o[0]))
				{
					if (o[1] != null)
					{
						Expression expr = (Expression)o[1];
						for (Object o2 : expr.usedColumns(allReaderColumns))
						{
							String columnName = o2.toString();
							if (!groupingColumns.contains(columnName))
							{
								if (tableName == null || (!groupingColumns.contains(tableName.toUpperCase() + "." + columnName)))
								{
									String tableAlias = this.reader.getTableAlias();
									if (tableAlias == null || (!groupingColumns.contains(tableAlias + "." + columnName)))
									{
										/*
										 * GROUP BY must include all queried columns.
										 */
										throw new SQLException(CsvResources.getString("columnNotInGroupBy") + ": " + columnName);
									}
								}
							}
						}
					}
				}
			}
			if (this.havingClause != null)
			{
				for (String columnName : this.havingClause.usedColumns(allReaderColumns))
				{
					if (!queryEnvironmentColumns.contains(columnName))
					{
						throw new SQLException(CsvResources.getString("invalidHaving") + ": " + columnName);
					}
				}
			}
			if (this.orderByColumns != null)
			{
				for (Object []o : this.orderByColumns)
				{
					Expression expr = (Expression)o[1];
					for (Object o2 : expr.usedColumns(allReaderColumns))
					{
						if (!queryEnvironmentColumns.contains(o2.toString()))
							throw new SQLException(CsvResources.getString("orderByNotInGroupBy") + ": " + o2);
					}
				}
			}

			/*
			 * A query containing GROUP BY without any aggregate functions can be simplified
			 * to a SELECT DISTINCT, avoiding the need to load all records into memory.
			 */
			boolean hasAggregateFunctions = false;
			for (int i = 0; i < this.queryEnvironment.size(); i++)
			{
				Object[] o = this.queryEnvironment.get(i);
				Expression expr = (Expression)o[1];
				if (expr.aggregateFunctions().size() > 0)
					hasAggregateFunctions = true;
			}
			if (this.havingClause != null && this.havingClause.aggregateFunctions().size() > 0)
				hasAggregateFunctions = true;
			if (!hasAggregateFunctions)
			{
				this.distinctValues = new HashSet<ArrayList<Object>>();
				this.distinctColumns = new ArrayList<Expression>(this.groupByColumns);
				this.groupByColumns = null;
			}
		}
	}

	private void sortRows(int sqlOffset) throws SQLException
	{
		Map<String, Object> []allRows = new Map[bufferedRecordEnvironments.size()];
		for (int i = 0; i < allRows.length; i++)
			allRows[i] = bufferedRecordEnvironments.get(i);
		bufferedRecordEnvironments.clear();
		try
		{
			Arrays.sort(allRows, new OrderByComparator());
		}
		catch (OrderByException e)
		{
			throw new SQLException(e.getMessage());
		}
		int rowLimit = allRows.length;
		if (maxRows != 0 && maxRows < rowLimit)
			rowLimit = maxRows;
		if (limit >= 0 && sqlOffset + limit < rowLimit)
			rowLimit = sqlOffset + limit;

		for (int i = sqlOffset; i < rowLimit; i++)
			bufferedRecordEnvironments.add(allRows[i]);
	}

	private void checkOpen() throws SQLException
	{
		if (isClosed)
			throw new SQLException(CsvResources.getString("closedResultSet"));
	}

	private void checkTimeout() throws SQLTimeoutException
	{
		if (statement.getTimeoutMillis() != Long.MAX_VALUE)
		{
			if (System.currentTimeMillis() >= statement.getTimeoutMillis())
				throw new SQLTimeoutException();
		}
	}

	@Override
	public boolean next() throws SQLException
	{
		boolean thereWasAnAnswer;

		checkOpen();
		checkTimeout();

		/*
		 * Has query been cancelled by another thread?
		 */
		if (statement.isCancelled())
			throw new SQLException(CsvResources.getString("statementCancelled"));

		if ((this.groupByColumns != null ||
		this.aggregateFunctions.size() > 0 ||
			this.orderByColumns != null || isScrollable()) &&
			currentRow < bufferedRecordEnvironments.size())
		{
			currentRow++;
			recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
			updateRecordEnvironment(true);
			thereWasAnAnswer = true;
		}
		else
		{
			if(maxRows != 0 && currentRow >= maxRows)
			{
				// Do not fetch any more rows, we have reached the row limit set by caller.
				thereWasAnAnswer = false;
			}
			else if(limit >= 0 && currentRow >= limit)
			{
				thereWasAnAnswer = false;
			}
			else if(hitTail)
			{
				thereWasAnAnswer = false;
			}
			else
			{
				thereWasAnAnswer = reader.next();
			}

			if(thereWasAnAnswer)
			{
				recordEnvironment = reader.getEnvironment();
				recordEnvironment.put(CsvStatement.STATEMENT_COLUMN_NAME, statement);
			}
			else
			{
				recordEnvironment = null;
			}

			// We have a where clause or DISTINCT keyword, honor it
			if (whereClause != null || distinctValues != null)
			{
				Map<String, Object> objectEnvironment = updateRecordEnvironment(thereWasAnAnswer);
				while (thereWasAnAnswer)
				{
					if (whereClause == null || Boolean.TRUE.equals(whereClause.isTrue(objectEnvironment)))
					{
						/*
						 * Check HAVING clause if no aggregate functions in query and
						 * it is being processed just like SELECT DISTINCT.
						 * In this case HAVING is exactly the same as a WHERE clause.
						 */
						if (this.distinctColumns == null || this.havingClause == null || Boolean.TRUE.equals(this.havingClause.isTrue(objectEnvironment)))
						{
							if (distinctValues == null || addDistinctEnvironment(objectEnvironment))
							{
								break;
							}
						}
					}
					thereWasAnAnswer = reader.next();
					if(thereWasAnAnswer)
					{
						recordEnvironment = reader.getEnvironment();
						recordEnvironment.put(CsvStatement.STATEMENT_COLUMN_NAME, statement);
					}
					else
					{
						recordEnvironment = null;
					}
					objectEnvironment = updateRecordEnvironment(thereWasAnAnswer);
				}
			}
			if (this.orderByColumns != null || isScrollable())
			{
				if(thereWasAnAnswer)
				{
					Map<String, Object> env = reader.getEnvironment();
					env.put(CsvStatement.STATEMENT_COLUMN_NAME, statement);
					bufferedRecordEnvironments.add(env);
					currentRow++;
				}
				else
				{
					hitTail = true;
					currentRow = bufferedRecordEnvironments.size() + 1;
				}
			}
			else
			{
				if (thereWasAnAnswer)
					currentRow++;
				else
					hitTail = true;
			}
		}

		nextResult = thereWasAnAnswer;
		return thereWasAnAnswer;
	}

	private Map<String, Object> updateRecordEnvironment(boolean thereWasAnAnswer) throws SQLException
	{
		Map<String, Object> objectEnvironment = new MinimumMemoryMap<String, Object>();
		if(!thereWasAnAnswer)
		{
			recordEnvironment = null;
			return objectEnvironment;
		}

		/*
		 * Set any parent environment first so it is overridden by current environment.
		 */
		objectEnvironment.put(CsvStatement.STATEMENT_COLUMN_NAME, this.statement);
		objectEnvironment.putAll(this.parentObjectEnvironment);

		for (int i = 0; i < queryEnvironment.size(); i++)
		{
			Object[] o = queryEnvironment.get(i);
			String key = (String) o[0];
			Object value = ((Expression) o[1]).eval(recordEnvironment);
			objectEnvironment.put(key.toUpperCase(), value);
		}
		for (int i=0; i<usedColumns.size(); i++)
		{
			String key = usedColumns.get(i);
			key = key.toUpperCase();
			if (recordEnvironment.containsKey(key))
					objectEnvironment.put(key, recordEnvironment.get(key));
		}

		/*
		 * Always include any group of rows so we have assembled so we can evaluate
		 * any aggregate functions.
		 */
		String key = AggregateFunction.GROUPING_COLUMN_NAME;
		Object groupRows = recordEnvironment.get(key);
		if (groupRows != null)
			objectEnvironment.put(key, groupRows);

		/*
		 * Always include the data type converter object so we can correctly
		 * convert data types when evaluating expressions such as MYDATE > '2012-06-31'.
		 */
		key = StringConverter.COLUMN_NAME;
		Object stringConverter = recordEnvironment.get(key);
		if (stringConverter != null)
			objectEnvironment.put(key, stringConverter);

		/*
		 * Always include the java.sql.Statement object that user-defined
		 * SQL functions can refer back to.
		 */
		key = CsvStatement.STATEMENT_COLUMN_NAME;
		Object statement = recordEnvironment.get(key);
		if (statement != null)
			objectEnvironment.put(key, statement);

		return objectEnvironment;
	}

	private boolean addDistinctEnvironment(Map<String, Object> objectEnvironment) throws SQLException
	{
		boolean isDistinct;

		/*
		 * Create list of query values for this row, either for a simple
		 * GROUP BY statement, or for a SELECT DISTINCT.
		 */
		ArrayList<Object> environment;
		if (this.distinctColumns != null)
		{
			environment = new ArrayList<Object>(distinctColumns.size());
			for (int i = 0; i < distinctColumns.size(); i++)
			{
				Object value = distinctColumns.get(i).eval(objectEnvironment);
				environment.add(value);
			}
		}
		else
		{
			environment = new ArrayList<Object>(queryEnvironment.size());
			for (int i = 0; i < queryEnvironment.size(); i++)
			{
				Object[] o = queryEnvironment.get(i);
				Object value = ((Expression) o[1]).eval(objectEnvironment);
				environment.add(value);
			}
		}

		/*
		 * Has this list of values been read before for this query?
		 */
		isDistinct = distinctValues.add(environment);
		return isDistinct;
	}

	@Override
	public void close() throws SQLException
	{
		isClosed = true;
		reader.close();

		/*
		 * Ensure that long row lists for this query can now be GC'ed
		 * even if the application still has a reference to this ResultSet.
		 */
		distinctValues = null;
		parentObjectEnvironment = null;
		bufferedRecordEnvironments = null;
		recordEnvironment = null;
	}

	@Override
	public boolean wasNull() throws SQLException
	{
		if(lastIndexRead >= 0)
		{
			return getString(lastIndexRead) == null;
		}
		else
		{
			throw new SQLException(CsvResources.getString("noGetMethod"));
		}
	}

	//======================================================================
	// Methods for accessing results by column index
	//======================================================================

	@Override
	public String getString(int columnIndex) throws SQLException
	{
		Object o = getObject(columnIndex);
		if (o != null)
			return o.toString();
		return null;
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Boolean b = converter.parseBoolean(s);
			if (b != null)
				return b.booleanValue();
		}
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Byte b = converter.parseByte(s);
			if (b != null)
				return b.byteValue();
		}
		return 0;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Short n = converter.parseShort(s);
			if (n != null)
				return n.shortValue();
		}
		return 0;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Integer i = converter.parseInt(s);
			if (i != null)
				return i.intValue();
		}
		return 0;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Long l = converter.parseLong(s);
			if (l != null)
				return l.longValue();
		}
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Float f = converter.parseFloat(s);
			if (f != null)
				return f.floatValue();
		}
		return 0;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
		{
			Double d = converter.parseDouble(s);
			if (d != null)
				return d.doubleValue();
		}
		return 0;
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException
	{
		// let getBigDecimal(int) handle this for now
		return getBigDecimal(columnIndex);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
			return converter.parseBytes(s);
		return null;
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException
	{
		return (Date) getObject(columnIndex);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException
	{
		return (Time) getObject(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		Object o = getObject(columnIndex);
		if (o instanceof Date)
			o = new Timestamp(((Date)o).getTime());
		return (Timestamp) o;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		String s = getString(columnIndex);
		if (s != null)
			return converter.parseAsciiStream(s);
		return null;
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		// delegate to getAsciiStream(int)
		return getAsciiStream(columnIndex);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		// delegate to getAsciiStream(int)
		return getAsciiStream(columnIndex);
	}

	//======================================================================
	// Methods for accessing results by column name
	//======================================================================

	@Override
	public String getString(String columnName) throws SQLException
	{
		return getString(findColumn(columnName));
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException
	{
		return getBoolean(findColumn(columnName));
	}

	@Override
	public byte getByte(String columnName) throws SQLException
	{
		return getByte(findColumn(columnName));
	}

	@Override
	public short getShort(String columnName) throws SQLException
	{
		return getShort(findColumn(columnName));
	}

	@Override
	public int getInt(String columnName) throws SQLException
	{
		return getInt(findColumn(columnName));
	}

	@Override
	public long getLong(String columnName) throws SQLException
	{
		return getLong(findColumn(columnName));
	}

	@Override
	public float getFloat(String columnName) throws SQLException
	{
		return getFloat(findColumn(columnName));
	}

	@Override
	public double getDouble(String columnName) throws SQLException
	{
		return getDouble(findColumn(columnName));
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(String columnName, int scale)
			throws SQLException
	{
		return getBigDecimal(findColumn(columnName));
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException
	{
		return getBytes(findColumn(columnName));
	}

	@Override
	public Date getDate(String columnName) throws SQLException
	{
		return getDate(findColumn(columnName));
	}

	@Override
	public Time getTime(String columnName) throws SQLException
	{
		return getTime(findColumn(columnName));
	}

	@Override
	public Timestamp getTimestamp(String columnName) throws SQLException
	{
		return getTimestamp(findColumn(columnName));
	}

	@Override
	public InputStream getAsciiStream(String columnName) throws SQLException
	{
		return getAsciiStream(findColumn(columnName));
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(String columnName) throws SQLException
	{
		return getUnicodeStream(findColumn(columnName));
	}

	@Override
	public InputStream getBinaryStream(String columnName) throws SQLException
	{
		return getBinaryStream(findColumn(columnName));
	}

	//=====================================================================
	// Advanced features:
	//=====================================================================

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException
	{
	}

	@Override
	public String getCursorName() throws SQLException
	{
		checkOpen();

		throw new SQLFeatureNotSupportedException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getCursorName()");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		if (resultSetMetaData == null)
		{
			String[] readerTypeNames = reader.getColumnTypes(); 
			String[] readerColumnNames = reader.getColumnNames();
			int[] readerColumnSizes = reader.getColumnSizes();
			String tableAlias = reader.getTableAlias();
			int columnCount = queryEnvironment.size();
			String []columnNames = new String[columnCount];
			String []columnLabels = new String[columnCount];
			int []columnSizes = new int[columnCount];
			String []typeNames = new String[columnCount];

			/*
			 * Create a record containing dummy values.
			 */
			HashSet<String> allReaderColumns = new HashSet<String>();
			HashMap<String, Object> env = new HashMap<String, Object>();
			for(int i=0; i<readerTypeNames.length; i++)
			{
				Object literal = StringConverter.getLiteralForTypeName(readerTypeNames[i]);
				String columnName = readerColumnNames[i].toUpperCase();
				env.put(columnName, literal);
				allReaderColumns.add(columnName);
				if (tableName != null)
				{
					env.put(tableName.toUpperCase() + "." + columnName, literal);
					allReaderColumns.add(tableName.toUpperCase() + "." + columnName);
				}
				if (tableAlias != null)
				{
					env.put(tableAlias + "." + columnName, literal);
					allReaderColumns.add(tableAlias + "." + columnName);
				}
			}
			if (converter != null)
				env.put(StringConverter.COLUMN_NAME, converter);
			env.put(CsvStatement.STATEMENT_COLUMN_NAME, statement);

			for(int i=0; i<columnCount; i++)
			{
				Object[] o = queryEnvironment.get(i);
				columnNames[i] = (String)o[0];
				for (int j = 0; j < readerColumnNames.length; j++)
				{
					if (columnNames[i].equalsIgnoreCase(readerColumnNames[j]))
					{
						/*
						 * Use original case of column name in CSV file.
						 */
						columnNames[i] = readerColumnNames[j];
						break;
					}
				}
				columnLabels[i] = columnNames[i];

				/*
				 * Evaluate each expression to determine what data type it returns.
				 */
				Object result = null;
				try
				{
					Expression expr = ((Expression)o[1]);
					
					int columnSize = DataReader.DEFAULT_COLUMN_SIZE;
					if (expr instanceof ColumnName)
					{
						String usedColumn = expr.usedColumns(allReaderColumns).get(0);
						for (int k = 0; k < readerColumnNames.length; k++)
						{
							if (usedColumn.equalsIgnoreCase(readerColumnNames[k]))
							{
								columnSize = readerColumnSizes[k];
								break;
							}
						}
					}
					columnSizes[i] = columnSize;
					result = expr.eval(env);
				}
				catch (NullPointerException e)
				{
					/* Expression is invalid */
					// TODO: should we throw an SQLException here?
				}
				if (result != null)
					typeNames[i] = StringConverter.getTypeNameForLiteral(result);
				else
					typeNames[i] = "expression";
			}
			resultSetMetaData = new CsvResultSetMetaData(tableName, columnNames, columnLabels, typeNames,
				columnSizes);
		}
		return resultSetMetaData;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException
	{
		// perform pre-accessor method processing
		preAccessor(columnIndex);

		Object[] o = queryEnvironment.get(columnIndex-1);
		if (recordEnvironment != null)
			return ((Expression) o[1]).eval(recordEnvironment);
		return null;
	}

	@Override
	public Object getObject(String columnName) throws SQLException
	{
		return getObject(findColumn(columnName));
	}

	//--------------------------JDBC 2.0-----------------------------------

	//---------------------------------------------------------------------
	// Getters and Setters
	//---------------------------------------------------------------------

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException
	{
		String str = getString(columnIndex);
		return (str == null) ? null : new StringReader(str);
	}

	@Override
	public Reader getCharacterStream(String columnName) throws SQLException
	{
		String str = getString(columnName);
		return (str == null) ? null : new StringReader(str);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{
		BigDecimal retval = null;
		String str = getString(columnIndex);
		if(str != null)
		{
			try
			{
				retval = new BigDecimal(str);
			}
			catch (NumberFormatException e)
			{
				throw new SQLException(CsvResources.getString("cannotConvertToBigDecimal") +
					": " + str);
			}
		}
		return retval;
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException
	{
		return getBigDecimal(findColumn(columnName));
	}

	//---------------------------------------------------------------------
	// Traversal/Positioning
	//---------------------------------------------------------------------

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			return currentRow == 0;
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.isBeforeFirst()");
		}
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			return currentRow == bufferedRecordEnvironments.size() + 1;
		}
		else
		{
			/*
			 * If next() has already returned false then we are after the last record.
			 */
			return (this.nextResult == false && this.currentRow > 0);
		}
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			return currentRow == 1;
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.isFirst()");
		}
	}

	@Override
	public boolean isLast() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			if (!hitTail && currentRow != 0)
			{
				next();
				previous();
			}
			return (currentRow == bufferedRecordEnvironments.size());
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.isLast()");
		}
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			first();
			previous();
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.beforeFirst()");
		}
	}

	@Override
	public void afterLast() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			while(next());
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.afterLast()");
		}
	}

	@Override
	public boolean first() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			currentRow = 0;
			boolean thereWasAnAnswer = next();
			updateRecordEnvironment(thereWasAnAnswer);
			return thereWasAnAnswer;
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.first()");
		}
	}

	@Override
	public boolean last() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			afterLast();
			previous();
			return (this.bufferedRecordEnvironments.size() != 0);
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.last()");
		}
	}

	@Override
	public int getRow() throws SQLException
	{
		checkOpen();

		if (isScrollable() == false && nextResult == false)
			return 0;
		else
			return currentRow;
	}

	@Override
	public boolean absolute(int row) throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			boolean found;
			if(row < 0)
			{
				last();
				row = currentRow + row + 1;
			}
			else
			{
				// this is a no-op if we have already buffered enough lines.
				while((bufferedRecordEnvironments.size() < row) && next());
			}
			if (row <= 0)
			{
				found = false;
				currentRow = 0;
			}
			else if(row > bufferedRecordEnvironments.size())
			{
				found = false;
				currentRow = bufferedRecordEnvironments.size() + 1;
			}
			else
			{
				found = true;
				currentRow = row;
				recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
			}
			updateRecordEnvironment(found);
			return found;
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.absolute()");
		}
	}

	@Override
	public boolean relative(int rows) throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			if(currentRow + rows >= 0)
				return absolute(currentRow + rows);
			currentRow = 0;
			updateRecordEnvironment(false);
			return false;
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.relative()");
		}
	}

	@Override
	public boolean previous() throws SQLException
	{
		checkOpen();

		if (isScrollable())
		{
			if(currentRow > 1)
			{
				currentRow--;
				recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
				updateRecordEnvironment(true);
				return true;
			}
			else
			{
				currentRow = 0;
				recordEnvironment = null;
				updateRecordEnvironment(false);
				return false;
			}
		}
		else
		{
			throw new SQLException(CsvResources.getString("wrongResultSetType") + ": ResultSet.previous()");
		}
	}

	//---------------------------------------------------------------------
	// Properties
	//---------------------------------------------------------------------

	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		checkOpen();

		if (direction == ResultSet.FETCH_FORWARD ||
			direction == ResultSet.FETCH_REVERSE ||
			direction == ResultSet.FETCH_UNKNOWN)
		{
			this.fetchDirection = direction;
		}
		else
		{
			throw new SQLException(CsvResources.getString("unsupportedDirection") + ": " + direction);
		}
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		checkOpen();

		return fetchDirection;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException
	{
		fetchSize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		return fetchSize;
	}

	@Override
	public int getType() throws SQLException
	{
		return resultSetType;
	}

	@Override
	public int getConcurrency() throws SQLException
	{
		return CONCUR_READ_ONLY;
	}

	//---------------------------------------------------------------------
	// Updates
	//---------------------------------------------------------------------

	@Override
	public boolean rowUpdated() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.rowUpdated()");
	}

	@Override
	public boolean rowInserted() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.rowInserted()");
	}

	@Override
	public boolean rowDeleted() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.rowDeleted()");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateNull(int)");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBoolean(int, boolean)");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateByte(int, byte)");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateShort(int, short)");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateInt(int, int)");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateLong(int, long)");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateFloat(int, float)");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateDouble(int, double)");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBigDecimal(int, BigDecimal)");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateString(int, String)");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBytes(int, byte[])");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateDate(int, Date)");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateTime(int, Time)");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateTimestamp(int, Timestamp)");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateAsciiStream(int, InputStream, int)");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
		   throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBinaryStreamint, InputStream, int)");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateCharacterStream(int, Reader, int)");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scale)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.udpateObject(int, Object)");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateObject(int, Object, int)");
	}

	@Override
	public void updateNull(String columnName) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateNull(String)");
	}

	@Override
	public void updateBoolean(String columnName, boolean x)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBoolean(String, boolean)");
	}

	@Override
	public void updateByte(String columnName, byte x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateByte(String, byte)");
	}

	@Override
	public void updateShort(String columnName, short x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateShort(String, short)");
	}

	@Override
	public void updateInt(String columnName, int x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateInt(String, int)");
	}

	@Override
	public void updateLong(String columnName, long x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateLong(String, long)");
	}

	@Override
	public void updateFloat(String columnName, float x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateFloat(String, float)");
	}

	@Override
	public void updateDouble(String columnName, double x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateDouble(String, double)");
	}

	@Override
	public void updateBigDecimal(String columnName, BigDecimal x)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBigDecimal(String, BigDecimal)");
	}

	@Override
	public void updateString(String columnName, String x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateString(String, String)");
	}

	@Override
	public void updateBytes(String columnName, byte[] x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBytes(String, byte[])");
	}

	@Override
	public void updateDate(String columnName, Date x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateDate(String, Date)");
	}

	@Override
	public void updateTime(String columnName, Time x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateTime(String, Time)");
	}

	@Override
	public void updateTimestamp(String columnName, Timestamp x)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateTimestamp(String, Timestamp)");
	}

	@Override
	public void updateAsciiStream(String columnName, InputStream x, int length)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateAsciiStream(String, InputStream, int)");
	}

	@Override
	public void updateBinaryStream(String columnName, InputStream x, int length)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateBinaryStream(String, InputStream, int)");
	}

	@Override
	public void updateCharacterStream(String columnName, Reader reader,
			int length) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateCharacterStream(String, Reader, int)");
	}

	@Override
	public void updateObject(String columnName, Object x, int scale)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateObject(String, Object, int)");
	}

	@Override
	public void updateObject(String columnName, Object x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateObject(String, Object)");
	}

	@Override
	public void insertRow() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.insertRow()");
	}

	@Override
	public void updateRow() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.updateRow()");
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.deleteRow()");
	}

	@Override
	public void refreshRow() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.refreshRow()");
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.cancelRowUpdates()");
	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.moveToInsertRow()");
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.moveToeCurrentRow()");
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		return statement;
	}

	@Override
	public Object getObject(int i, Map<String,Class<?>> map) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getObject(int, Map)");
	}

	@Override
	public Ref getRef(int i) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getRef(int)");
	}

	@Override
	public Blob getBlob(int i) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getBlob(int)");
	}

	@Override
	public Clob getClob(int i) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getClob(int)");
	}

	@Override
	public Array getArray(int i) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getArray(int)");
	}

	@Override
	public Object getObject(String colName, Map<String,Class<?>> map) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getObject(String, Map)");
	}

	@Override
	public Ref getRef(String colName) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getRef(String)");
	}

	@Override
	public Blob getBlob(String colName) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getBlob(String)");
	}

	@Override
	public Clob getClob(String colName) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getClob(String)");
	}

	@Override
	public Array getArray(String colName) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getArray(String)");
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getDate(int, Calendar)");
	}

	@Override
	public Date getDate(String columnName, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getDate(String, Calendar)");
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getTime(int, Calendar)");
	}

	@Override
	public Time getTime(String columnName, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getTime(String, Calendar)");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getTimestamp(int, Calendar)");
	}

	@Override
	public Timestamp getTimestamp(String columnName, Calendar cal)
		   throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getTimestamp(String, Calendar)");
	}

	//---------------------------------------------------------------------
	// CSV JDBC private helper methods
	//---------------------------------------------------------------------

	/**
	 * Perform pre-accessor method processing
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @exception SQLException if a database access error occurs
	 */
	private void preAccessor(int columnIndex) throws SQLException
	{
		// set last read column index for wasNull()
		lastIndexRead = columnIndex;

		checkOpen();

		if (columnIndex < 1 || columnIndex > this.queryEnvironment.size())
		{
			throw new SQLException(CsvResources.getString("invalidColumnIndex") + ": "+columnIndex);
		}
		if (this.currentRow == 0)
		{
			// Cursor is still before first row.
			throw new SQLException(CsvResources.getString("noCurrentRow"));
		}
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.getURL(int)");
	}

	@Override
	public URL getURL(String columnName) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.getURL(String)");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateRef(int,java.sql.Ref)");
	}

	@Override
	public void updateRef(String columnName, Ref x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateRef(String,java.sql.Ref)");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateBlob(int,java.sql.Blob)");
	}

	@Override
	public void updateBlob(String columnName, Blob x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateBlob(String,java.sql.Blob)");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateClob(int,java.sql.Clob)");
	}

	@Override
	public void updateClob(String columnName, Clob x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateClob(String,java.sql.Clob)");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateArray(int,java.sql.Array)");
	}

	@Override
	public void updateArray(String columnName, Array x) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") + ": ResultSet.updateArray(String,java.sql.Array)");
	}
	
	@Override
	public int getHoldability() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String getNString(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String getNString(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean isClosed() throws SQLException
	{
		return isClosed;
	}
	
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNString(int columnIndex, String string)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNString(String columnLabel, String string)
			throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		checkOpen();

		if (columnLabel.equals(""))
			throw new SQLException(CsvResources.getString("invalidColumnName") + ": " + columnLabel);
		for (int i = 0; i < this.queryEnvironment.size(); i++)
		{
			Object[] queryEnvEntry = this.queryEnvironment.get(i);
			if(((String)queryEnvEntry[0]).equalsIgnoreCase(columnLabel))
				return i+1;
		}
		throw new SQLException(CsvResources.getString("invalidColumnName") + ": " + columnLabel);
	}

	@Override
	public NClob getNClob(int arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public NClob getNClob(String arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public RowId getRowId(int arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public RowId getRowId(String arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public SQLXML getSQLXML(int arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public SQLXML getSQLXML(String arg0) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void updateNClob(int arg0, NClob arg1) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateNClob(String arg0, NClob arg1) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateRowId(int arg0, RowId arg1) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateRowId(String arg0, RowId arg1) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException
	{
		// TODO Auto-generated method stub
	}
	
	@Override
	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException
	{
		// TODO Auto-generated method stub
	}

	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getObject(String, Class<T>)");
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
	{
		throw new UnsupportedOperationException(CsvResources.getString("methodNotSupported") +
			": ResultSet.getObject(int, Class<T>)");
	}
}

