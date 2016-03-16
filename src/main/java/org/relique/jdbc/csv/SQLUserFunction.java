/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2014  Simon Chenery
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

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SQLUserFunction extends Expression
{
	String name;
	List<Expression> expressions;

	public SQLUserFunction(String name, List<Expression> expressions)
	{
		this.name = name.toUpperCase();
		this.expressions = expressions;
	}

	public Object eval(Map<String, Object> env) throws SQLException
	{
		Object retval = null;
		CsvStatement statement = (CsvStatement)env.get(CsvStatement.STATEMENT_COLUMN_NAME);
		CsvConnection connection = (CsvConnection)statement.getConnection();
		Method method = connection.getSqlFunctions().get(name);

		if (method == null)
			throw new SQLException(CsvResources.getString("noFunction") + ": " + name);

		Class<?>[] parameterTypes = method.getParameterTypes();

		StringConverter converter = (StringConverter)env.get(StringConverter.COLUMN_NAME);

		boolean isVarArgs = method.isVarArgs();
		Object lastArg = null;
		if (isVarArgs && expressions.size() >= parameterTypes.length)
		{
			/*
			 * Strip '[]' from end of parameter class name.
			 */
			String parameterDataType = parameterTypes[parameterTypes.length - 1].getSimpleName();
			parameterDataType = parameterDataType.substring(0, parameterDataType.length() - 2);
			Class<?> parameterClass = getParameterClass(converter, parameterDataType);

			lastArg = Array.newInstance(parameterClass, expressions.size() - parameterTypes.length + 1);		
		}
		else if (parameterTypes.length != expressions.size())
		{
			throw new SQLException(CsvResources.getString("functionArgCount") + ": " + name);
		}

		Object []args = new Object[parameterTypes.length];
		String parameterDataType = null;
		try
		{
			int i = 0;
			int varArgsIndex = 0;
			for (Expression expr : expressions)
			{
				Object obj = expr.eval(env);
				if (obj != null)
				{
					/*
					 * Convert each expression to correct data type for Java method.
					 */
					parameterDataType = parameterTypes[i].getSimpleName();
					if (isVarArgs && i >= parameterTypes.length - 1)
					{
						/*
						 * Fill array for variable argument method.
						 */
						parameterDataType = parameterDataType.substring(0, parameterDataType.length() - 2);
						Class<?> parameterClass = getParameterClass(converter, parameterDataType);
						if (parameterClass == Object.class)
						{
							/*
							 * Everything is an Object class so nothing to do.
							 */
						}
						else if (parameterClass == CharSequence.class)
						{
							obj = obj.toString();
						}
						else if (!parameterClass.equals(obj.getClass()))
						{
							obj = converter.convert(parameterDataType, obj.toString());
						}
						Array.set(lastArg, varArgsIndex++, obj);
					}
					else
					{
						Class<?> parameterClass = getParameterClass(converter, parameterDataType);
						if (parameterClass == Object.class)
						{
							/*
							 * Everything is an Object class so nothing to do.
							 */
						}
						else if (parameterClass == CharSequence.class)
						{
							obj = obj.toString();
						}
						else if (!parameterClass.equals(obj.getClass()))
						{
							obj = converter.convert(parameterDataType, obj.toString());
						}
						args[i++] = obj;
					}
				}
			}
			if (isVarArgs)
				args[args.length - 1] = lastArg;
			retval = method.invoke(null, args);
		}
		catch (IllegalArgumentException e)
		{
			throw new SQLException(getInvokeString(args), e);
		}
		catch (IllegalAccessException e)
		{
			throw new SQLException(name + ": " + e.getMessage(), e);
		}
		catch (InvocationTargetException e)
		{
			throw new SQLException(getInvokeString(args), e);
		}
		return retval;
	}

	private Class<?> getParameterClass(StringConverter converter, String parameterDataType)
		throws SQLException
	{
		Class<?> parameterClass = null;
		if (parameterDataType.equals(Object.class.getSimpleName()))
			parameterClass = Object.class;
		else if (parameterDataType.equals(CharSequence.class.getSimpleName()))
			parameterClass = CharSequence.class;
		else
			parameterClass = converter.forSQLName(parameterDataType);

		if (parameterClass == null)
			throw new SQLException(CsvResources.getString("functionArgClass") + ": " + parameterDataType);

		return parameterClass;
	}

	public String getInvokeString(Object []args)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append("(");
		String separator = "";
		for (Object o : args)
		{
			sb.append(separator);
			if (o != null)
				sb.append(o.toString());
			else
				sb.append("null");
			separator = ",";
		}
		sb.append(")");
		return sb.toString();
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append("(");
		String separator = "";
		for (Expression expression : expressions)
		{
			sb.append(separator);
			sb.append(expression.toString());
			separator = ", ";
		}
		sb.append(")");
		return sb.toString();
	}
	public List<String> usedColumns(Set<String> availableColumns)
	{
		List<String> result = new LinkedList<String>();
		for (Expression expression : expressions)
		{
			result.addAll(expression.usedColumns(availableColumns));
		}
		return result;
	}
	public List<AggregateFunction> aggregateFunctions()
	{
		List<AggregateFunction> result = new LinkedList<AggregateFunction>();
		for (Expression expression : expressions)
		{
			result.addAll(expression.aggregateFunctions());
		}
		return result;
	}
}
