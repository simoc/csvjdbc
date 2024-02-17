/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2024 Simon Chenery
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

class SQLRandomFunction extends Expression
{
	private Random random = null;

	/**
	 * Remembers random numbers already generated for rows.
	 */
	private ArrayList<Double> randomsForRows = new ArrayList<>();

	private Expression expression = new SQLLineNumberFunction();

	public SQLRandomFunction()
	{
	}

	@Override
	public Object eval(Map<String, Object> env) throws SQLException
	{
		/*
		 * Delay creation of random number generator until first evaluation,
		 * when we are able to access any random seed set for the connection.
		 */
		if (random == null)
		{
			CsvStatement statement = (CsvStatement)env.get(CsvStatement.STATEMENT_COLUMN_NAME);
			Long randomSeed = ((CsvConnection)statement.getConnection()).getRandomSeed();
			if (randomSeed != null)
			{
				random = new Random(randomSeed.longValue());
			}
			else
			{
				random = new Random();
			}
		}

		/*
		 * Random function will be evaluated several times if the application
		 * calls ResultSet.getDouble(n) to fetch the column value several times.
		 *
		 * Cache the random number generated for each row, and return the same
		 * value if the random function is evaluated again for the same row.
		 */
		Integer lineNumber = (Integer)expression.eval(env);
		if (lineNumber != null && lineNumber.intValue() < randomsForRows.size())
		{
			Double previousValue = randomsForRows.get(lineNumber);
			if (previousValue != null)
			{
				return previousValue;
			}
		}
		Double nextDouble = Double.valueOf(random.nextDouble());
		if (lineNumber != null)
		{
			/*
			 * Extend array for this row.
			 */
			int i = lineNumber.intValue();
			randomsForRows.ensureCapacity(i + 1);
			while (randomsForRows.size() < i + 1)
			{
				randomsForRows.add(null);
			}
			randomsForRows.set(i, nextDouble);
		}
		return Double.valueOf(nextDouble);
	}

	@Override
	public String toString()
	{
		return "RANDOM";
	}

	@Override
	public List<String> usedColumns(Set<String> availableColumns)
	{
		return new LinkedList<>();
	}

	@Override
	public List<AggregateFunction> aggregateFunctions()
	{
		return new LinkedList<>();
	}
}
