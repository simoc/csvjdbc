/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2013  Simon Chenery
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Main class for CsvJdbc, so user can easily experiment with SQL statements in a terminal window.
 */
public class CsvMain
{
	public static void main(String []args)
	{
		Connection connection = null;
		Properties properties = null;
		int exitStatus = 0;

		try
		{
			Class.forName("org.relique.jdbc.csv.CsvDriver");

			int argIndex = 0;
			if (argIndex + 1 < args.length && args[argIndex].equals("-p"))
			{
				/*
				 * Read directory and properties from a Properties file.
				 */
				properties = new Properties();
				Reader propertiesReader = new FileReader(args[argIndex + 1]);
				try
				{
					properties.load(propertiesReader);
				}
				finally
				{
					propertiesReader.close();
				}
				argIndex += 2;
			}
			else if (argIndex < args.length && args[argIndex].equals("-h"))
			{
				/*
				 * Print usage/help message.
				 */
				argIndex = args.length;
			}
			else if (argIndex < args.length && args[argIndex].startsWith("-"))
			{
				System.err.println(CsvResources.getString("unknownCommandLine") + ": " + args[argIndex]);
				System.err.println();

				argIndex = args.length;
			}

			if (argIndex < args.length)
			{	
				/*
				 * Connect using URL containing directory name and properties.
				 */
				if (properties != null)
					connection = DriverManager.getConnection(args[argIndex], properties);
				else
					connection = DriverManager.getConnection(args[argIndex]);
				argIndex++;

				CsvStatement statement = (CsvStatement)connection.createStatement();

				BufferedReader reader;
				boolean isStdin;
				if (argIndex == args.length)
				{
					/*
					 * No files given, so read stdin.
					 */
					reader = new BufferedReader(new InputStreamReader(System.in));
					isStdin = true;
				}
				else
				{
					reader = new BufferedReader(new FileReader(args[argIndex]));
					isStdin = false;
					argIndex++;
				}

				/*
				 * Do not include header line in CSV output, if not included in source CSV file.
				 */
				boolean writeHeaderLine = !((CsvConnection)connection).isSuppressHeaders();

				/*
				 * Read and execute each SQL statement and write results to stdout.
				 */
				do
				{
					String line;
					StringBuilder sb = new StringBuilder();
					while ((line = reader.readLine()) != null)
					{
						sb.append(line);
						sb.append("\n");
					}
					if (!isStdin)
						reader.close();
					reader = null;

					if (statement.execute(sb.toString()))
					{
						ResultSet resultSet = statement.getResultSet();
						CsvDriver.writeToCsv(resultSet, System.out, writeHeaderLine);
						
						/*
						 * No header line for second, third, ... queries, so that
						 * output of several SQL queries can be concatenated.  
						 */
						writeHeaderLine = false;

						while (statement.getMoreResults())
						{
							resultSet = statement.getResultSet();
							CsvDriver.writeToCsv(resultSet, System.out, writeHeaderLine);
						}
					}

					if (argIndex < args.length)
					{
						reader = new BufferedReader(new FileReader(args[argIndex]));
						argIndex++;
					}
				}
				while (reader != null);
			}
			else
			{
				System.err.println(CsvResources.getString("usage"));
				exitStatus = -1;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			exitStatus = -1;
		}
		finally
		{
			try
			{
				if (connection != null)
					connection.close();
			}
			catch (SQLException e)
			{
			}
		}
		System.exit(exitStatus);
	}
}
