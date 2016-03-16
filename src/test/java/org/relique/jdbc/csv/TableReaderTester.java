/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2013  Simon Chenery
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

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;

import org.relique.io.TableReader;

/**
 * Class for testing TableReader functionality that enables user to provide the
 * database tables from a Java class.
 */
public class TableReaderTester implements TableReader
{
	@Override
	public Reader getReader(Statement statement, String tableName) throws SQLException
	{
		if (tableName.equalsIgnoreCase("AIRLINE"))
			return new StringReader("CODE,NAME\nLH,Lufthansa\nBA,British Airways\nAF,Air France\n");
		else if (tableName.equalsIgnoreCase("AIRPORT"))
			return new StringReader("CODE,NAME\nFRA,Frankfurt\nLHR,London Heathrow\nCDG,Paris Charles De Gaulle");
		throw new SQLException("Table does not exist: " + tableName);
	}

	@Override
	public List<String> getTableNames(Connection connection) throws SQLException
	{
		Vector<String> v = new Vector<String>();
		v.add("AIRLINE");
		v.add("AIRPORT");
		return v;
	}		
}
