/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2013  Simon Chenery
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

import java.io.StringReader;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Parses one or many SQL statements.
 */
public class MultipleSqlParser
{
	public List<SqlParser> parse(String sql) throws ParseException, SQLException
	{
		// Ensure last line of SQL statement ends with newline so we can
		// correctly skip single-line comments.
		sql = sql + "\n";

		ExpressionParser cs2 = new ExpressionParser(new StringReader(sql));
		List<ParsedStatement> statements = cs2.parseMultipleStatements();
		LinkedList<SqlParser> retval = new LinkedList<SqlParser>();
		for (ParsedStatement parsedStatement : statements)
		{
			SqlParser sqlParser = new SqlParser();
			sqlParser.setParsedStatement(parsedStatement);
			retval.add(sqlParser);
		}
		return retval;
	}
}
