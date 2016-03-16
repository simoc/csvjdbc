/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
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
package org.relique.io;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Interface for reading database tables.
 */
public interface TableReader
{
	/**
	 * Get reader for a database table.
	 * @param statement JDBC statement being executed.
	 * @param tableName name of database table to read.
	 * @return reader for the table, csvjdbc will close the reader itself at the end.
	 * @throws SQLException if table does not exist or cannot be read.
	 */
	public Reader getReader(Statement statement, String tableName) throws SQLException;

	/**
	 * Returns a list of the names of all tables in the database.
	 * @param connection JDBC connection.
	 * @return list of String values containing table names.
	 * @throws SQLException if there is a problem creating table name list.
	 */
	public List<String> getTableNames(Connection connection) throws SQLException;
}
