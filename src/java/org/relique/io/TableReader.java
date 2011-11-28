package org.relique.io;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Interface for reading database tables.
 */
public interface TableReader {

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
	public List getTableNames(Connection connection) throws SQLException;
}
