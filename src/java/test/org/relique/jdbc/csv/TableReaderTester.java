package test.org.relique.jdbc.csv;

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
public class TableReaderTester implements TableReader {

	public Reader getReader(Statement statement, String tableName) throws SQLException {
		if (tableName.equalsIgnoreCase("AIRLINE"))
			return new StringReader("CODE,NAME\nLH,Lufthansa\nBA,British Airways\nAF,Air France\n");
		else if (tableName.equalsIgnoreCase("AIRPORT"))
			return new StringReader("CODE,NAME\nFRA,Frankfurt\nLHR,London Heathrow\nCDG,Paris Charles De Gaulle");
		throw new SQLException("Table does not exist: " + tableName);
	}

	public List getTableNames(Connection connection) throws SQLException {
		Vector v = new Vector();
		v.add("AIRLINE");
		v.add("AIRPORT");
		return v;
	}		
}
