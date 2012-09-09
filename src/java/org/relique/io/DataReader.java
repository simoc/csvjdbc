package org.relique.io;

import java.sql.SQLException;
import java.util.Map;

public abstract class DataReader {

	public static final int DEFAULT_COLUMN_SIZE = 20;

	public DataReader() {
		super();
	}

	abstract public boolean next() throws SQLException;

	abstract public String[] getColumnNames() throws SQLException;

	abstract public Object getField(int i) throws SQLException;

	abstract public void close() throws SQLException;

	abstract public Map getEnvironment() throws SQLException;
	
	abstract public String[] getColumnTypes() throws SQLException;

	abstract public int[] getColumnSizes() throws SQLException;

	abstract public String getTableAlias();
}