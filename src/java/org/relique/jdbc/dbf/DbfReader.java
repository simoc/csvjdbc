package org.relique.jdbc.dbf;

import nl.knaw.dans.common.dbflib.CorruptedTableException;
import nl.knaw.dans.common.dbflib.Field;
import nl.knaw.dans.common.dbflib.Record;
import nl.knaw.dans.common.dbflib.Table;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.relique.io.DataReader;

public class DbfReader extends DataReader {
	private Table table = null;
	private List fields;
	private Record record;
	private int rowNo;

	public DbfReader(String path, String tableName) throws SQLException {
		super();
		table = new Table(new File(path, tableName + ".dbf"));
		try {
			table.open();
		} catch (CorruptedTableException e) {
			throw new SQLException("" + e);
		} catch (IOException e) {
			throw new SQLException("" + e);
		}
		fields = table.getFields();
		record = null;
		rowNo = -1;
	}

	public void close() throws SQLException {
		if(table != null)
			try {
				table.close();
			} catch (IOException e) {
				throw new SQLException("" + e);
			}
		table = null;
	}

	public String[] getColumnNames() {
		int columnCount = fields.size();
		String[] result = new String[columnCount];
		for(int i=0; i < columnCount; i++) {
			result[i] = ((Field) fields.get(i)).getName();
		}
		return result;
	}

	public Object getField(int i) throws SQLException {
		String fieldName = ((Field) fields.get(i - 1)).getName();
		Object result = record.getTypedValue(fieldName);
		if(result instanceof String)
			result = ((String) result).trim();
		return result;
	}

	public boolean next() throws SQLException {
		rowNo++;
		try {
			record = table.getRecordAt(rowNo);
		} catch (CorruptedTableException e) {
			throw new SQLException("" + e);
		} catch (IOException e) {
			return false;
		} catch (NoSuchElementException e) {
			return false;
		}
		return true;
	}

	public String[] getColumnTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	public Map getEnvironment() throws SQLException {
		Map result = new HashMap();
		for(int i=0; i < fields.size(); i++) {
			String name = ((Field) fields.get(i)).getName();
			result.put(name, getField(i + 1));
		}
		return result;
	}

	public void setFieldValues(String[] strings) throws SQLException {
		throw new SQLException("can't set fieldValues on DbfReader");
	}

}
